#!/usr/bin/env node
// server.js — HenryDB PostgreSQL-compatible TCP server
// Accepts psql connections via the PostgreSQL wire protocol v3.

import net from 'node:net';
import http from 'node:http';
import crypto from 'node:crypto';
import { Database } from './db.js';
import { AdaptiveQueryEngine } from './adaptive-engine.js';
import { parse } from './sql.js';
import { QueryCache } from './query-cache.js';
import {
  writeAuthenticationOk, writeAuthenticationMD5, parsePasswordMessage,
  writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseStartupMessage, parseQueryMessage, inferTypeOid,
  parseParseMessage, parseBindMessage, parseDescribeMessage,
  parseExecuteMessage, parseCloseMessage,
  writeParseComplete, writeBindComplete, writeCloseComplete,
  writeNoData, writeParameterDescription, writeEmptyQueryResponse,
  writePortalSuspended,
  writeNotificationResponse,
  writeCopyInResponse, writeCopyOutResponse, writeCopyData, writeCopyDone,
  PG_TYPES,
} from './pg-protocol.js';

const DEFAULT_PORT = 5433; // Avoid conflict with real PostgreSQL on 5432

export class HenryDBServer {
  constructor(options = {}) {
    this.port = options.port || DEFAULT_PORT;
    this.host = options.host || '127.0.0.1';
    this.db = options.db || new Database();
    // User authentication: { username → { password } }
    // If empty, authentication is disabled (accept all connections)
    this._users = options.users || new Map();
    this.adaptiveEngine = null;
    if (options.adaptive !== false) {
      try {
        this.adaptiveEngine = new AdaptiveQueryEngine(this.db);
      } catch (e) {
        // Adaptive engine not available — fall back to Volcano
      }
    }
    this.server = null;
    this.connections = new Set();
    this._nextPid = 1;
    this._verbose = options.verbose || false;
    // Global prepared statement plan cache (shared across connections)
    this._planCache = new Map();
    // LISTEN/NOTIFY channel registry: channel → Set<conn>
    this._channels = new Map();
    // Query result cache
    this._queryCache = new QueryCache({
      maxSize: options.cacheSize || 500,
      maxAgeMs: options.cacheTtlMs || 60000,
      enabled: options.queryCache !== false,
    });
    // Server metrics
    this._metrics = {
      startTime: Date.now(),
      totalConnections: 0,
      peakConnections: 0,
      totalQueries: 0,
      totalErrors: 0,
      queryLatencySum: 0,
      queryLatencyCount: 0,
    };
    // Slow query log: circular buffer of slow queries
    this._slowQueries = [];
    this._slowQueryMaxEntries = 100;
    this._slowQueryThresholdMs = options.slowQueryThresholdMs || 100;
    // pg_stat_statements: query pattern tracking
    this._queryStats = new Map(); // normalized SQL → { calls, totalTime, minTime, maxTime, rows } // sql → { result: pre-computed result, hits: number, lastUsed: number }
  }

  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer(socket => this._handleConnection(socket));
      this.server.on('error', reject);
      this.server.listen(this.port, this.host, () => {
        if (this._verbose) {
          console.log(`HenryDB server listening on ${this.host}:${this.port}`);
        }
        
        // Start HTTP health check server on port+1
        this._healthServer = http.createServer((req, res) => {
          if (req.url === '/health' || req.url === '/') {
            const status = this._getHealthStatus();
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(status, null, 2));
          } else if (req.url === '/metrics') {
            const metrics = this._getPrometheusMetrics();
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end(metrics);
          } else {
            res.writeHead(404);
            res.end('Not found');
          }
        });
        this._healthServer.listen(this.port + 1, this.host, () => {
          if (this._verbose) console.log(`Health check on ${this.host}:${this.port + 1}`);
          resolve(this);
        });
        this._healthServer.on('error', () => {
          // Health server is optional — don't fail if port is busy
          resolve(this);
        });
      });
    });
  }

  stop() {
    return new Promise((resolve) => {
      for (const conn of this.connections) {
        conn.socket.destroy();
      }
      this.connections.clear();
      if (this._healthServer) {
        this._healthServer.close();
        this._healthServer = null;
      }
      if (this.server) {
        this.server.close(() => resolve());
      } else {
        resolve();
      }
    });
  }

  _getHealthStatus() {
    const uptimeMs = Date.now() - this._metrics.startTime;
    const cacheStats = this._queryCache.getStats();
    const walStats = this.db.wal?.getStats?.() || {};
    const tableCount = this.db.tables?.size || 0;
    
    return {
      status: 'healthy',
      version: '15.0 (HenryDB)',
      uptime_seconds: Math.floor(uptimeMs / 1000),
      connections: {
        active: this.connections.size,
        total: this._metrics.totalConnections,
        peak: this._metrics.peakConnections,
      },
      queries: {
        total: this._metrics.totalQueries,
        per_second: uptimeMs > 0 ? parseFloat((this._metrics.totalQueries / (uptimeMs / 1000)).toFixed(2)) : 0,
        slow_count: this._slowQueries.length,
        errors: this._metrics.totalErrors,
      },
      cache: {
        entries: cacheStats.entries,
        hit_rate: cacheStats.hitRate,
        hits: cacheStats.hits,
        misses: cacheStats.misses,
      },
      wal: {
        records_written: walStats.recordsWritten || 0,
        bytes_written: walStats.bytesWritten || 0,
        checkpoints: walStats.checkpoints || 0,
      },
      tables: tableCount,
      statement_patterns: this._queryStats.size,
    };
  }

  _getPrometheusMetrics() {
    const m = this._metrics;
    const c = this._queryCache.getStats();
    const uptimeMs = Date.now() - m.startTime;
    return [
      `# HELP henrydb_uptime_seconds Server uptime`,
      `henrydb_uptime_seconds ${Math.floor(uptimeMs / 1000)}`,
      `# HELP henrydb_connections_active Active connections`,
      `henrydb_connections_active ${this.connections.size}`,
      `henrydb_connections_total ${m.totalConnections}`,
      `henrydb_connections_peak ${m.peakConnections}`,
      `# HELP henrydb_queries_total Total queries executed`,
      `henrydb_queries_total ${m.totalQueries}`,
      `henrydb_queries_errors ${m.totalErrors}`,
      `# HELP henrydb_cache_hits Query cache hits`,
      `henrydb_cache_hits ${c.hits}`,
      `henrydb_cache_misses ${c.misses}`,
      `henrydb_cache_entries ${c.entries}`,
      `henrydb_tables_count ${this.db.tables?.size || 0}`,
    ].join('\n');
  }

  _handleConnection(socket) {
    const conn = {
      socket,
      pid: this._nextPid++,
      secretKey: Math.floor(Math.random() * 0x7FFFFFFF),
      buffer: Buffer.alloc(0),
      state: 'startup', // startup | ready | terminated
      txStatus: 'I',    // I = idle, T = in transaction, E = failed transaction
      useAdaptive: true, // Use adaptive engine for SELECTs
      preparedStatements: new Map(), // name → { sql, paramTypes, ast }
      portals: new Map(), // name → { statement, paramValues, result }
      listeningChannels: new Set(), // channels this connection is LISTENing on
      copyState: null, // { table, columns, buffer, rowCount } when in COPY mode
      cursors: new Map(), // name → { rows, position, columns }
      currentQuery: null, // { sql, startTime }
      connectTime: Date.now(),
      queryCount: 0,
      tempTables: new Set(), // Temp tables created by this connection
      params: new Map([
        ['server_version', '15.0'],
        ['server_encoding', 'UTF8'],
        ['client_encoding', 'UTF8'],
        ['datestyle', 'ISO, MDY'],
        ['standard_conforming_strings', 'on'],
        ['search_path', 'public'],
        ['work_mem', '4MB'],
        ['statement_timeout', '0'],
        ['enable_seqscan', 'on'],
        ['enable_indexscan', 'on'],
        ['enable_hashjoin', 'on'],
        ['enable_mergejoin', 'on'],
        ['timezone', 'UTC'],
        ['application_name', ''],
      ]),
    };
    this.connections.add(conn);
    this._metrics.totalConnections++;
    if (this.connections.size > this._metrics.peakConnections) {
      this._metrics.peakConnections = this.connections.size;
    }

    if (this._verbose) {
      console.log(`[${conn.pid}] Client connected from ${socket.remoteAddress}`);
    }

    socket.on('data', (data) => {
      conn.buffer = Buffer.concat([conn.buffer, data]);
      this._processBuffer(conn);
    });

    socket.on('error', (err) => {
      if (this._verbose) console.log(`[${conn.pid}] Socket error: ${err.message}`);
    });

    socket.on('close', () => {
      // Cleanup temp tables
      for (const tempTable of conn.tempTables) {
        try {
          this.db.execute(`DROP TABLE IF EXISTS ${tempTable}`);
        } catch (e) { /* ignore */ }
      }
      conn.tempTables.clear();
      // Unregister from all LISTEN channels
      for (const channel of conn.listeningChannels) {
        const listeners = this._channels.get(channel);
        if (listeners) {
          listeners.delete(conn);
          if (listeners.size === 0) this._channels.delete(channel);
        }
      }
      conn.listeningChannels.clear();
      this.connections.delete(conn);
      if (this._verbose) console.log(`[${conn.pid}] Client disconnected`);
    });
  }

  _sendStartupComplete(conn) {
    const response = Buffer.concat([
      writeAuthenticationOk(),
      writeParameterStatus('server_version', '15.0 (HenryDB)'),
      writeParameterStatus('server_encoding', 'UTF8'),
      writeParameterStatus('client_encoding', 'UTF8'),
      writeParameterStatus('DateStyle', 'ISO, MDY'),
      writeParameterStatus('integer_datetimes', 'on'),
      writeParameterStatus('standard_conforming_strings', 'on'),
      writeBackendKeyData(conn.pid, conn.secretKey),
      writeReadyForQuery(conn.txStatus),
    ]);
    conn.socket.write(response);
    conn.state = 'ready';
  }

  _processBuffer(conn) {
    while (conn.buffer.length > 0) {
      if (conn.state === 'startup' || conn.state === 'authenticating') {
        if (conn.state === 'authenticating') {
          // Expecting password message
          if (conn.buffer.length < 5) return;
          const msgType = conn.buffer[0];
          if (msgType !== 0x70) { // 'p' = PasswordMessage
            conn.socket.write(writeErrorResponse('FATAL', '08P01', 'Expected password message'));
            conn.socket.destroy();
            return;
          }
          const len = conn.buffer.readInt32BE(1);
          const totalLen = 1 + len;
          if (conn.buffer.length < totalLen) return;

          const msgBody = conn.buffer.subarray(5, totalLen);
          conn.buffer = conn.buffer.subarray(totalLen);

          const password = parsePasswordMessage(msgBody);

          // Verify MD5: md5(md5(password + user) + salt)
          const inner = crypto.createHash('md5').update(conn.authPassword + conn.user).digest('hex');
          const outerHash = crypto.createHash('md5');
          outerHash.update(inner);
          outerHash.update(conn.authSalt);
          const expected = 'md5' + outerHash.digest('hex');
          if (password === expected) {
            this._sendStartupComplete(conn);
          } else {
            conn.socket.write(writeErrorResponse('FATAL', '28P01', `password authentication failed for user "${conn.user}"`));
            conn.socket.destroy();
            return;
          }
          continue;
        }

        // Startup message: first 4 bytes are length
        if (conn.buffer.length < 4) return;
        const len = conn.buffer.readInt32BE(0);
        if (conn.buffer.length < len) return;

        const msgBuf = conn.buffer.subarray(0, len);
        conn.buffer = conn.buffer.subarray(len);

        // Check for SSL request (protocol 80877103)
        const protocolCode = msgBuf.readInt32BE(4);
        if (protocolCode === 80877103) {
          // SSL request — send 'N' (not supported)
          conn.socket.write(Buffer.from('N'));
          continue;
        }

        // Check for cancel request (protocol 80877102)
        if (protocolCode === 80877102) {
          // Cancel request — ignore for now
          conn.socket.destroy();
          return;
        }

        const startup = parseStartupMessage(msgBuf);
        if (this._verbose) {
          console.log(`[${conn.pid}] Startup: user=${startup.params.user} db=${startup.params.database}`);
        }
        conn.user = startup.params.user || 'anonymous';

        // Check if authentication is required
        if (this._users.size > 0) {
          const userEntry = this._users.get(conn.user);
          if (!userEntry) {
            conn.socket.write(writeErrorResponse('FATAL', '28P01', `password authentication failed for user "${conn.user}"`));
            conn.socket.destroy();
            return;
          }
          // Send MD5 auth challenge
          const salt = Buffer.alloc(4);
          for (let i = 0; i < 4; i++) salt[i] = Math.floor(Math.random() * 256);
          conn.authSalt = salt;
          conn.authPassword = userEntry.password;
          conn.socket.write(writeAuthenticationMD5(salt));
          conn.state = 'authenticating';
          continue;
        }

        // No auth required — send OK
        this._sendStartupComplete(conn);
        continue;
      }

      // Normal message: first byte is type, next 4 bytes are length (including self)
      if (conn.buffer.length < 5) return;
      const msgType = conn.buffer[0];
      const len = conn.buffer.readInt32BE(1);
      const totalLen = 1 + len;

      if (conn.buffer.length < totalLen) return;

      const msgBody = conn.buffer.subarray(1, totalLen);
      conn.buffer = conn.buffer.subarray(totalLen);

      switch (msgType) {
        case 0x51: // 'Q' — Simple Query
          this._handleQuery(conn, msgBody);
          break;
        case 0x50: // 'P' — Parse
          this._handleParse(conn, msgBody);
          break;
        case 0x42: // 'B' — Bind
          this._handleBind(conn, msgBody);
          break;
        case 0x44: // 'D' — Describe
          this._handleDescribe(conn, msgBody);
          break;
        case 0x45: // 'E' — Execute
          this._handleExecute(conn, msgBody);
          break;
        case 0x43: // 'C' — Close (statement/portal)
          this._handleClose(conn, msgBody);
          break;
        case 0x53: // 'S' — Sync
          conn.socket.write(writeReadyForQuery(conn.txStatus));
          break;
        case 0x48: // 'H' — Flush
          // Just drain any pending data (no-op for us)
          break;
        case 0x64: // 'd' — CopyData
          this._handleCopyData(conn, msgBody);
          break;
        case 0x63: // 'c' — CopyDone
          this._handleCopyDone(conn);
          break;
        case 0x66: // 'f' — CopyFail
          this._handleCopyFail(conn, msgBody);
          break;
        case 0x58: // 'X' — Terminate
          conn.state = 'terminated';
          conn.socket.destroy();
          return;
        default:
          if (this._verbose) {
            console.log(`[${conn.pid}] Unknown message type: 0x${msgType.toString(16)}`);
          }
          break;
      }
    }
  }

  _handleQuery(conn, msgBody) {
    const sql = parseQueryMessage(msgBody);
    if (this._verbose) {
      console.log(`[${conn.pid}] Query: ${sql}`);
    }

    // Track current query for pg_stat_activity
    conn.currentQuery = { sql: sql.substring(0, 1000), startTime: Date.now() };
    conn.queryCount++;
    this._metrics.totalQueries++;

    // Handle empty query
    if (!sql.trim()) {
      conn.socket.write(Buffer.concat([
        writeCommandComplete('EMPTY'),
        writeReadyForQuery(conn.txStatus),
      ]));
      return;
    }

    // Split on semicolons for multi-statement queries
    // (simple approach — doesn't handle semicolons inside strings)
    const statements = sql.split(';').map(s => s.trim()).filter(s => s.length > 0);

    for (const stmt of statements) {
      try {
        // Intercept PostgreSQL system queries that ORMs/drivers commonly use
        const intercepted = this._interceptSystemQuery(conn, stmt);
        if (intercepted) continue;

        // Check query cache for SELECT queries
        const isSelect = /^\s*SELECT/i.test(stmt);
        if (isSelect) {
          const cached = this._queryCache.get(stmt);
          if (cached) {
            this._sendResult(conn, stmt, cached);
            continue;
          }
        }

        // Try adaptive engine for SELECT queries
        let result;
        if (this.adaptiveEngine && conn.useAdaptive && /^\s*SELECT/i.test(stmt)) {
          try {
            const ast = parse(stmt);
            if (ast.type === 'SELECT' && this._isAdaptiveEligible(ast)) {
              const adaptive = this.adaptiveEngine.executeSelect(ast);
              // Validate that adaptive engine returned meaningful results
              if (adaptive.rows && adaptive.rows.length > 0 && Object.keys(adaptive.rows[0]).length > 0) {
                result = { type: 'ROWS', rows: adaptive.rows, _engine: adaptive.engine, _timeMs: adaptive.timeMs };
              } else {
                // Adaptive engine returned empty-keyed rows; fall back to Volcano
                result = this.db.execute(stmt);
              }
            } else {
              result = this.db.execute(stmt);
            }
          } catch (e) {
            // Fallback to standard execution if adaptive fails
            result = this.db.execute(stmt);
          }
        } else {
          result = this.db.execute(stmt);
        }
        // Cache SELECT results
        if (isSelect && result && result.type === 'ROWS') {
          this._queryCache.set(stmt, null, result);
        }

        // Invalidate cache on mutations
        if (!isSelect) {
          const upper = stmt.toUpperCase().trim();
          const tableMatch = stmt.match(/(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|DROP\s+TABLE|ALTER\s+TABLE|TRUNCATE)\s+(\w+)/i);
          if (tableMatch) {
            this._queryCache.invalidate(tableMatch[1]);
          }
        }

        this._sendResult(conn, stmt, result);
      } catch (err) {
        conn.txStatus = conn.txStatus === 'T' ? 'E' : 'I';
        conn.socket.write(writeErrorResponse('ERROR', '42000', err.message));
      }
    }

    // Track query latency and slow queries
    if (conn.currentQuery) {
      const duration = Date.now() - conn.currentQuery.startTime;
      this._metrics.queryLatencySum += duration;
      this._metrics.queryLatencyCount++;
      
      if (duration >= this._slowQueryThresholdMs) {
        this._slowQueries.push({
          pid: conn.pid,
          user: conn.user || 'unknown',
          query: conn.currentQuery.sql.substring(0, 500),
          duration_ms: duration,
          timestamp: new Date().toISOString(),
        });
        if (this._slowQueries.length > this._slowQueryMaxEntries) {
          this._slowQueries.shift();
        }
      }

      // pg_stat_statements tracking
      const normalized = conn.currentQuery.sql.substring(0, 200).trim().replace(/\s+/g, ' ').toLowerCase();
      const existing = this._queryStats.get(normalized);
      if (existing) {
        existing.calls++;
        existing.totalTimeMs += duration;
        existing.minTimeMs = Math.min(existing.minTimeMs, duration);
        existing.maxTimeMs = Math.max(existing.maxTimeMs, duration);
      } else {
        this._queryStats.set(normalized, {
          query: conn.currentQuery.sql.substring(0, 200),
          calls: 1,
          totalTimeMs: duration,
          minTimeMs: duration,
          maxTimeMs: duration,
        });
      }
    }
    conn.currentQuery = null;
    conn.socket.write(writeReadyForQuery(conn.txStatus));
  }

  _sendResult(conn, sql, result) {
    if (!result) {
      conn.socket.write(writeCommandComplete('OK'));
      return;
    }

    // Update transaction status
    const upperSql = sql.toUpperCase().trim();
    if (upperSql.startsWith('BEGIN')) conn.txStatus = 'T';
    else if (upperSql.startsWith('COMMIT') || upperSql.startsWith('ROLLBACK')) conn.txStatus = 'I';

    if (result.type === 'PLAN') {
      // EXPLAIN result — convert plan to rows for display
      const rows = result.plan.map(step => ({
        operation: step.operation,
        detail: step.table || step.condition || step.detail || '',
        estimated_rows: step.estimated_rows || '',
      }));
      const columns = ['operation', 'detail', 'estimated_rows'];
      conn.socket.write(writeRowDescription(
        columns.map(c => ({ name: c, typeOid: PG_TYPES.TEXT }))
      ));
      for (const row of rows) {
        conn.socket.write(writeDataRow(columns.map(c => row[c] || null)));
      }
      conn.socket.write(writeCommandComplete(`EXPLAIN`));
      return;
    }

    if (result.type === 'OK') {
      // DDL or DML result
      const tag = this._getCommandTag(sql, result);
      conn.socket.write(writeCommandComplete(tag));
      return;
    }

    if (result.type === 'ROWS') {
      const rows = result.rows || [];

      if (rows.length === 0) {
        // Empty result set — still send RowDescription with no rows
        const columns = result.columns || [];
        if (columns.length > 0) {
          conn.socket.write(writeRowDescription(
            columns.map(c => ({ name: c, typeOid: PG_TYPES.TEXT }))
          ));
        }
        conn.socket.write(writeCommandComplete(`SELECT 0`));
        return;
      }

      // Derive columns from first row
      const columnNames = Object.keys(rows[0]);
      const sampleValues = Object.values(rows[0]);

      // Send RowDescription
      const colDescs = columnNames.map((name, i) => ({
        name,
        typeOid: inferTypeOid(sampleValues[i]),
        typeSize: -1,
      }));
      conn.socket.write(writeRowDescription(colDescs));

      // Send DataRows
      for (const row of rows) {
        const values = columnNames.map(c => {
          const v = row[c];
          return v === null || v === undefined ? null : v;
        });
        conn.socket.write(writeDataRow(values));
      }

      // Send CommandComplete
      const upperForTag = sql.toUpperCase().trim();
      let tag;
      if (upperForTag.startsWith('INSERT') && result.count !== undefined) {
        tag = `INSERT 0 ${result.count}`;
      } else if (upperForTag.startsWith('UPDATE') && result.count !== undefined) {
        tag = `UPDATE ${result.count}`;
      } else if (upperForTag.startsWith('DELETE') && result.count !== undefined) {
        tag = `DELETE ${result.count}`;
      } else {
        tag = `SELECT ${rows.length}`;
      }
      conn.socket.write(writeCommandComplete(tag));
      return;
    }

    // Fallback
    conn.socket.write(writeCommandComplete('OK'));
  }

  _getCommandTag(sql, result) {
    const upper = sql.toUpperCase().trim();
    if (upper.startsWith('INSERT')) {
      const count = result.count || result.message?.match(/(\d+)/)?.[1] || 0;
      return `INSERT 0 ${count}`;
    }
    if (upper.startsWith('UPDATE')) {
      const count = result.count || result.message?.match(/(\d+)/)?.[1] || 0;
      return `UPDATE ${count}`;
    }
    if (upper.startsWith('DELETE')) {
      const count = result.count || result.message?.match(/(\d+)/)?.[1] || 0;
      return `DELETE ${count}`;
    }
    if (upper.startsWith('CREATE')) return 'CREATE TABLE';
    if (upper.startsWith('DROP')) return 'DROP TABLE';
    if (upper.startsWith('ALTER')) return 'ALTER TABLE';
    if (upper.startsWith('BEGIN')) return 'BEGIN';
    if (upper.startsWith('COMMIT')) return 'COMMIT';
    if (upper.startsWith('ROLLBACK')) return 'ROLLBACK';
    return 'OK';
  }

  _handleParse(conn, msgBody) {
    try {
      const { name, query, paramTypes } = parseParseMessage(msgBody);
      if (this._verbose) {
        console.log(`[${conn.pid}] Parse: name="${name}" query="${query}" params=${paramTypes.length}`);
      }

      // Parse the SQL and store the prepared statement
      let ast = null;
      const trimmedQuery = query.trim();
      if (trimmedQuery) {
        // Replace $1, $2, etc. with placeholders that our parser can handle
        ast = { _rawSql: trimmedQuery, _paramTypes: paramTypes };
      }

      conn.preparedStatements.set(name, { sql: trimmedQuery, paramTypes, ast });
      conn.socket.write(writeParseComplete());
    } catch (err) {
      conn.socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
  }

  _handleBind(conn, msgBody) {
    try {
      const { portal, statement, paramValues, resultFormats } = parseBindMessage(msgBody);
      if (this._verbose) {
        console.log(`[${conn.pid}] Bind: portal="${portal}" stmt="${statement}" params=${paramValues.length}`);
      }

      const stmt = conn.preparedStatements.get(statement);
      if (!stmt) {
        conn.socket.write(writeErrorResponse('ERROR', '26000', `Prepared statement "${statement}" not found`));
        return;
      }

      // Substitute parameters into SQL
      let sql = stmt.sql;
      for (let i = 0; i < paramValues.length; i++) {
        const val = paramValues[i];
        const placeholder = `$${i + 1}`;
        let replacement;
        if (val === null) {
          replacement = 'NULL';
        } else if (typeof val === 'string') {
          // Check if it looks numeric
          if (/^-?\d+(\.\d+)?$/.test(val)) {
            replacement = val;
          } else {
            replacement = `'${val.replace(/'/g, "''")}'`;
          }
        } else {
          replacement = String(val);
        }
        // Replace all occurrences of the placeholder
        sql = sql.split(placeholder).join(replacement);
      }

      // Execute and store the result for the portal
      let result = null;
      if (sql.trim()) {
        // Check system queries first
        const upper = sql.toUpperCase().trim();
        if (upper === 'SELECT VERSION()' || upper === 'SELECT VERSION ()') {
          result = { type: 'ROWS', rows: [{ version: 'PostgreSQL 15.0 on HenryDB (JavaScript)' }] };
        } else if (upper.includes('PG_CATALOG') || upper.includes('PG_TYPE') || upper.includes('PG_ATTRIBUTE') || upper.includes('PG_CLASS') || upper.includes('PG_NAMESPACE')) {
          result = { type: 'ROWS', rows: [], columns: [] };
        } else if (upper.includes('INFORMATION_SCHEMA.TABLES')) {
          const rows = [];
          for (const [name] of this.db.tables) {
            rows.push({ table_catalog: 'henrydb', table_schema: 'public', table_name: name, table_type: 'BASE TABLE' });
          }
          result = { type: 'ROWS', rows };
        } else if (upper.includes('INFORMATION_SCHEMA.COLUMNS')) {
          const rows = [];
          const tableFilter = sql.match(/table_name\s*=\s*'([^']+)'/i);
          for (const [tableName, table] of this.db.tables) {
            if (tableFilter && tableName !== tableFilter[1]) continue;
            if (table.schema) {
              table.schema.forEach((col, idx) => {
                rows.push({ table_catalog: 'henrydb', table_schema: 'public', table_name: tableName, column_name: col.name, ordinal_position: idx + 1, data_type: col.type || 'text', is_nullable: 'YES' });
              });
            }
          }
          result = { type: 'ROWS', rows };
        } else if (upper.startsWith('SET ')) {
          result = { type: 'OK', message: 'SET' };
        } else if (upper.startsWith('DEALLOCATE')) {
          result = { type: 'OK', message: 'DEALLOCATE' };
        } else {
          result = this.db.execute(sql);
        }
      }

      conn.portals.set(portal, { statement, paramValues, result, resultFormats, sql });
      conn.socket.write(writeBindComplete());
    } catch (err) {
      conn.socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
  }

  _handleDescribe(conn, msgBody) {
    try {
      const { type, name } = parseDescribeMessage(msgBody);
      if (this._verbose) {
        console.log(`[${conn.pid}] Describe: type=${type} name="${name}"`);
      }

      if (type === 'S') {
        // Describe prepared statement
        const stmt = conn.preparedStatements.get(name);
        if (!stmt) {
          conn.socket.write(writeErrorResponse('ERROR', '26000', `Prepared statement "${name}" not found`));
          return;
        }
        // Send ParameterDescription — count $N placeholders
        const paramCount = (stmt.sql.match(/\$\d+/g) || []).length;
        const paramOids = stmt.paramTypes.length > 0 
          ? stmt.paramTypes 
          : new Array(paramCount).fill(PG_TYPES.TEXT);
        conn.socket.write(writeParameterDescription(paramOids));
        
        // Try to determine result columns by executing with dummy values
        if (/^\s*SELECT/i.test(stmt.sql)) {
          // Probe execution: replace $N with NULLs to get column info
          let probeSql = stmt.sql;
          for (let i = paramCount; i >= 1; i--) {
            probeSql = probeSql.split(`$${i}`).join('NULL');
          }
          try {
            const probeResult = this.db.execute(probeSql);
            if (probeResult && probeResult.type === 'ROWS') {
              if (probeResult.rows.length > 0) {
                const columnNames = Object.keys(probeResult.rows[0]);
                const sampleValues = Object.values(probeResult.rows[0]);
                conn.socket.write(writeRowDescription(
                  columnNames.map((cname, i) => ({
                    name: cname,
                    typeOid: inferTypeOid(sampleValues[i]),
                    typeSize: -1,
                  }))
                ));
              } else if (probeResult.columns && probeResult.columns.length > 0) {
                conn.socket.write(writeRowDescription(
                  probeResult.columns.map(cname => ({
                    name: cname,
                    typeOid: PG_TYPES.TEXT,
                    typeSize: -1,
                  }))
                ));
              } else {
                conn.socket.write(writeNoData());
              }
            } else {
              conn.socket.write(writeNoData());
            }
          } catch (e) {
            // Probe failed (e.g., table doesn't exist), send NoData
            conn.socket.write(writeNoData());
          }
        } else {
          conn.socket.write(writeNoData());
        }
      } else {
        // Describe portal
        const portal = conn.portals.get(name);
        if (!portal) {
          conn.socket.write(writeErrorResponse('ERROR', '34000', `Portal "${name}" not found`));
          return;
        }
        
        if (portal.result && portal.result.type === 'ROWS' && portal.result.rows.length > 0) {
          const columnNames = Object.keys(portal.result.rows[0]);
          const sampleValues = Object.values(portal.result.rows[0]);
          conn.socket.write(writeRowDescription(
            columnNames.map((cname, i) => ({
              name: cname,
              typeOid: inferTypeOid(sampleValues[i]),
              typeSize: -1,
            }))
          ));
        } else {
          conn.socket.write(writeNoData());
        }
      }
    } catch (err) {
      conn.socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
  }

  _handleExecute(conn, msgBody) {
    try {
      const { portal: portalName, maxRows } = parseExecuteMessage(msgBody);
      if (this._verbose) {
        console.log(`[${conn.pid}] Execute: portal="${portalName}" maxRows=${maxRows}`);
      }

      const portal = conn.portals.get(portalName);
      if (!portal) {
        conn.socket.write(writeErrorResponse('ERROR', '34000', `Portal "${portalName}" not found`));
        return;
      }

      const result = portal.result;
      if (!result) {
        conn.socket.write(writeEmptyQueryResponse());
        return;
      }

      this._sendResult(conn, portal.sql, result);
    } catch (err) {
      conn.txStatus = conn.txStatus === 'T' ? 'E' : 'I';
      conn.socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
  }

  _handleCopyData(conn, msgBody) {
    if (!conn.copyState) return;
    // Extract data from CopyData message (skip 4-byte length)
    const data = msgBody.toString('utf8', 4);
    conn.copyState.buffer += data;
  }

  _handleCopyDone(conn) {
    if (!conn.copyState) {
      conn.socket.write(writeReadyForQuery(conn.txStatus));
      return;
    }

    const { table, columns, buffer } = conn.copyState;
    let rowCount = 0;

    try {
      // Parse TSV (PostgreSQL default COPY format)
      const lines = buffer.split('\n').filter(l => l.length > 0 && l !== '\\.');
      for (const line of lines) {
        const values = line.split('\t').map(v => {
          if (v === '\\N') return null;
          return v;
        });

        // Build INSERT SQL
        const vals = values.map((v, i) => {
          if (v === null) return 'NULL';
          // Try to detect numbers
          if (/^-?\d+(\.\d+)?$/.test(v)) return v;
          return `'${v.replace(/'/g, "''")}'`;
        });

        this.db.execute(`INSERT INTO ${table} (${columns.join(', ')}) VALUES (${vals.join(', ')})`);
        rowCount++;
      }

      conn.copyState = null;
      conn.socket.write(Buffer.concat([
        writeCommandComplete(`COPY ${rowCount}`),
        writeReadyForQuery(conn.txStatus),
      ]));
    } catch (err) {
      conn.copyState = null;
      conn.socket.write(Buffer.concat([
        writeErrorResponse('ERROR', '42000', err.message),
        writeReadyForQuery(conn.txStatus),
      ]));
    }
  }

  _handleCopyFail(conn, msgBody) {
    const message = msgBody.toString('utf8', 4).replace(/\0/g, '');
    conn.copyState = null;
    conn.socket.write(Buffer.concat([
      writeErrorResponse('ERROR', '57014', `COPY failed: ${message}`),
      writeReadyForQuery(conn.txStatus),
    ]));
  }

  _handleClose(conn, msgBody) {
    try {
      const { type, name } = parseCloseMessage(msgBody);
      if (type === 'S') {
        conn.preparedStatements.delete(name);
      } else {
        conn.portals.delete(name);
      }
      conn.socket.write(writeCloseComplete());
    } catch (err) {
      conn.socket.write(writeErrorResponse('ERROR', '42000', err.message));
    }
  }

  /**
   * Check if a query AST is eligible for the adaptive engine.
   * Only simple scans with optional WHERE, ORDER BY, and LIMIT qualify.
   * Aggregates, GROUP BY, JOINs, subqueries go through the standard path
   * (which is more correct if less optimized).
   */
  _isAdaptiveEligible(ast) {
    // Must be a simple SELECT
    if (ast.type !== 'SELECT') return false;
    // No aggregates
    if (ast.columns?.some(c => c.type === 'aggregate' || c.type === 'function_call')) return false;
    // No GROUP BY
    if (ast.groupBy && ast.groupBy.length > 0) return false;
    // No HAVING
    if (ast.having) return false;
    // No JOINs (must be single table)
    if (ast.from?.type === 'join') return false;
    // No subqueries in WHERE
    if (ast.where && this._hasSubquery(ast.where)) return false;
    // No UNION/INTERSECT/EXCEPT
    if (ast.union || ast.intersect || ast.except) return false;
    return true;
  }

  /**
   * Intercept PostgreSQL system queries that ORMs and drivers commonly execute.
   * Returns true if the query was intercepted and handled.
   */
  _interceptSystemQuery(conn, sql) {
    const upper = sql.toUpperCase().trim();
    
    // version() — Knex, Prisma, etc. call this on connect
    if (upper === 'SELECT VERSION()' || upper === 'SELECT VERSION ()') {
      const versionResult = {
        type: 'ROWS',
        rows: [{ version: 'PostgreSQL 15.0 on HenryDB (JavaScript)' }],
      };
      this._sendResult(conn, sql, versionResult);
      return true;
    }

    // current_database() 
    if (upper.includes('CURRENT_DATABASE()')) {
      this._sendResult(conn, sql, { type: 'ROWS', rows: [{ current_database: 'henrydb' }] });
      return true;
    }

    // current_schema
    if (upper.includes('CURRENT_SCHEMA')) {
      this._sendResult(conn, sql, { type: 'ROWS', rows: [{ current_schema: 'public' }] });
      return true;
    }

    // Query cache stats
    if (upper.includes('PG_STAT_CACHE') || (upper.includes('QUERY') && upper.includes('CACHE') && upper.includes('STAT'))) {
      const stats = this._queryCache.getStats();
      this._sendResult(conn, sql, { type: 'ROWS', rows: [stats] });
      return true;
    }

    // Server metrics
    if (upper.includes('PG_STAT_SERVER') || upper.includes('SERVER_METRICS')) {
      const uptimeMs = Date.now() - this._metrics.startTime;
      const qps = uptimeMs > 0 ? (this._metrics.totalQueries / (uptimeMs / 1000)).toFixed(2) : '0';
      const cacheStats = this._queryCache.getStats();
      this._sendResult(conn, sql, { type: 'ROWS', rows: [{
        uptime_seconds: Math.floor(uptimeMs / 1000),
        total_connections: this._metrics.totalConnections,
        active_connections: this.connections.size,
        peak_connections: this._metrics.peakConnections,
        total_queries: this._metrics.totalQueries,
        queries_per_second: parseFloat(qps),
        total_errors: this._metrics.totalErrors,
        cache_hit_rate: cacheStats.hitRate,
        cache_entries: cacheStats.entries,
      }] });
      return true;
    }

    // pg_stat_user_tables — table statistics
    if (upper.includes('PG_STAT_USER_TABLES') || upper.includes('PG_STAT_ALL_TABLES')) {
      const rows = [];
      for (const [name, table] of this.db.tables) {
        const rowCount = table.heap?.data?.length || table.heap?._rowCount || 0;
        rows.push({
          schemaname: 'public',
          relname: name,
          n_live_tup: rowCount,
          n_dead_tup: 0,
          n_mod_since_analyze: 0,
          seq_scan: 0,
          seq_tup_read: 0,
          idx_scan: table.indexes?.size || 0,
          n_tup_ins: 0,
          n_tup_upd: 0,
          n_tup_del: 0,
        });
      }
      this._sendResult(conn, sql, { type: 'ROWS', rows });
      return true;
    }

    // pg_stat_activity — connection monitoring
    if (upper.includes('PG_STAT_ACTIVITY')) {
      const rows = [];
      for (const c of this.connections) {
        rows.push({
          pid: c.pid,
          datname: 'henrydb',
          usename: 'user',
          state: c.currentQuery ? 'active' : 'idle',
          query: c.currentQuery?.sql || '',
          query_start: c.currentQuery?.startTime ? new Date(c.currentQuery.startTime).toISOString() : null,
          state_change: new Date().toISOString(),
          backend_start: new Date(c.connectTime).toISOString(),
          wait_event_type: null,
          wait_event: null,
          client_addr: c.socket?.remoteAddress || '',
          client_port: c.socket?.remotePort || 0,
          backend_type: 'client backend',
          xact_start: c.txStatus === 'T' ? new Date().toISOString() : null,
          query_count: c.queryCount || 0,
        });
      }
      this._sendResult(conn, sql, { type: 'ROWS', rows });
      return true;
    }

    // pg_stat_statements — query pattern tracking
    if (upper.includes('PG_STAT_STATEMENTS')) {
      const rows = [];
      for (const [normalized, stats] of this._queryStats) {
        rows.push({
          query: stats.query,
          calls: stats.calls,
          total_time_ms: Math.round(stats.totalTimeMs * 100) / 100,
          mean_time_ms: Math.round(stats.totalTimeMs / stats.calls * 100) / 100,
          min_time_ms: stats.minTimeMs,
          max_time_ms: stats.maxTimeMs,
        });
      }
      rows.sort((a, b) => b.total_time_ms - a.total_time_ms);
      this._sendResult(conn, sql, { type: 'ROWS', rows: rows.slice(0, 100) });
      return true;
    }

    // Slow query log
    if (upper.includes('PG_STAT_SLOW') || upper.includes('SLOW_QUERIES')) {
      this._sendResult(conn, sql, { type: 'ROWS', rows: [...this._slowQueries].reverse() });
      return true;
    }

    // pg_locks — lock information
    if (upper.includes('PG_LOCKS')) {
      const rows = [];
      for (const c of this.connections) {
        if (c.txStatus === 'T') {
          // Connection has an active transaction — show advisory lock
          rows.push({
            locktype: 'transactionid',
            database: 'henrydb',
            relation: null,
            page: null,
            tuple: null,
            pid: c.pid,
            mode: 'ExclusiveLock',
            granted: true,
            fastpath: false,
          });
        }
      }
      this._sendResult(conn, sql, { type: 'ROWS', rows });
      return true;
    }

    // pg_stat_bgwriter — WAL and checkpoint metrics
    if (upper.includes('PG_STAT_BGWRITER')) {
      const walStats = this.db.wal?.getStats?.() || {};
      this._sendResult(conn, sql, { type: 'ROWS', rows: [{
        checkpoints_timed: walStats.checkpoints || 0,
        checkpoints_req: 0,
        buffers_checkpoint: walStats.syncs || 0,
        buffers_clean: 0,
        maxwritten_clean: 0,
        buffers_backend: 0,
        buffers_backend_fsync: 0,
        buffers_alloc: 0,
        wal_records: walStats.recordsWritten || 0,
        wal_bytes: walStats.bytesWritten || 0,
        stats_reset: new Date(this._metrics.startTime).toISOString(),
      }] });
      return true;
    }

    // pg_cancel_backend / pg_terminate_backend
    if (upper.includes('PG_CANCEL_BACKEND') || upper.includes('PG_TERMINATE_BACKEND')) {
      const pidMatch = sql.match(/pg_(?:cancel|terminate)_backend\s*\(\s*(\d+)\s*\)/i);
      if (pidMatch) {
        const targetPid = parseInt(pidMatch[1]);
        let found = false;
        for (const c of this.connections) {
          if (c.pid === targetPid && c !== conn) {
            if (upper.includes('TERMINATE')) {
              c.socket.write(writeErrorResponse('FATAL', '57P01', 'terminating connection due to administrator command'));
              c.socket.destroy();
            } else {
              c.socket.write(writeErrorResponse('ERROR', '57014', 'canceling statement due to user request'));
            }
            found = true;
            break;
          }
        }
        this._sendResult(conn, sql, { type: 'ROWS', rows: [{ pg_cancel_backend: found }] });
        return true;
      }
    }

    // pg_catalog queries (introspection)
    if (upper.includes('PG_CATALOG') || upper.includes('PG_TYPE') || upper.includes('PG_ATTRIBUTE') || upper.includes('PG_CLASS') || upper.includes('PG_NAMESPACE')) {
      // Return empty result set for catalog queries we can't fully handle
      this._sendResult(conn, sql, { type: 'ROWS', rows: [], columns: [] });
      return true;
    }

    // information_schema.columns (check BEFORE tables since column queries contain 'table_name')
    if (upper.includes('INFORMATION_SCHEMA.COLUMNS')) {
      const rows = [];
      const tableFilter = sql.match(/table_name\s*=\s*'([^']+)'/i);
      for (const [tableName, table] of this.db.tables) {
        if (tableFilter && tableName !== tableFilter[1]) continue;
        if (table.schema) {
          table.schema.forEach((col, idx) => {
            rows.push({
              table_catalog: 'henrydb',
              table_schema: 'public',
              table_name: tableName,
              column_name: col.name,
              ordinal_position: idx + 1,
              data_type: col.type || 'text',
              is_nullable: 'YES',
            });
          });
        }
      }
      this._sendResult(conn, sql, { type: 'ROWS', rows });
      return true;
    }

    // information_schema.tables
    if (upper.includes('INFORMATION_SCHEMA.TABLES')) {
      const rows = [];
      const tableFilter = sql.match(/table_name\s*=\s*'([^']+)'/i);
      const schemaFilter = sql.match(/table_schema\s*=\s*'([^']+)'/i);
      for (const [name] of this.db.tables) {
        if (tableFilter && name !== tableFilter[1]) continue;
        rows.push({
          table_catalog: 'henrydb',
          table_schema: 'public',
          table_name: name,
          table_type: 'BASE TABLE',
        });
      }
      this._sendResult(conn, sql, { type: 'ROWS', rows });
      return true;
    }

    // SET statements (client configuration)
    if (upper.startsWith('SET ')) {
      const match = sql.match(/SET\s+(?:SESSION\s+)?(\w+)\s*(?:=|TO)\s*(.+)/i);
      if (match) {
        const param = match[1].toLowerCase();
        let value = match[2].trim().replace(/^'|'$/g, '');
        conn.params.set(param, value);
        if (this._verbose) console.log(`[${conn.pid}] SET ${param} = ${value}`);
      }
      conn.socket.write(writeCommandComplete('SET'));
      return true;
    }

    // SHOW (e.g., SHOW server_version)
    if (upper.startsWith('SHOW ') && !upper.startsWith('SHOW TABLES')) {
      const param = sql.substring(5).trim().toLowerCase().replace(/;$/, '');
      if (param === 'all') {
        // SHOW ALL — return all parameters
        const rows = [];
        for (const [name, setting] of conn.params) {
          rows.push({ name, setting, description: '' });
        }
        this._sendResult(conn, sql, { type: 'ROWS', rows });
        return true;
      }
      const value = conn.params.get(param) || 'unknown';
      this._sendResult(conn, sql, { type: 'ROWS', rows: [{ [param]: value }] });
      return true;
    }

    // RESET parameter
    if (upper.startsWith('RESET ')) {
      const param = sql.substring(6).trim().toLowerCase().replace(/;$/, '');
      conn.socket.write(writeCommandComplete('RESET'));
      return true;
    }

    // SAVEPOINT
    if (upper.startsWith('SAVEPOINT ')) {
      const name = sql.substring(10).trim().replace(/;$/, '');
      try {
        this.db.execute(`SAVEPOINT ${name}`);
      } catch (e) { /* ignore if not in tx */ }
      conn.socket.write(writeCommandComplete('SAVEPOINT'));
      return true;
    }

    // RELEASE SAVEPOINT
    if (upper.startsWith('RELEASE SAVEPOINT') || upper.startsWith('RELEASE ')) {
      const name = sql.replace(/RELEASE\s+(?:SAVEPOINT\s+)?/i, '').trim().replace(/;$/, '');
      try {
        this.db.execute(`RELEASE SAVEPOINT ${name}`);
      } catch (e) { /* ignore */ }
      conn.socket.write(writeCommandComplete('RELEASE'));
      return true;
    }

    // ROLLBACK TO SAVEPOINT
    if (upper.startsWith('ROLLBACK TO')) {
      const name = sql.replace(/ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?/i, '').trim().replace(/;$/, '');
      try {
        this.db.execute(`ROLLBACK TO SAVEPOINT ${name}`);
      } catch (e) { /* ignore */ }
      conn.socket.write(writeCommandComplete('ROLLBACK'));
      return true;
    }

    // VACUUM [ANALYZE] [table_name]
    if (upper.startsWith('VACUUM')) {
      const analyzeFlag = upper.includes('ANALYZE');
      const tableMatch = sql.match(/VACUUM\s+(?:FULL\s+)?(?:ANALYZE\s+)?(\w+)/i);
      const tableName = tableMatch?.[1]?.toUpperCase() !== 'ANALYZE' ? tableMatch?.[1] : null;
      
      const tables = tableName ? [tableName] : [...this.db.tables.keys()];
      let totalReclaimed = 0;
      
      for (const t of tables) {
        const table = this.db.tables.get(t);
        if (!table) continue;
        // Simulate vacuum: count and "reclaim" dead rows
        // In our in-memory engine, there aren't really dead rows, but we can still report
        totalReclaimed += 0; // placeholder
      }
      
      conn.socket.write(writeCommandComplete('VACUUM'));
      return true;
    }

    // ANALYZE [table_name]
    if (upper.startsWith('ANALYZE') && !upper.includes('EXPLAIN')) {
      const tableMatch = sql.match(/ANALYZE\s+(\w+)/i);
      const tableName = tableMatch?.[1];
      const tables = tableName ? [tableName] : [...this.db.tables.keys()];
      
      for (const t of tables) {
        const table = this.db.tables.get(t);
        if (!table) continue;
        // Update table statistics
        const rowCount = table.heap?.data?.length || table.heap?._rowCount || 0;
        table._stats = {
          rowCount,
          lastAnalyze: new Date().toISOString(),
          columnStats: (table.schema || []).map(col => ({
            name: col.name,
            type: col.type,
            distinct: 0, // would need full scan to compute
          })),
        };
      }
      
      conn.socket.write(writeCommandComplete('ANALYZE'));
      return true;
    }

    // CREATE TEMP TABLE — track for auto-cleanup
    if (upper.startsWith('CREATE TEMP TABLE') || upper.startsWith('CREATE TEMPORARY TABLE')) {
      const match = sql.match(/CREATE\s+(?:TEMP|TEMPORARY)\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i);
      if (match) {
        const tableName = match[1];
        try {
          // Create the table normally but track it for this connection
          const createSql = sql.replace(/CREATE\s+(?:TEMP|TEMPORARY)\s+TABLE/i, 'CREATE TABLE');
          this.db.execute(createSql);
          conn.tempTables.add(tableName);
          conn.socket.write(writeCommandComplete('CREATE TABLE'));
        } catch (e) {
          conn.socket.write(writeErrorResponse('ERROR', '42000', e.message));
        }
        return true;
      }
    }

    // TRUNCATE table_name
    if (upper.startsWith('TRUNCATE')) {
      const tableMatch = sql.match(/TRUNCATE\s+(?:TABLE\s+)?(\w+)/i);
      if (tableMatch) {
        const tableName = tableMatch[1];
        try {
          this.db.execute(`DELETE FROM ${tableName}`);
          this._queryCache.invalidate(tableName);
          conn.socket.write(writeCommandComplete('TRUNCATE TABLE'));
        } catch (e) {
          conn.socket.write(writeErrorResponse('ERROR', '42P01', e.message));
        }
        return true;
      }
    }

    // DEALLOCATE (pg driver cleanup)
    if (upper.startsWith('DEALLOCATE')) {
      conn.socket.write(writeCommandComplete('DEALLOCATE'));
      return true;
    }

    // LISTEN channel
    if (upper.startsWith('LISTEN ')) {
      const channel = sql.substring(7).trim().replace(/['"]/g, '');
      conn.listeningChannels.add(channel);
      if (!this._channels.has(channel)) {
        this._channels.set(channel, new Set());
      }
      this._channels.get(channel).add(conn);
      if (this._verbose) console.log(`[${conn.pid}] LISTEN ${channel}`);
      conn.socket.write(writeCommandComplete('LISTEN'));
      return true;
    }

    // UNLISTEN channel / UNLISTEN *
    if (upper.startsWith('UNLISTEN')) {
      const arg = sql.substring(8).trim().replace(/['"]/g, '');
      if (arg === '*') {
        // Unlisten all
        for (const channel of conn.listeningChannels) {
          const listeners = this._channels.get(channel);
          if (listeners) {
            listeners.delete(conn);
            if (listeners.size === 0) this._channels.delete(channel);
          }
        }
        conn.listeningChannels.clear();
      } else {
        conn.listeningChannels.delete(arg);
        const listeners = this._channels.get(arg);
        if (listeners) {
          listeners.delete(conn);
          if (listeners.size === 0) this._channels.delete(arg);
        }
      }
      if (this._verbose) console.log(`[${conn.pid}] UNLISTEN ${arg}`);
      conn.socket.write(writeCommandComplete('UNLISTEN'));
      return true;
    }

    // EXPLAIN (FORMAT JSON|YAML|DOT)
    if (upper.match(/EXPLAIN\s*\(\s*FORMAT\s+(JSON|YAML|DOT)/i)) {
      const formatMatch = sql.match(/FORMAT\s+(JSON|YAML|DOT)/i);
      const format = formatMatch[1].toUpperCase();
      const selectSql = sql.replace(/EXPLAIN\s*\([^)]+\)\s*/i, '').trim();
      
      try {
        let planRows = [];
        try {
          const planResult = this.db.execute('EXPLAIN ' + selectSql);
          if (planResult.type === 'PLAN') planRows = planResult.plan || [];
        } catch (e) { /* plan not available */ }

        // Execute to get actual stats
        const startTime = process.hrtime.bigint();
        const result = this.db.execute(selectSql);
        const execMs = Number(process.hrtime.bigint() - startTime) / 1_000_000;
        const rowCount = result.type === 'ROWS' ? result.rows.length : 0;

        const plan = {
          Plan: {
            'Node Type': planRows[0]?.operation || 'Result',
            'Relation Name': planRows[0]?.table || null,
            'Startup Cost': 0.00,
            'Total Cost': execMs,
            'Plan Rows': rowCount,
            'Plan Width': 0,
            'Actual Total Time': execMs,
            'Actual Rows': rowCount,
            'Actual Loops': 1,
            Plans: planRows.slice(1).map(p => ({
              'Node Type': p.operation,
              'Relation Name': p.table || null,
              'Filter': p.condition || null,
            })),
          },
          'Execution Time': execMs,
        };

        let output;
        if (format === 'JSON') {
          output = JSON.stringify([plan], null, 2);
        } else if (format === 'YAML') {
          output = this._planToYaml(plan, 0);
        } else if (format === 'DOT') {
          output = this._planToDot(plan);
        }

        this._sendResult(conn, sql, { type: 'ROWS', rows: [{ 'QUERY PLAN': output }] });
        return true;
      } catch (e) {
        conn.socket.write(writeErrorResponse('ERROR', '42000', e.message));
        return true;
      }
    }

    // EXPLAIN ANALYZE
    if (upper.startsWith('EXPLAIN ANALYZE') || upper.startsWith('EXPLAIN (ANALYZE)')) {
      const selectSql = sql.replace(/^EXPLAIN\s+(?:\(ANALYZE\)|ANALYZE)\s*/i, '').trim();
      try {
        // Get the query plan
        let planRows = [];
        try {
          const planResult = this.db.execute('EXPLAIN ' + selectSql);
          if (planResult.type === 'PLAN') {
            planRows = planResult.plan || [];
          }
        } catch (e) {
          // Plan not available — continue without it
        }

        // Actually execute the query and measure
        const startTime = process.hrtime.bigint();
        const result = this.db.execute(selectSql);
        const endTime = process.hrtime.bigint();
        const execTimeMs = Number(endTime - startTime) / 1_000_000;
        
        const rowCount = result.type === 'ROWS' ? result.rows.length : 0;

        // Check if adaptive engine was used
        let engineUsed = 'volcano';
        if (this.adaptiveEngine && /^\s*SELECT/i.test(selectSql)) {
          try {
            const ast = parse(selectSql);
            if (ast.type === 'SELECT' && this._isAdaptiveEligible(ast)) {
              engineUsed = 'adaptive';
            }
          } catch (e) { /* ignore */ }
        }

        // Build EXPLAIN ANALYZE output
        const outputRows = [];
        
        // Plan steps
        for (const step of planRows) {
          outputRows.push({
            'QUERY PLAN': `${step.operation || 'Scan'}${step.table ? ` on ${step.table}` : ''}${step.condition ? ` (filter: ${step.condition})` : ''}`
          });
        }

        // Execution stats
        outputRows.push({ 'QUERY PLAN': `Planning Time: 0.01 ms` });
        outputRows.push({ 'QUERY PLAN': `Execution Time: ${execTimeMs.toFixed(3)} ms` });
        outputRows.push({ 'QUERY PLAN': `Rows Returned: ${rowCount}` });
        outputRows.push({ 'QUERY PLAN': `Engine: ${engineUsed}` });

        // Count tables scanned
        const tablesInQuery = [];
        for (const [name] of this.db.tables) {
          if (selectSql.toLowerCase().includes(name.toLowerCase())) {
            const tableData = this.db.tables.get(name);
            if (tableData && tableData.heap) {
              tablesInQuery.push({ name, rows: tableData.heap._rowCount || tableData.heap.data?.length || 0 });
            }
          }
        }
        for (const t of tablesInQuery) {
          outputRows.push({ 'QUERY PLAN': `  Seq Scan on ${t.name}: rows=${t.rows}` });
        }

        if (outputRows.length === 0) {
          outputRows.push({ 'QUERY PLAN': `Result: ${rowCount} rows in ${execTimeMs.toFixed(3)} ms` });
        }

        this._sendResult(conn, sql, { type: 'ROWS', rows: outputRows });
        return true;
      } catch (e) {
        conn.socket.write(writeErrorResponse('ERROR', '42000', e.message));
        return true;
      }
    }

    // DECLARE cursor_name CURSOR FOR select_statement
    if (upper.startsWith('DECLARE ')) {
      const match = sql.match(/DECLARE\s+(\w+)\s+(?:NO\s+SCROLL\s+)?CURSOR\s+(?:WITH(?:OUT)?\s+HOLD\s+)?FOR\s+(.+)/is);
      if (match) {
        const cursorName = match[1];
        const selectSql = match[2].trim().replace(/;$/, '');
        try {
          const result = this.db.execute(selectSql);
          if (result.type === 'ROWS') {
            const columns = result.rows.length > 0 ? Object.keys(result.rows[0]) : (result.columns || []);
            conn.cursors.set(cursorName, { rows: result.rows, position: 0, columns });
          } else {
            conn.cursors.set(cursorName, { rows: [], position: 0, columns: [] });
          }
          conn.socket.write(writeCommandComplete('DECLARE CURSOR'));
        } catch (e) {
          conn.socket.write(writeErrorResponse('ERROR', '42000', e.message));
        }
        return true;
      }
    }

    // FETCH [FORWARD] [count|ALL|NEXT] [FROM|IN] cursor_name
    if (upper.startsWith('FETCH ')) {
      const match = sql.match(/FETCH\s+(?:FORWARD\s+)?(?:(ALL|NEXT|\d+)\s+)?(?:FROM|IN)\s+(\w+)/i);
      if (match) {
        const countStr = (match[1] || 'NEXT').toUpperCase();
        const cursorName = match[2];
        const cursor = conn.cursors.get(cursorName);
        if (!cursor) {
          conn.socket.write(writeErrorResponse('ERROR', '34000', `Cursor "${cursorName}" not found`));
          return true;
        }

        const count = countStr === 'ALL' ? cursor.rows.length - cursor.position
                    : countStr === 'NEXT' ? 1
                    : parseInt(countStr);

        const endPos = Math.min(cursor.position + count, cursor.rows.length);
        const fetchedRows = cursor.rows.slice(cursor.position, endPos);
        cursor.position = endPos;

        // Send result
        if (fetchedRows.length > 0) {
          const columnNames = cursor.columns.length > 0 ? cursor.columns : Object.keys(fetchedRows[0]);
          const sampleValues = Object.values(fetchedRows[0]);
          conn.socket.write(writeRowDescription(
            columnNames.map((name, i) => ({ name, typeOid: inferTypeOid(sampleValues[i]), typeSize: -1 }))
          ));
          for (const row of fetchedRows) {
            const values = columnNames.map(c => {
              const v = row[c];
              return v === null || v === undefined ? null : v;
            });
            conn.socket.write(writeDataRow(values));
          }
        }
        conn.socket.write(writeCommandComplete(`FETCH ${fetchedRows.length}`));
        return true;
      }
    }

    // MOVE [FORWARD] [count|ALL|NEXT] [FROM|IN] cursor_name
    if (upper.startsWith('MOVE ')) {
      const match = sql.match(/MOVE\s+(?:FORWARD\s+)?(?:(ALL|NEXT|\d+)\s+)?(?:FROM|IN)\s+(\w+)/i);
      if (match) {
        const countStr = (match[1] || 'NEXT').toUpperCase();
        const cursorName = match[2];
        const cursor = conn.cursors.get(cursorName);
        if (!cursor) {
          conn.socket.write(writeErrorResponse('ERROR', '34000', `Cursor "${cursorName}" not found`));
          return true;
        }
        const count = countStr === 'ALL' ? cursor.rows.length - cursor.position
                    : countStr === 'NEXT' ? 1
                    : parseInt(countStr);
        const moved = Math.min(count, cursor.rows.length - cursor.position);
        cursor.position += moved;
        conn.socket.write(writeCommandComplete(`MOVE ${moved}`));
        return true;
      }
    }

    // CLOSE cursor_name | CLOSE ALL
    if (upper.startsWith('CLOSE ')) {
      const arg = sql.substring(6).trim();
      if (arg.toUpperCase() === 'ALL') {
        conn.cursors.clear();
      } else {
        conn.cursors.delete(arg);
      }
      conn.socket.write(writeCommandComplete('CLOSE CURSOR'));
      return true;
    }

    // COPY FROM STDIN
    if (upper.startsWith('COPY ') && upper.includes('FROM STDIN')) {
      const match = sql.match(/COPY\s+(\w+)\s*(?:\(([^)]+)\))?\s+FROM\s+STDIN/i);
      if (match) {
        const table = match[1];
        const tableObj = this.db.tables.get(table);
        if (!tableObj) {
          conn.socket.write(writeErrorResponse('ERROR', '42P01', `Table "${table}" not found`));
          return true;
        }
        const columns = match[2] 
          ? match[2].split(',').map(c => c.trim())
          : tableObj.schema.map(c => c.name);
        
        conn.copyState = { table, columns, buffer: '', rowCount: 0 };
        conn.socket.write(writeCopyInResponse(columns.length));
        return true;
      }
    }

    // COPY TO STDOUT
    if (upper.startsWith('COPY ') && upper.includes('TO STDOUT')) {
      const match = sql.match(/COPY\s+(\w+)\s*(?:\(([^)]+)\))?\s+TO\s+STDOUT/i);
      if (match) {
        const table = match[1];
        const tableObj = this.db.tables.get(table);
        if (!tableObj) {
          conn.socket.write(writeErrorResponse('ERROR', '42P01', `Table "${table}" not found`));
          return true;
        }
        const columns = match[2]
          ? match[2].split(',').map(c => c.trim())
          : tableObj.schema.map(c => c.name);

        // Send CopyOutResponse
        conn.socket.write(writeCopyOutResponse(columns.length));

        // Send all rows as CSV
        const result = this.db.execute(`SELECT ${columns.join(', ')} FROM ${table}`);
        let rowCount = 0;
        if (result.type === 'ROWS') {
          for (const row of result.rows) {
            const line = columns.map(c => {
              const v = row[c];
              if (v === null || v === undefined) return '\\N';
              return String(v);
            }).join('\t') + '\n';
            conn.socket.write(writeCopyData(line));
            rowCount++;
          }
        }

        // Send CopyDone + CommandComplete
        conn.socket.write(writeCopyDone());
        conn.socket.write(writeCommandComplete(`COPY ${rowCount}`));
        return true;
      }
    }

    // NOTIFY channel [, 'payload']
    if (upper.startsWith('NOTIFY ')) {
      const args = sql.substring(7).trim();
      let channel, payload = '';
      const commaIdx = args.indexOf(',');
      if (commaIdx !== -1) {
        channel = args.substring(0, commaIdx).trim().replace(/['"]/g, '');
        payload = args.substring(commaIdx + 1).trim().replace(/^'|'$/g, '').replace(/''/g, "'");
      } else {
        channel = args.replace(/['"]/g, '');
      }

      if (this._verbose) console.log(`[${conn.pid}] NOTIFY ${channel}: ${payload}`);

      // Send notification to all listeners (including the sender)
      const listeners = this._channels.get(channel);
      if (listeners) {
        const notifBuf = writeNotificationResponse(conn.pid, channel, payload);
        for (const listener of listeners) {
          try {
            listener.socket.write(notifBuf);
          } catch (e) {
            // Listener socket might be dead
          }
        }
      }

      conn.socket.write(writeCommandComplete('NOTIFY'));
      return true;
    }

    return false;
  }

  _planToYaml(obj, indent = 0) {
    const lines = [];
    const pad = '  '.repeat(indent);
    for (const [key, val] of Object.entries(obj)) {
      if (val === null || val === undefined) continue;
      if (Array.isArray(val)) {
        lines.push(`${pad}${key}:`);
        for (const item of val) {
          lines.push(`${pad}  -`);
          lines.push(this._planToYaml(item, indent + 2));
        }
      } else if (typeof val === 'object') {
        lines.push(`${pad}${key}:`);
        lines.push(this._planToYaml(val, indent + 1));
      } else {
        lines.push(`${pad}${key}: ${val}`);
      }
    }
    return lines.join('\n');
  }

  _planToDot(plan) {
    const lines = ['digraph QueryPlan {', '  rankdir=BT;', '  node [shape=box, style=filled, fillcolor=lightyellow];'];
    let nodeId = 0;
    
    const addNode = (node, parentId) => {
      const myId = nodeId++;
      const label = `${node['Node Type'] || 'Unknown'}${node['Relation Name'] ? '\\n' + node['Relation Name'] : ''}${node['Actual Rows'] !== undefined ? '\\nrows=' + node['Actual Rows'] : ''}${node['Actual Total Time'] !== undefined ? '\\n' + node['Actual Total Time'].toFixed(2) + 'ms' : ''}`;
      lines.push(`  n${myId} [label="${label}"];`);
      if (parentId !== null) {
        lines.push(`  n${myId} -> n${parentId};`);
      }
      if (node.Plans) {
        for (const child of node.Plans) {
          addNode(child, myId);
        }
      }
      return myId;
    };
    
    addNode(plan.Plan, null);
    lines.push('}');
    return lines.join('\n');
  }

  _hasSubquery(expr) {
    if (!expr || typeof expr !== 'object') return false;
    const t = (expr.type || '').toLowerCase();
    if (t === 'subquery' || t === 'exists' || t === 'in_subquery') return true;
    for (const key of Object.keys(expr)) {
      const val = expr[key];
      if (typeof val === 'object' && val !== null) {
        if (Array.isArray(val)) {
          for (const item of val) {
            if (this._hasSubquery(item)) return true;
          }
        } else {
          if (this._hasSubquery(val)) return true;
        }
      }
    }
    return false;
  }
}

// CLI entry point
if (process.argv[1] && (process.argv[1].endsWith('server.js') || process.argv[1].endsWith('server'))) {
  const port = parseInt(process.argv[2]) || DEFAULT_PORT;
  const server = new HenryDBServer({ port, verbose: true });
  server.start().then(() => {
    console.log(`HenryDB ready. Connect with: psql -h 127.0.0.1 -p ${port}`);
    console.log('Press Ctrl+C to stop.');
  }).catch(err => {
    console.error('Failed to start:', err.message);
    process.exit(1);
  });

  process.on('SIGINT', () => {
    console.log('\nShutting down...');
    server.stop().then(() => process.exit(0));
  });
}

export default HenryDBServer;
