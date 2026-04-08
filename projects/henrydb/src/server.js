#!/usr/bin/env node
// server.js — HenryDB PostgreSQL-compatible TCP server
// Accepts psql connections via the PostgreSQL wire protocol v3.

import net from 'node:net';
import { Database } from './db.js';
import { AdaptiveQueryEngine } from './adaptive-engine.js';
import { parse } from './sql.js';
import {
  writeAuthenticationOk, writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseStartupMessage, parseQueryMessage, inferTypeOid,
  parseParseMessage, parseBindMessage, parseDescribeMessage,
  parseExecuteMessage, parseCloseMessage,
  writeParseComplete, writeBindComplete, writeCloseComplete,
  writeNoData, writeParameterDescription, writeEmptyQueryResponse,
  writePortalSuspended,
  writeNotificationResponse,
  PG_TYPES,
} from './pg-protocol.js';

const DEFAULT_PORT = 5433; // Avoid conflict with real PostgreSQL on 5432

export class HenryDBServer {
  constructor(options = {}) {
    this.port = options.port || DEFAULT_PORT;
    this.host = options.host || '127.0.0.1';
    this.db = options.db || new Database();
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
    this._channels = new Map(); // sql → { result: pre-computed result, hits: number, lastUsed: number }
  }

  start() {
    return new Promise((resolve, reject) => {
      this.server = net.createServer(socket => this._handleConnection(socket));
      this.server.on('error', reject);
      this.server.listen(this.port, this.host, () => {
        if (this._verbose) {
          console.log(`HenryDB server listening on ${this.host}:${this.port}`);
        }
        resolve(this);
      });
    });
  }

  stop() {
    return new Promise((resolve) => {
      // Close all active connections
      for (const conn of this.connections) {
        conn.socket.destroy();
      }
      this.connections.clear();
      if (this.server) {
        this.server.close(() => resolve());
      } else {
        resolve();
      }
    });
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
    };
    this.connections.add(conn);

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

  _processBuffer(conn) {
    while (conn.buffer.length > 0) {
      if (conn.state === 'startup') {
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

        // Send authentication OK + server parameters
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
        this._sendResult(conn, stmt, result);
      } catch (err) {
        conn.txStatus = conn.txStatus === 'T' ? 'E' : 'I';
        conn.socket.write(writeErrorResponse('ERROR', '42000', err.message));
      }
    }

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
      conn.socket.write(writeCommandComplete(`SELECT ${rows.length}`));
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
        } else if (upper.includes('PG_CATALOG') || upper.includes('INFORMATION_SCHEMA') || upper.includes('PG_TYPE') || upper.includes('PG_ATTRIBUTE') || upper.includes('PG_CLASS')) {
          result = { type: 'ROWS', rows: [], columns: [] };
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

    // pg_catalog queries (introspection)
    if (upper.includes('PG_CATALOG') || upper.includes('INFORMATION_SCHEMA') || upper.includes('PG_TYPE') || upper.includes('PG_ATTRIBUTE') || upper.includes('PG_CLASS')) {
      // Return empty result set for catalog queries
      this._sendResult(conn, sql, { type: 'ROWS', rows: [], columns: [] });
      return true;
    }

    // SET statements (client configuration)
    if (upper.startsWith('SET ')) {
      conn.socket.write(writeCommandComplete('SET'));
      return true;
    }

    // SHOW (e.g., SHOW server_version)
    if (upper.startsWith('SHOW ') && !upper.startsWith('SHOW TABLES')) {
      const param = sql.substring(5).trim().toLowerCase();
      let value = 'unknown';
      if (param === 'server_version') value = '15.0';
      else if (param === 'server_encoding') value = 'UTF8';
      else if (param === 'client_encoding') value = 'UTF8';
      else if (param === 'standard_conforming_strings') value = 'on';
      else if (param === 'datestyle') value = 'ISO, MDY';
      this._sendResult(conn, sql, { type: 'ROWS', rows: [{ [param]: value }] });
      return true;
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

  _hasSubquery(expr) {
    if (!expr || typeof expr !== 'object') return false;
    if (expr.type === 'subquery' || expr.type === 'exists' || expr.type === 'in_subquery') return true;
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
