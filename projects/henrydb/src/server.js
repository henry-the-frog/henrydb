#!/usr/bin/env node
// server.js — HenryDB PostgreSQL-compatible TCP server
// Accepts psql connections via the PostgreSQL wire protocol v3.

import net from 'node:net';
import http from 'node:http';
import crypto from 'node:crypto';
import { Database } from './db.js';
import { PersistentDatabase } from './persistent-db.js';
import { TransactionalDatabase } from './transactional-db.js';
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
    // If dataDir is provided, use PersistentDatabase for durable storage
    if (options.dataDir) {
      this._persistent = true;
      this._dataDir = options.dataDir;
      if (options.transactional) {
        // TransactionalDatabase for full ACID with BEGIN/COMMIT/ROLLBACK
        this._txDb = options.db || TransactionalDatabase.open(options.dataDir, {
          poolSize: options.poolSize || 64,
          recover: options.recover !== false,
          isolationLevel: options.isolationLevel || 'snapshot',
        });
        this.db = this._txDb; // For compatibility (system queries, catalog, etc.)
      } else {
        this.db = options.db || PersistentDatabase.open(options.dataDir, {
          poolSize: options.poolSize || 64,
          recover: options.recover !== false,
          walSync: options.walSync || 'batch',
          walBatchMs: options.walBatchMs || 5,
        });
      }
    } else if (options.transactional && options.txDb) {
      this._persistent = false;
      this._txDb = options.txDb;
      this.db = this._txDb;
    } else {
      this._persistent = false;
      this.db = options.db || new Database();
    }
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
    // Slow query log — stores all executed queries with timing
    this._queryLog = []; // [{query, duration_ms, timestamp, pid, rows}]
    this._queryLogMaxSize = 10000;
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
          } else if (req.url === '/explain' && req.method === 'POST') {
            this._handleExplainEndpoint(req, res);
          } else if (req.url === '/explain' && req.method === 'GET') {
            this._serveExplainUI(req, res);
          } else if (req.url === '/dashboard' && req.method === 'GET') {
            this._serveDashboard(req, res);
          } else if (req.url === '/query' && req.method === 'POST') {
            this._handleQueryEndpoint(req, res);
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
      // Flush persistent database before closing
      if (this._persistent && this.db && typeof this.db.close === 'function') {
        try {
          this.db.close();
          this.db = null; // prevent double-close
        } catch (e) {
          // Best effort — log but don't block shutdown
          if (this._verbose) console.error('Error closing persistent db:', e.message);
        }
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

  // ===== HTTP API Endpoints =====

  _handleExplainEndpoint(req, res) {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const { query, analyze } = JSON.parse(body);
        if (!query) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing "query" field' }));
          return;
        }
        
        const explainSQL = analyze 
          ? `EXPLAIN ANALYZE ${query}`
          : `EXPLAIN (FORMAT HTML) ${query}`;
        
        const result = this.db.execute(explainSQL);
        
        if (result.html) {
          res.writeHead(200, { 'Content-Type': 'text/html', 'Access-Control-Allow-Origin': '*' });
          res.end(result.html);
        } else if (result.planTreeText) {
          // EXPLAIN ANALYZE: return the tree plan text + HTML
          const { PlanBuilder } = require('./query-plan.js');
          const { planToHTML } = require('./plan-html.js');
          const { parse: parseSql } = require('./sql.js');
          
          try {
            const builder = new PlanBuilder(this.db);
            const ast = parseSql(query);
            const planTree = builder.buildPlan(ast);
            // Fill in actuals from the analyze result
            planTree.setActuals(result.actual_rows, result.execution_time_ms);
            const html = planToHTML(planTree, { analyze: true });
            res.writeHead(200, { 'Content-Type': 'text/html', 'Access-Control-Allow-Origin': '*' });
            res.end(html);
          } catch (e) {
            // Fallback: return text plan
            res.writeHead(200, { 'Content-Type': 'text/plain', 'Access-Control-Allow-Origin': '*' });
            res.end(result.planTreeText.join('\n'));
          }
        } else {
          res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
          res.end(JSON.stringify(result));
        }
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
  }

  _handleQueryEndpoint(req, res) {
    let body = '';
    req.on('data', chunk => body += chunk);
    req.on('end', () => {
      try {
        const { query } = JSON.parse(body);
        if (!query) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Missing "query" field' }));
          return;
        }
        const result = this.db.execute(query);
        res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
        res.end(JSON.stringify(result));
      } catch (e) {
        res.writeHead(400, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
  }

  _serveExplainUI(req, res) {
    const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>HenryDB — EXPLAIN Visualizer</title>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; background: #FAFAFA; color: #333; }
    h1 { color: #1565C0; }
    textarea { width: 100%; height: 120px; font-family: 'SF Mono', monospace; font-size: 14px; padding: 12px; border: 2px solid #E0E0E0; border-radius: 8px; resize: vertical; }
    textarea:focus { border-color: #1565C0; outline: none; }
    .btn { background: #1565C0; color: white; border: none; padding: 10px 24px; border-radius: 6px; font-size: 14px; cursor: pointer; margin: 8px 4px 8px 0; }
    .btn:hover { background: #0D47A1; }
    .btn-analyze { background: #2E7D32; }
    .btn-analyze:hover { background: #1B5E20; }
    #result { margin-top: 20px; }
    .error { color: #C62828; background: #FFEBEE; padding: 12px; border-radius: 8px; }
    .info { color: #666; font-size: 13px; margin-top: 8px; }
  </style>
</head>
<body>
  <h1>🔍 HenryDB EXPLAIN Visualizer</h1>
  <p class="info">Enter a SELECT query to see its execution plan as an interactive SVG tree.</p>
  <textarea id="query" placeholder="SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE u.active = 1 ORDER BY o.total DESC LIMIT 10">SELECT * FROM orders o JOIN users u ON o.user_id = u.id ORDER BY o.total DESC LIMIT 10</textarea>
  <br>
  <button class="btn" onclick="explain(false)">EXPLAIN</button>
  <button class="btn btn-analyze" onclick="explain(true)">EXPLAIN ANALYZE</button>
  <div id="result"></div>
  <script>
    async function explain(analyze) {
      const query = document.getElementById('query').value.trim();
      if (!query) return;
      const res = await fetch('/explain', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query, analyze })
      });
      const el = document.getElementById('result');
      if (res.headers.get('content-type')?.includes('text/html')) {
        el.innerHTML = await res.text();
      } else {
        const data = await res.json();
        if (data.error) {
          el.innerHTML = '<div class="error">' + data.error + '</div>';
        } else {
          el.innerHTML = '<pre>' + JSON.stringify(data, null, 2) + '</pre>';
        }
      }
    }
  </script>
</body>
</html>`;
    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(html);
  }

  _serveDashboard(req, res) {
    const health = this._getHealthStatus();
    const cacheStats = this.db._planCache ? this.db._planCache.stats() : {};
    const indexRecs = this.db._indexAdvisor ? this.db._indexAdvisor.recommend() : [];
    const slowQueries = this.db._queryStats ? this.db._queryStats.getSlowest(10) : [];
    const queryStatsSummary = this.db._queryStats ? this.db._queryStats.summary() : {};
    const tableCount = this.db.tables?.size || 0;
    const indexCount = this.db.indexCatalog?.size || 0;

    const html = `<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>HenryDB — Performance Dashboard</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #F5F5F5; color: #333; }
    h1 { color: #1565C0; margin-bottom: 4px; }
    .subtitle { color: #666; font-size: 14px; margin-bottom: 24px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); }
    .card h3 { margin: 0 0 4px 0; font-size: 13px; color: #666; text-transform: uppercase; letter-spacing: 0.5px; }
    .card .value { font-size: 32px; font-weight: bold; color: #1565C0; }
    .card .sub { font-size: 12px; color: #999; margin-top: 4px; }
    .section { background: white; border-radius: 12px; padding: 20px; box-shadow: 0 1px 4px rgba(0,0,0,0.08); margin-bottom: 16px; }
    .section h2 { margin: 0 0 12px 0; color: #333; font-size: 18px; }
    table { width: 100%; border-collapse: collapse; }
    th { text-align: left; padding: 8px 12px; border-bottom: 2px solid #E0E0E0; font-size: 12px; text-transform: uppercase; color: #666; }
    td { padding: 8px 12px; border-bottom: 1px solid #F0F0F0; font-size: 13px; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: bold; }
    .badge-high { background: #FFCDD2; color: #C62828; }
    .badge-medium { background: #FFE0B2; color: #E65100; }
    .badge-low { background: #C8E6C9; color: #2E7D32; }
    code { background: #ECEFF1; padding: 2px 6px; border-radius: 4px; font-size: 12px; }
    .nav { display: flex; gap: 12px; margin-bottom: 20px; }
    .nav a { color: #1565C0; text-decoration: none; font-size: 14px; }
    .nav a:hover { text-decoration: underline; }
    .empty { color: #999; font-style: italic; padding: 12px 0; }
  </style>
</head>
<body>
  <h1>📊 HenryDB Performance Dashboard</h1>
  <p class="subtitle">Real-time database health, query plan cache, and index recommendations</p>
  
  <div class="nav">
    <a href="/explain">🔍 EXPLAIN Visualizer</a>
    <a href="/health">❤️ Health Check</a>
    <a href="/metrics">📈 Prometheus Metrics</a>
  </div>

  <div class="grid">
    <div class="card">
      <h3>Tables</h3>
      <div class="value">${tableCount}</div>
    </div>
    <div class="card">
      <h3>Indexes</h3>
      <div class="value">${indexCount}</div>
    </div>
    <div class="card">
      <h3>Plan Cache</h3>
      <div class="value">${cacheStats.entries || 0}</div>
      <div class="sub">Hit rate: ${cacheStats.hitRate || 0}%</div>
    </div>
    <div class="card">
      <h3>Cache Hits / Misses</h3>
      <div class="value">${cacheStats.hits || 0} / ${cacheStats.misses || 0}</div>
    </div>
    <div class="card">
      <h3>Connections</h3>
      <div class="value">${health.connections?.active || 0}</div>
      <div class="sub">Peak: ${health.connections?.peak || 0}</div>
    </div>
    <div class="card">
      <h3>Unique Queries</h3>
      <div class="value">${queryStatsSummary.uniqueQueries || 0}</div>
      <div class="sub">${queryStatsSummary.totalCalls || 0} total calls</div>
    </div>
  </div>

  <div class="section">
    <h2>🎯 Index Recommendations</h2>
    ${indexRecs.length === 0 ? '<p class="empty">No recommendations yet. Run more queries to build workload profile.</p>' : `
    <table>
      <thead><tr><th>Table</th><th>Columns</th><th>Impact</th><th>Score</th><th>Reason</th><th>SQL</th></tr></thead>
      <tbody>
        ${indexRecs.slice(0, 10).map(r => `<tr>
          <td>${esc(r.table)}</td>
          <td>${esc(r.columns.join(', '))}</td>
          <td><span class="badge badge-${r.level}">${r.level}</span></td>
          <td>${r.impact}</td>
          <td>${esc(r.reason)}</td>
          <td><code>${esc(r.sql)}</code></td>
        </tr>`).join('')}
      </tbody>
    </table>`}
  </div>

  <div class="section">
    <h2>🐌 Slowest Queries</h2>
    ${slowQueries.length === 0 ? '<p class="empty">No query statistics yet.</p>' : `
    <table>
      <thead><tr><th>Query</th><th>Calls</th><th>Mean Time</th><th>Total Time</th><th>Mean Rows</th><th>Errors</th></tr></thead>
      <tbody>
        ${slowQueries.map(q => `<tr>
          <td><code>${esc(q.query.substring(0, 80))}${q.query.length > 80 ? '…' : ''}</code></td>
          <td>${q.calls}</td>
          <td>${q.mean_time_ms.toFixed(3)}ms</td>
          <td>${q.total_time_ms.toFixed(1)}ms</td>
          <td>${q.mean_rows}</td>
          <td>${q.errors || 0}</td>
        </tr>`).join('')}
      </tbody>
    </table>
    <p class="empty">Total: ${queryStatsSummary.uniqueQueries || 0} unique queries, ${queryStatsSummary.totalCalls || 0} calls, ${(queryStatsSummary.totalTimeMs || 0).toFixed(1)}ms total</p>`}
  </div>

  <div class="section">
    <h2>📋 Tables</h2>
    <table>
      <thead><tr><th>Name</th><th>Columns</th><th>Rows</th><th>Engine</th></tr></thead>
      <tbody>
        ${[...this.db.tables.entries()].map(([name, table]) => {
          const cols = table.schema?.length || table.columns?.length || 0;
          const rows = table.heap?._rowCount || table.heap?.tupleCount || 0;
          const engine = table.heap?.constructor?.name || 'heap';
          return `<tr><td>${esc(name)}</td><td>${cols}</td><td>${rows}</td><td>${engine}</td></tr>`;
        }).join('')}
      </tbody>
    </table>
  </div>
</body>
</html>`;

    function esc(s) { return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); }

    res.writeHead(200, { 'Content-Type': 'text/html' });
    res.end(html);
  }

  /**
   * Execute SQL for a connection, routing through transaction session if in-transaction.
   */
  _connExecute(conn, sql) {
    // If connection has an active transaction session, route through it
    if (conn._session && conn.txStatus === 'T') {
      return conn._session.execute(sql);
    }
    // Otherwise, use the default database
    return this.db.execute(sql);
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
      _session: this._txDb ? this._txDb.session() : null, // TransactionSession for BEGIN/COMMIT
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
            this._sendResult(conn, stmt, cached.result);
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
              const firstRowKeys = Object.keys(adaptive.rows[0]);
              if (adaptive.rows && adaptive.rows.length > 0 && firstRowKeys.length > 0 && !firstRowKeys.includes('undefined')) {
                result = { type: 'ROWS', rows: adaptive.rows, _engine: adaptive.engine, _timeMs: adaptive.timeMs };
              } else {
                // Adaptive engine returned empty-keyed rows; fall back to Volcano
                result = this._connExecute(conn, stmt);
              }
            } else {
              result = this._connExecute(conn, stmt);
            }
          } catch (e) {
            // Fallback to standard execution if adaptive fails
            result = this._connExecute(conn, stmt);
          }
        } else {
          result = this._connExecute(conn, stmt);
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
      
      // Log all queries for pg_stat_slow_queries
      this._queryLog.push({
        pid: conn.pid,
        user: conn.user || 'unknown',
        query: conn.currentQuery.sql.substring(0, 500),
        duration_ms: duration,
        timestamp: new Date().toISOString(),
        rows: 0,
      });
      if (this._queryLog.length > this._queryLogMaxSize) {
        this._queryLog.shift();
      }

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
    // Don't send ReadyForQuery during COPY — the client is in COPY mode
    if (!conn.copyState) {
      conn.socket.write(writeReadyForQuery(conn.txStatus));
    }
  }

  _sendResult(conn, sql, result, options = {}) {
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

      // Send RowDescription (skip for Extended Query — Describe already sent it)
      if (!options.skipRowDescription) {
        const colDescs = columnNames.map((name, i) => ({
          name,
          typeOid: inferTypeOid(sampleValues[i]),
          typeSize: -1,
        }));
        conn.socket.write(writeRowDescription(colDescs));
      }

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
        } else if (upper.startsWith('COPY ') && upper.includes('FROM STDIN')) {
          // COPY FROM STDIN through Extended Query — handle specially
          const match = sql.match(/COPY\s+(\w+)\s*(?:\(([^)]+)\))?\s+FROM\s+STDIN/i);
          if (match) {
            const table = match[1];
            const tableObj = this.db.tables.get(table);
            if (tableObj) {
              const columns = match[2]
                ? match[2].split(',').map(c => c.trim())
                : tableObj.schema.map(c => c.name);
              conn.copyState = { table, columns, buffer: '', rowCount: 0 };
              // Don't store result — we'll send CopyInResponse in Execute
              result = { type: 'COPY_IN', columnCount: columns.length };
            } else {
              result = { type: 'ERROR', message: `Table "${table}" not found` };
            }
          }
        } else {
          result = this._connExecute(conn, sql);
          // Invalidate cache on mutations (same logic as simple query path)
          const isSelectExt = /^\s*SELECT/i.test(sql);
          if (!isSelectExt) {
            const tableMatchExt = sql.match(/(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM|DROP\s+TABLE|ALTER\s+TABLE|TRUNCATE)\s+(\w+)/i);
            if (tableMatchExt) {
              this._queryCache.invalidate(tableMatchExt[1]);
            }
          }
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

      // COPY FROM STDIN: send CopyInResponse instead of normal result
      if (result.type === 'COPY_IN') {
        const numCols = result.columnCount;
        const buf = Buffer.alloc(1 + 4 + 1 + 2 + numCols * 2);
        buf[0] = 0x47; // 'G' CopyInResponse
        buf.writeInt32BE(4 + 1 + 2 + numCols * 2, 1);
        buf[5] = 0; // text format
        buf.writeInt16BE(numCols, 6);
        for (let i = 0; i < numCols; i++) {
          buf.writeInt16BE(0, 8 + i * 2); // text format for each column
        }
        conn.socket.write(buf);
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
    // No aggregates, window functions, or expressions
    if (ast.columns?.some(c => c.type === 'aggregate' || c.type === 'function_call' || c.type === 'window' || c.type === 'expression' || c.type === 'function')) return false;
    // Only use adaptive for simple WHERE conditions (no LIKE, IN subquery, etc.)
    if (ast.where && this._hasComplexPredicate(ast.where)) return false;
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
    
    // Transaction control: BEGIN/COMMIT/ROLLBACK
    if (upper === 'BEGIN' || upper === 'BEGIN TRANSACTION' || upper === 'START TRANSACTION') {
      if (conn._session) {
        try {
          conn._session.begin();
          conn.txStatus = 'T';
          this._sendResult(conn, sql, { type: 'OK', message: 'BEGIN' });
        } catch (e) {
          conn.txStatus = 'E';
          conn.socket.write(writeErrorResponse('ERROR', '25001', e.message));
        }
      } else {
        // No TransactionalDatabase — fake BEGIN (just set status)
        conn.txStatus = 'T';
        conn._txBuffer = []; // Buffer statements for "best effort" transaction
        this._sendResult(conn, sql, { type: 'OK', message: 'BEGIN' });
      }
      return true;
    }
    
    if (upper === 'COMMIT' || upper === 'END') {
      if (conn._session) {
        try {
          conn._session.commit();
          conn.txStatus = 'I';
          // Invalidate query cache — committed changes may affect cached results
          this._queryCache.invalidateAll();
          this._sendResult(conn, sql, { type: 'OK', message: 'COMMIT' });
        } catch (e) {
          conn.txStatus = 'E';
          conn.socket.write(writeErrorResponse('ERROR', '40001', e.message));
        }
      } else {
        conn.txStatus = 'I';
        conn._txBuffer = null;
        this._sendResult(conn, sql, { type: 'OK', message: 'COMMIT' });
      }
      return true;
    }
    
    if (upper === 'ROLLBACK' || upper === 'ABORT') {
      if (conn._session) {
        try {
          conn._session.rollback();
        } catch (e) {
          // Rollback shouldn't fail, but if it does, ignore
        }
      }
      conn.txStatus = 'I';
      conn._txBuffer = null;
      // Invalidate query cache — rolled-back mutations may have polluted cached results
      this._queryCache.invalidateAll();
      this._sendResult(conn, sql, { type: 'OK', message: 'ROLLBACK' });
      return true;
    }

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
      this._sendResult(conn, sql, { type: 'ROWS', rows: [...this._queryLog].reverse() });
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
      // Try to detect \d tablename pattern (pg_attribute + relname)
      const relMatch = sql.match(/relname\s*=\s*'([^']+)'/i) || sql.match(/c\.relname\s*=\s*\$\d/i);
      if (upper.includes('PG_ATTRIBUTE') && relMatch) {
        // \d tablename — return column info from our schema
        const tableName = relMatch[1];
        const table = this.db.tables.get(tableName);
        if (table && table.schema) {
          const rows = table.schema.map((col, idx) => ({
            attname: col.name,
            format_type: col.type || 'text',
            attnotnull: col.notNull ? true : false,
            atthasdef: col.default !== undefined && col.default !== null,
            attnum: idx + 1,
            atttypid: 25, // text OID
            atttypmod: -1,
            attlen: -1,
            attidentity: '',
            attgenerated: '',
            attisdropped: false,
          }));
          this._sendResult(conn, sql, { type: 'ROWS', rows });
          return true;
        }
      }
      
      // pg_tables query
      if (upper.includes('PG_TABLES') || (upper.includes('PG_CLASS') && upper.includes('RELKIND'))) {
        const rows = [];
        for (const [name] of this.db.tables) {
          rows.push({ tablename: name, schemaname: 'public', tableowner: 'henrydb', tablespace: null });
        }
        this._sendResult(conn, sql, { type: 'ROWS', rows });
        return true;
      }
      
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
        if (conn._session && conn.txStatus === 'T') {
          conn._session.savepoint(name);
        }
      } catch (e) { /* ignore if not in tx */ }
      conn.socket.write(writeCommandComplete('SAVEPOINT'));
      return true;
    }

    // RELEASE SAVEPOINT
    if (upper.startsWith('RELEASE SAVEPOINT') || upper.startsWith('RELEASE ')) {
      const name = sql.replace(/RELEASE\s+(?:SAVEPOINT\s+)?/i, '').trim().replace(/;$/, '');
      try {
        if (conn._session && conn.txStatus === 'T') {
          conn._session.releaseSavepoint(name);
        }
      } catch (e) { /* ignore */ }
      conn.socket.write(writeCommandComplete('RELEASE'));
      return true;
    }

    // ROLLBACK TO SAVEPOINT
    if (upper.startsWith('ROLLBACK TO')) {
      const name = sql.replace(/ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?/i, '').trim().replace(/;$/, '');
      try {
        if (conn._session && conn.txStatus === 'T') {
          conn._session.rollbackToSavepoint(name);
        }
        // Invalidate cache on partial rollback
        this._queryCache.invalidateAll();
      } catch (e) { /* ignore */ }
      conn.socket.write(writeCommandComplete('ROLLBACK'));
      return true;
    }

    // VACUUM [ANALYZE] [table_name]
    if (upper.startsWith('VACUUM')) {
      try {
        const result = this.db.execute(sql);
        if (result && result.type === 'ROWS' && result.rows) {
          this._sendResult(conn, sql, result);
        } else {
          conn.socket.write(writeCommandComplete('VACUUM'));
        }
      } catch (e) {
        // Fallback: manual vacuum
        const tables = [...this.db.tables.keys()];
        for (const t of tables) {
          const table = this.db.tables.get(t);
          if (table && table.heap && typeof table.heap.compact === 'function') {
            try { table.heap.compact(); } catch (_) {}
          }
        }
        conn.socket.write(writeCommandComplete('VACUUM'));
      }
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

  _hasComplexPredicate(node) {
    if (!node) return false;
    if (node.type === 'LIKE' || node.type === 'NOT_LIKE' || node.type === 'ILIKE' || node.type === 'IN' || 
        node.type === 'NOT_IN' || node.type === 'IN_LIST' || node.type === 'NOT_IN_LIST' ||
        node.type === 'EXISTS' || node.type === 'BETWEEN' ||
        node.type === 'IS_NULL' || node.type === 'IS_NOT_NULL' || node.type === 'SUBQUERY') return true;
    if (node.left && this._hasComplexPredicate(node.left)) return true;
    if (node.right && this._hasComplexPredicate(node.right)) return true;
    return false;
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
  const args = process.argv.slice(2);
  let port = DEFAULT_PORT;
  let dataDir = null;
  let verbose = true;
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--data-dir' && args[i + 1]) {
      dataDir = args[++i];
    } else if (args[i] === '--port' && args[i + 1]) {
      port = parseInt(args[++i]);
    } else if (args[i] === '--quiet') {
      verbose = false;
    } else if (!args[i].startsWith('-')) {
      port = parseInt(args[i]) || port;
    }
  }
  
  const opts = { port, verbose };
  if (dataDir) {
    opts.dataDir = dataDir;
    console.log(`Using persistent storage: ${dataDir}`);
  } else {
    console.log('Running in-memory mode (data lost on restart). Use --data-dir <path> for persistence.');
  }
  
  const server = new HenryDBServer(opts);
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
