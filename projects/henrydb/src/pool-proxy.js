// pool-proxy.js — Connection pool proxy for HenryDB
// Accepts N client TCP connections, multiplexes onto M database connections (N:M pooling).
// Similar to PgBouncer — sits between clients and HenryDB server.
// Supports transaction-level pooling (client gets dedicated backend for duration of transaction).

import net from 'node:net';
import {
  writeAuthenticationOk, writeParameterStatus, writeBackendKeyData,
  writeReadyForQuery, writeRowDescription, writeDataRow,
  writeCommandComplete, writeErrorResponse,
  parseStartupMessage,
  PG_TYPES, inferTypeOid,
} from './pg-protocol.js';

/**
 * PooledBackend — represents a connection to the real HenryDB server.
 */
class PooledBackend {
  constructor(id, db) {
    this.id = id;
    this.db = db;
    this.inTransaction = false;
    this.assignedClient = null;
    this.queriesExecuted = 0;
    this.createdAt = Date.now();
    this.lastActiveAt = Date.now();
  }

  execute(sql) {
    this.lastActiveAt = Date.now();
    this.queriesExecuted++;
    const upper = sql.trim().toUpperCase();
    if (upper === 'BEGIN' || upper.startsWith('BEGIN ')) {
      this.inTransaction = true;
    } else if (upper === 'COMMIT' || upper === 'ROLLBACK') {
      this.inTransaction = false;
      this.assignedClient = null;
    }
    return this.db.execute(sql);
  }
}

/**
 * ClientSession — tracks state for one connected client.
 */
class ClientSession {
  constructor(id, socket) {
    this.id = id;
    this.socket = socket;
    this.backend = null;
    this.inTransaction = false;
    this.queriesExecuted = 0;
    this.connectedAt = Date.now();
    this.lastActiveAt = Date.now();
    this.pendingQueue = [];
    this.authenticated = false;
    this.parameters = {};
  }
}

/**
 * PoolProxy — N:M connection pool proxy.
 */
export class PoolProxy {
  constructor(options = {}) {
    this.db = options.db;
    this.port = options.port || 6433;
    this.host = options.host || '127.0.0.1';
    this.poolSize = options.poolSize || 5;
    this.maxClients = options.maxClients || 50;
    this.poolMode = options.poolMode || 'transaction';
    
    this.server = null;
    this._backends = new Map();
    this._availableBackends = [];
    this._clients = new Map();
    this._waitingClients = [];
    this._nextBackendId = 1;
    this._nextClientId = 1;
    
    this._metrics = {
      totalClients: 0,
      peakClients: 0,
      totalQueries: 0,
      totalWaits: 0,
      totalErrors: 0,
      queryLatencySum: 0,
      waitTimeSum: 0,
    };

    // Pre-create backend connections
    for (let i = 0; i < this.poolSize; i++) {
      const backend = new PooledBackend(this._nextBackendId++, this.db);
      this._backends.set(backend.id, backend);
      this._availableBackends.push(backend);
    }
  }

  start() {
    return new Promise((resolve) => {
      this.server = net.createServer(socket => this._handleClient(socket));
      this.server.listen(this.port, this.host, () => resolve(this));
    });
  }

  stop() {
    return new Promise((resolve) => {
      for (const client of this._clients.values()) {
        try { client.socket.destroy(); } catch {}
      }
      this._clients.clear();
      this._backends.clear();
      this._availableBackends = [];
      if (this.server) {
        this.server.close(() => resolve());
      } else {
        resolve();
      }
    });
  }

  _handleClient(socket) {
    if (this._clients.size >= this.maxClients) {
      try {
        socket.write(writeErrorResponse('FATAL', '53300', 'too many connections'));
        socket.end();
      } catch {}
      return;
    }

    const client = new ClientSession(this._nextClientId++, socket);
    this._clients.set(client.id, client);
    this._metrics.totalClients++;
    if (this._clients.size > this._metrics.peakClients) {
      this._metrics.peakClients = this._clients.size;
    }

    let buffer = Buffer.alloc(0);
    let startupDone = false;

    socket.on('data', (data) => {
      buffer = Buffer.concat([buffer, data]);

      if (!startupDone) {
        if (buffer.length < 4) return;
        const len = buffer.readInt32BE(0);
        if (buffer.length < len) return;

        const startupData = buffer.subarray(0, len);
        buffer = buffer.subarray(len);

        // Check for SSL request
        if (len === 8 && startupData.readInt32BE(4) === 80877103) {
          socket.write(Buffer.from('N'));
          // Don't set startupDone — wait for real startup
          return;
        }

        startupDone = true;
        const startup = parseStartupMessage(startupData);
        client.parameters = startup.parameters || {};
        client.authenticated = true;

        // Send auth OK + parameters + ready
        socket.write(Buffer.concat([
          writeAuthenticationOk(),
          writeParameterStatus('server_version', '16.0 (HenryDB PoolProxy)'),
          writeParameterStatus('server_encoding', 'UTF8'),
          writeParameterStatus('client_encoding', 'UTF8'),
          writeParameterStatus('DateStyle', 'ISO, MDY'),
          writeParameterStatus('integer_datetimes', 'on'),
          writeBackendKeyData(client.id, 0),
          writeReadyForQuery('I'),
        ]));
        return;
      }

      // Process query messages
      while (buffer.length >= 5) {
        const msgType = String.fromCharCode(buffer[0]);
        const msgLen = buffer.readInt32BE(1);
        if (buffer.length < 1 + msgLen) break;

        const msgBody = buffer.subarray(5, 1 + msgLen);
        buffer = buffer.subarray(1 + msgLen);

        if (msgType === 'X') {
          this._disconnectClient(client);
          return;
        }

        if (msgType === 'Q') {
          const nullIdx = msgBody.indexOf(0);
          const query = msgBody.subarray(0, nullIdx >= 0 ? nullIdx : msgBody.length).toString('utf8');
          this._handleQuery(client, query);
        }
      }
    });

    socket.on('error', () => this._disconnectClient(client));
    socket.on('close', () => this._disconnectClient(client));
  }

  _handleQuery(client, sql) {
    const startTime = Date.now();
    client.lastActiveAt = Date.now();
    client.queriesExecuted++;
    this._metrics.totalQueries++;

    const upper = sql.trim().toUpperCase();
    const isBegin = upper === 'BEGIN' || upper.startsWith('BEGIN ');
    const isEnd = upper === 'COMMIT' || upper === 'ROLLBACK';

    let backend = client.backend;

    if (!backend) {
      if (this._availableBackends.length > 0) {
        backend = this._availableBackends.pop();
        backend.assignedClient = client.id;
        client.backend = backend;
      } else {
        // No backends available — queue
        this._metrics.totalWaits++;
        client.pendingQueue.push({ sql, waitStart: Date.now() });
        this._waitingClients.push(client.id);
        return;
      }
    }

    try {
      const result = backend.execute(sql);
      const elapsed = Date.now() - startTime;
      this._metrics.queryLatencySum += elapsed;

      if (isBegin) client.inTransaction = true;

      this._sendResult(client.socket, sql, result, client.inTransaction);

      if (isEnd) client.inTransaction = false;

      this._maybeReleaseBackend(client);

    } catch (err) {
      this._metrics.totalErrors++;
      try {
        socket_write_safe(client.socket, Buffer.concat([
          writeErrorResponse('ERROR', '42000', err.message),
          writeReadyForQuery(client.inTransaction ? 'T' : 'I'),
        ]));
      } catch {}

      if (isEnd || !client.inTransaction) {
        this._maybeReleaseBackend(client);
      }
    }
  }

  _sendResult(socket, sql, result, inTransaction) {
    const upper = sql.trim().toUpperCase();
    const txStatus = inTransaction ? 'T' : 'I';
    // BEGIN puts us in transaction after this statement
    const effectiveTxStatus = (upper === 'BEGIN' || upper.startsWith('BEGIN ')) ? 'T'
      : (upper === 'COMMIT' || upper === 'ROLLBACK') ? 'I'
      : txStatus;

    if (!result || (!result.rows && !result.changes && !Array.isArray(result))) {
      const tag = upper.startsWith('CREATE') ? 'CREATE TABLE'
        : upper.startsWith('DROP') ? 'DROP TABLE'
        : upper.startsWith('BEGIN') ? 'BEGIN'
        : upper.startsWith('COMMIT') ? 'COMMIT'
        : upper.startsWith('ROLLBACK') ? 'ROLLBACK'
        : 'OK';
      socket_write_safe(socket, Buffer.concat([
        writeCommandComplete(tag),
        writeReadyForQuery(effectiveTxStatus),
      ]));
      return;
    }

    const rows = result.rows || result || [];
    const bufs = [];

    if (rows.length > 0) {
      const cols = Object.keys(rows[0]);
      const fields = cols.map(name => ({
        name,
        tableOid: 0, colAttr: 0, typeOid: inferTypeOid(rows[0][name]),
        typeLen: -1, typeMod: -1, format: 0,
      }));
      bufs.push(writeRowDescription(fields));
      for (const row of rows) {
        const values = cols.map(c => row[c] == null ? null : String(row[c]));
        bufs.push(writeDataRow(values));
      }
    }

    const tag = upper.startsWith('SELECT') ? `SELECT ${rows.length}`
      : upper.startsWith('INSERT') ? `INSERT 0 ${result.changes || rows.length}`
      : upper.startsWith('UPDATE') ? `UPDATE ${result.changes || 0}`
      : upper.startsWith('DELETE') ? `DELETE ${result.changes || 0}`
      : `OK`;
    bufs.push(writeCommandComplete(tag));
    bufs.push(writeReadyForQuery(effectiveTxStatus));
    socket_write_safe(socket, Buffer.concat(bufs));
  }

  _maybeReleaseBackend(client) {
    if (this.poolMode === 'session') return;
    if (this.poolMode === 'statement') {
      this._releaseBackend(client);
      return;
    }
    // Transaction mode
    if (!client.inTransaction) {
      this._releaseBackend(client);
    }
  }

  _releaseBackend(client) {
    const backend = client.backend;
    if (!backend) return;

    client.backend = null;
    backend.assignedClient = null;

    // Check waiting clients
    while (this._waitingClients.length > 0) {
      const waitingId = this._waitingClients.shift();
      const waitingClient = this._clients.get(waitingId);
      if (!waitingClient || waitingClient.socket.destroyed) continue;

      if (waitingClient.pendingQueue.length > 0) {
        waitingClient.backend = backend;
        backend.assignedClient = waitingClient.id;

        const pending = waitingClient.pendingQueue.shift();
        const waitMs = Date.now() - pending.waitStart;
        this._metrics.waitTimeSum += waitMs;
        this._handleQuery(waitingClient, pending.sql);
        return;
      }
    }

    this._availableBackends.push(backend);
  }

  _disconnectClient(client) {
    if (!this._clients.has(client.id)) return;

    if (client.backend && client.inTransaction) {
      try { client.backend.execute('ROLLBACK'); } catch {}
      client.backend.inTransaction = false;
      client.inTransaction = false;
    }

    if (client.backend) {
      this._releaseBackend(client);
    }

    this._waitingClients = this._waitingClients.filter(id => id !== client.id);
    this._clients.delete(client.id);
    try { client.socket.destroy(); } catch {}
  }

  getStats() {
    const activeBackends = [...this._backends.values()].filter(b => b.assignedClient !== null).length;
    return {
      clients: {
        active: this._clients.size,
        peak: this._metrics.peakClients,
        total: this._metrics.totalClients,
        waiting: this._waitingClients.length,
      },
      backends: {
        total: this._backends.size,
        active: activeBackends,
        idle: this._availableBackends.length,
      },
      pool: {
        mode: this.poolMode,
        utilization: this._backends.size > 0
          ? (activeBackends / this._backends.size * 100).toFixed(1) + '%'
          : '0%',
        utilizationPct: this._backends.size > 0
          ? activeBackends / this._backends.size
          : 0,
        ratio: `${this._clients.size}:${this._backends.size}`,
      },
      queries: {
        total: this._metrics.totalQueries,
        errors: this._metrics.totalErrors,
        avgLatencyMs: this._metrics.totalQueries > 0
          ? +(this._metrics.queryLatencySum / this._metrics.totalQueries).toFixed(2)
          : 0,
      },
      waits: {
        total: this._metrics.totalWaits,
        avgWaitMs: this._metrics.totalWaits > 0
          ? +(this._metrics.waitTimeSum / this._metrics.totalWaits).toFixed(2)
          : 0,
      },
    };
  }
}

function socket_write_safe(socket, buf) {
  if (!socket.destroyed) socket.write(buf);
}
