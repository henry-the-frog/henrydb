// replication.js — Streaming replication for HenryDB
// Primary-Replica architecture using LISTEN/NOTIFY + WAL replay.
//
// Primary: writes WAL, notifies replicas of each change
// Replica: subscribes to primary, receives and replays changes

import pg from 'pg';
import { HenryDBServer } from './server.js';
import { Database } from './db.js';

const REPLICATION_CHANNEL = '__henrydb_replication';

/**
 * Replication Publisher — runs on the primary server.
 * Intercepts Database mutations and publishes them via NOTIFY.
 */
export class ReplicationPublisher {
  constructor(server) {
    this.server = server;
    this._enabled = false;
    this._seqNo = 0;
  }

  /**
   * Enable replication — wraps the database to publish mutations.
   */
  enable() {
    if (this._enabled) return;
    this._enabled = true;

    // Hook into the server to broadcast WAL-like events
    const origExecute = this.server.db.execute.bind(this.server.db);
    const self = this;

    this.server.db.execute = function(sql) {
      const result = origExecute(sql);
      
      // Broadcast DDL and DML operations
      const upper = sql.toUpperCase().trim();
      if (upper.startsWith('CREATE TABLE') || upper.startsWith('INSERT') || 
          upper.startsWith('UPDATE') || upper.startsWith('DELETE') ||
          upper.startsWith('DROP TABLE')) {
        self._broadcast(sql);
      }
      
      return result;
    };
  }

  _broadcast(sql) {
    this._seqNo++;
    const payload = JSON.stringify({ seq: this._seqNo, sql, ts: Date.now() });
    
    // Send to all connections listening on replication channel
    const listeners = this.server._channels.get(REPLICATION_CHANNEL);
    if (listeners && listeners.size > 0) {
      for (const conn of listeners) {
        try {
          const channelBuf = Buffer.from(REPLICATION_CHANNEL + '\0', 'utf8');
          const payloadBuf = Buffer.from(payload + '\0', 'utf8');
          const len = 4 + 4 + channelBuf.length + payloadBuf.length;
          const buf = Buffer.alloc(1 + len);
          buf[0] = 0x41; // 'A' NotificationResponse
          buf.writeInt32BE(len, 1);
          buf.writeInt32BE(0, 5); // pid 0 for replication
          channelBuf.copy(buf, 9);
          payloadBuf.copy(buf, 9 + channelBuf.length);
          conn.socket.write(buf);
        } catch (e) {
          // Listener might be dead
        }
      }
    }
  }

  getStats() {
    return {
      enabled: this._enabled,
      seqNo: this._seqNo,
      replicaCount: this.server._channels.get(REPLICATION_CHANNEL)?.size || 0,
    };
  }
}

/**
 * Replication Subscriber — runs on the replica.
 * Connects to primary and replays SQL operations.
 */
export class ReplicationSubscriber {
  constructor(replicaDb, primaryConfig) {
    this.replicaDb = replicaDb;
    this.primaryConfig = primaryConfig;
    this.client = null;
    this._running = false;
    this._lastSeq = 0;
    this.stats = {
      operationsReceived: 0,
      operationsApplied: 0,
      errors: 0,
      lagMs: 0,
    };
    this._onError = null;
  }

  /**
   * Connect to primary and start replicating.
   */
  async start() {
    this.client = new pg.Client(this.primaryConfig);
    await this.client.connect();
    
    // Subscribe to replication channel
    await this.client.query(`LISTEN ${REPLICATION_CHANNEL}`);
    
    this.client.on('notification', (msg) => {
      if (msg.channel === REPLICATION_CHANNEL) {
        this._handleReplicationEvent(msg.payload);
      }
    });

    this._running = true;
  }

  /**
   * Stop replicating and disconnect.
   */
  async stop() {
    this._running = false;
    if (this.client) {
      try {
        await this.client.query(`UNLISTEN ${REPLICATION_CHANNEL}`);
        await this.client.end();
      } catch (e) {
        // Might already be disconnected
      }
      this.client = null;
    }
  }

  _handleReplicationEvent(payload) {
    try {
      const event = JSON.parse(payload);
      this.stats.operationsReceived++;
      this.stats.lagMs = Date.now() - event.ts;

      // Check sequence ordering
      if (event.seq <= this._lastSeq) {
        return; // Duplicate or out-of-order — skip
      }
      this._lastSeq = event.seq;

      // Replay the SQL on the replica
      try {
        this.replicaDb.execute(event.sql);
        this.stats.operationsApplied++;
      } catch (e) {
        this.stats.errors++;
        if (this._onError) this._onError(event, e);
      }
    } catch (e) {
      this.stats.errors++;
    }
  }

  onError(handler) {
    this._onError = handler;
    return this;
  }
}

export default { ReplicationPublisher, ReplicationSubscriber };
export { REPLICATION_CHANNEL };
