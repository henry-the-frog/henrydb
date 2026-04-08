// wal-replay.js — WAL Replay Engine for HenryDB
// Reconstructs database state from WAL records after crash recovery.

import { WALReader, RECORD_TYPES } from './wal.js';

/**
 * Replays WAL records against a Database instance.
 * Handles transaction tracking — only committed transactions' changes are applied.
 */
export class WALReplayEngine {
  constructor(db) {
    this.db = db;
    this.stats = {
      recordsProcessed: 0,
      recordsApplied: 0,
      recordsSkipped: 0,
      transactionsReplayed: 0,
      transactionsRolledBack: 0,
      tablesCreated: 0,
      tablesDropped: 0,
      rowsInserted: 0,
      rowsUpdated: 0,
      rowsDeleted: 0,
    };
  }

  /**
   * Two-pass replay: first identify committed transactions, then apply only those.
   * This handles the case where a transaction started but didn't commit before crash.
   */
  replay(records) {
    const allRecords = Array.isArray(records) ? records : [...records];

    // Pass 1: Identify committed transactions
    const committedTxIds = new Set();
    const rolledBackTxIds = new Set();
    
    for (const record of allRecords) {
      if (record.type === 'COMMIT') {
        committedTxIds.add(record.payload.txId);
      } else if (record.type === 'ROLLBACK') {
        rolledBackTxIds.add(record.payload.txId);
      }
    }

    // Pass 2: Apply records from committed transactions (and non-transactional DDL)
    for (const record of allRecords) {
      this.stats.recordsProcessed++;
      const txId = record.payload?.txId;

      // Skip records from uncommitted or rolled-back transactions
      if (txId !== undefined && txId !== null) {
        if (!committedTxIds.has(txId)) {
          this.stats.recordsSkipped++;
          continue;
        }
      }

      try {
        this._applyRecord(record);
        this.stats.recordsApplied++;
      } catch (err) {
        // Log but continue — partial recovery is better than no recovery
        if (this._onError) {
          this._onError(record, err);
        }
      }
    }

    // Count transactions
    this.stats.transactionsReplayed = committedTxIds.size;
    this.stats.transactionsRolledBack = rolledBackTxIds.size;

    // Count uncommitted transactions (started but neither committed nor rolled back)
    const allTxIds = new Set();
    for (const record of allRecords) {
      if (record.type === 'BEGIN') allTxIds.add(record.payload.txId);
    }
    for (const txId of allTxIds) {
      if (!committedTxIds.has(txId) && !rolledBackTxIds.has(txId)) {
        this.stats.transactionsRolledBack++; // Implicitly rolled back
      }
    }

    return this.stats;
  }

  /**
   * Apply a single WAL record to the database.
   */
  _applyRecord(record) {
    switch (record.type) {
      case 'CREATE_TABLE':
        this._replayCreateTable(record.payload);
        break;
      case 'DROP_TABLE':
        this._replayDropTable(record.payload);
        break;
      case 'CREATE_INDEX':
        this._replayCreateIndex(record.payload);
        break;
      case 'INSERT':
        this._replayInsert(record.payload);
        break;
      case 'UPDATE':
        this._replayUpdate(record.payload);
        break;
      case 'DELETE':
        this._replayDelete(record.payload);
        break;
      case 'BEGIN':
      case 'COMMIT':
      case 'ROLLBACK':
      case 'CHECKPOINT':
        // No-op for replay — these are control records
        break;
      default:
        // Unknown record type — skip
        break;
    }
  }

  _replayCreateTable(payload) {
    const { table, columns } = payload;
    // Only create if it doesn't exist
    try {
      const colDefs = columns.map(c => {
        if (typeof c === 'string') return `${c} TEXT`;
        return `${c.name} ${c.type || 'TEXT'}`;
      }).join(', ');
      this.db.execute(`CREATE TABLE IF NOT EXISTS ${table} (${colDefs})`);
      this.stats.tablesCreated++;
    } catch (e) {
      // Table might already exist — that's OK
      if (!e.message.includes('already exists')) throw e;
    }
  }

  _replayDropTable(payload) {
    try {
      this.db.execute(`DROP TABLE IF EXISTS ${payload.table}`);
      this.stats.tablesDropped++;
    } catch (e) {
      // Ignore if table doesn't exist
    }
  }

  _replayCreateIndex(payload) {
    try {
      const { index, table, columns } = payload;
      this.db.execute(`CREATE INDEX IF NOT EXISTS ${index} ON ${table} (${columns.join(', ')})`);
    } catch (e) {
      // Ignore index creation errors during replay
    }
  }

  _replayInsert(payload) {
    const { table, row } = payload;
    const columns = Object.keys(row);
    const values = Object.values(row).map(v => {
      if (v === null) return 'NULL';
      if (typeof v === 'number') return String(v);
      return `'${String(v).replace(/'/g, "''")}'`;
    });
    this.db.execute(`INSERT INTO ${table} (${columns.join(', ')}) VALUES (${values.join(', ')})`);
    this.stats.rowsInserted++;
  }

  _replayUpdate(payload) {
    const { table, old: oldRow, new: newRow } = payload;
    
    // Build SET clause from new values
    const setClauses = Object.entries(newRow).map(([col, val]) => {
      if (val === null) return `${col} = NULL`;
      if (typeof val === 'number') return `${col} = ${val}`;
      return `${col} = '${String(val).replace(/'/g, "''")}'`;
    });

    // Build WHERE clause from old values (use primary key if available, otherwise all columns)
    const whereClauses = Object.entries(oldRow).map(([col, val]) => {
      if (val === null) return `${col} IS NULL`;
      if (typeof val === 'number') return `${col} = ${val}`;
      return `${col} = '${String(val).replace(/'/g, "''")}'`;
    });

    this.db.execute(`UPDATE ${table} SET ${setClauses.join(', ')} WHERE ${whereClauses.join(' AND ')}`);
    this.stats.rowsUpdated++;
  }

  _replayDelete(payload) {
    const { table, row } = payload;
    const whereClauses = Object.entries(row).map(([col, val]) => {
      if (val === null) return `${col} IS NULL`;
      if (typeof val === 'number') return `${col} = ${val}`;
      return `${col} = '${String(val).replace(/'/g, "''")}'`;
    });
    this.db.execute(`DELETE FROM ${table} WHERE ${whereClauses.join(' AND ')}`);
    this.stats.rowsDeleted++;
  }

  /**
   * Set an error handler for replay errors.
   */
  onError(handler) {
    this._onError = handler;
    return this;
  }
}

export default WALReplayEngine;
