// persistent-db.js — PersistentDatabase: SQL database with file-backed storage
// Wires together: Database + DiskManager + BufferPool + FileWAL + FileBackedHeap

import { Database } from './db.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { FileWAL, recoverFromFileWAL } from './file-wal.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { TableStats, QueryPlanner } from './planner.js';
import { mkdirSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

/**
 * PersistentDatabase — a SQL database that persists to disk.
 * 
 * Usage:
 *   const db = PersistentDatabase.open('/path/to/db');
 *   db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
 *   db.execute("INSERT INTO users VALUES (1, 'Alice')");
 *   db.close();
 *   
 *   // Later, reopen:
 *   const db2 = PersistentDatabase.open('/path/to/db');
 *   db2.execute('SELECT * FROM users'); // → [{ id: 1, name: 'Alice' }]
 */
export class PersistentDatabase {
  /**
   * Open (or create) a persistent database at the given directory.
   * @param {string} dirPath — directory for database files
   * @param {object} [options]
   * @param {number} [options.poolSize=64] — buffer pool size in pages
   * @param {boolean} [options.recover=true] — run crash recovery on open
   */
  static open(dirPath, { poolSize = 64, recover = true } = {}) {
    // Ensure directory exists
    if (!existsSync(dirPath)) {
      mkdirSync(dirPath, { recursive: true });
    }

    const catalogPath = join(dirPath, 'catalog.json');
    const walPath = join(dirPath, 'wal.log');

    // Create shared buffer pool and WAL
    const bp = new BufferPool(poolSize);
    const wal = new FileWAL(walPath);
    
    // Track disk managers for each table (one file per table)
    const diskManagers = new Map();
    const heaps = new Map();

    // Heap factory: creates file-backed heaps with per-table buffer pools
    const heapFactory = (tableName) => {
      if (!tableName) tableName = '_unnamed_' + Date.now();
      const dbPath = join(dirPath, `${tableName}.db`);
      const dm = new DiskManager(dbPath);
      diskManagers.set(tableName, dm);
      // Each table gets its own buffer pool to avoid page ID conflicts
      const tableBp = new BufferPool(Math.max(8, Math.floor(poolSize / 4)));
      const heap = new FileBackedHeap(tableName, dm, tableBp, wal);
      heaps.set(tableName, heap);
      return heap;
    };

    // Create database with custom heap factory
    const db = new Database({ heapFactory });

    const pdb = new PersistentDatabase(db, dirPath, bp, wal, diskManagers, heaps, catalogPath);

    // Load catalog (table schemas) if exists
    if (existsSync(catalogPath)) {
      const catalog = JSON.parse(readFileSync(catalogPath, 'utf8'));
      for (const table of catalog.tables) {
        pdb._createSqls.set(table.name, table.createSql);
        try {
          db.execute(table.createSql);
        } catch (e) {
          // Table may already exist in memory
        }
      }
      
      // Run crash recovery if requested
      // Recovery uses lastAppliedLSN to skip already-applied records
      if (recover) {
        for (const [name, heap] of heaps) {
          recoverFromFileWAL(heap, wal);
        }
      }
      
      // Rebuild indexes from heap data (indexes are in-memory only)
      pdb._rebuildIndexes();
    }

    return pdb;
  }

  constructor(db, dirPath, bufferPool, wal, diskManagers, heaps, catalogPath) {
    this._db = db;
    this._dirPath = dirPath;
    this._bp = bufferPool;
    this._wal = wal;
    this._diskManagers = diskManagers;
    this._heaps = heaps;
    this._catalogPath = catalogPath;
    this._statsPath = join(dirPath, 'stats.json');
    this._createSqls = new Map(); // tableName → CREATE TABLE SQL
    
    // Load persistent stats if available
    this._loadStats();
  }

  /**
   * Execute a SQL statement. Returns the same result as Database.execute().
   */
  execute(sql) {
    const trimmed = sql.trim().toUpperCase();
    const isDML = trimmed.startsWith('INSERT') || trimmed.startsWith('UPDATE') || 
                  trimmed.startsWith('DELETE') || trimmed.startsWith('REPLACE');
    
    // For DML: wrap in WAL transaction for crash recovery
    let txId;
    if (isDML && this._wal) {
      txId = this._wal.allocateTxId();
      this._wal.beginTransaction(txId);
      // Set txId on all heaps so WAL records have correct transaction
      for (const heap of this._heaps.values()) {
        heap._currentTxId = txId;
      }
    }
    
    const result = this._db.execute(sql);
    
    // Commit the WAL transaction
    if (isDML && this._wal && txId !== undefined) {
      this._wal.appendCommit(txId);
      for (const heap of this._heaps.values()) {
        heap._currentTxId = 0;
      }
    }
    
    // Track CREATE TABLE and ANALYZE statements
    if (trimmed.startsWith('CREATE TABLE') || trimmed.startsWith('CREATE INDEX')) {
      const match = sql.match(/CREATE\s+(?:TABLE|INDEX)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i);
      if (match) {
        this._createSqls.set(match[1], sql);
      }
      this._saveCatalog();
    }
    
    // Persist stats after ANALYZE
    if (trimmed.startsWith('ANALYZE')) {
      this._saveStats();
    }
    
    return result;
  }

  /**
   * Flush all dirty pages and WAL to disk.
   */
  flush() {
    for (const heap of this._heaps.values()) {
      heap.flush();
    }
  }

  /**
   * Close the database. Flushes all data and closes files.
   */
  close() {
    this._saveCatalog();
    this.flush();
    this._wal.close();
    for (const dm of this._diskManagers.values()) {
      dm.close();
    }
  }

  /**
   * Get buffer pool statistics.
   */
  stats() {
    return typeof this._bp.stats === 'function' ? this._bp.stats() : this._bp.stats;
  }

  // --- Internal ---

  _saveCatalog() {
    const tables = [];
    for (const [name, sql] of this._createSqls) {
      tables.push({ name, createSql: sql });
    }
    writeFileSync(this._catalogPath, JSON.stringify({ tables }, null, 2), 'utf8');
  }

  _saveStats() {
    try {
      const statsObj = {};
      const planner = new QueryPlanner(this._db);
      for (const tableName of this._db.tables.keys()) {
        const stats = planner.getStats(tableName);
        statsObj[tableName] = stats.toJSON();
      }
      writeFileSync(this._statsPath, JSON.stringify(statsObj, null, 2), 'utf8');
    } catch (e) {
      // Stats save is best-effort
    }
  }

  /**
   * Rebuild in-memory indexes by scanning heap data.
   * Called on database open after recovery.
   */
  _rebuildIndexes() {
    for (const [tableName, tableObj] of this._db.tables) {
      const { heap, schema, indexes } = tableObj;
      if (!indexes || indexes.size === 0) continue;
      
      // Find which column index maps to which index
      const pkCol = schema.find(c => c.primaryKey);
      if (!pkCol) continue;
      
      const pkIndex = indexes.get(pkCol.name);
      if (!pkIndex) continue;
      
      const pkColIdx = schema.findIndex(c => c.name === pkCol.name);
      
      // Scan heap and populate index
      for (const { pageId, slotIdx, values } of heap.scan()) {
        if (values && values.length > pkColIdx) {
          try {
            pkIndex.insert(values[pkColIdx], { pageId, slotIdx });
          } catch (e) {
            // Duplicate key on rebuild — skip (data integrity issue)
          }
        }
      }
    }
  }

  _loadStats() {
    if (!existsSync(this._statsPath)) return;
    try {
      const data = JSON.parse(readFileSync(this._statsPath, 'utf8'));
      const planner = new QueryPlanner(this._db);
      for (const [tableName, statsData] of Object.entries(data)) {
        const stats = TableStats.fromJSON(statsData);
        planner.statsCache.set(tableName, stats);
      }
    } catch (e) {
      // Stats load is best-effort
    }
  }
}
