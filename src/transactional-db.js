// transactional-db.js — TransactionalDatabase: ACID-compliant database engine
// Integrates: PersistentDatabase + MVCCManager for full transactional support
// 
// Architecture: Instead of reimplementing DML, we intercept heap scans to apply
// MVCC visibility. The regular Database engine runs queries normally, but sees
// only rows visible to the current transaction's snapshot.

import { Database } from './db.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { FileWAL, recoverFromFileWAL } from './file-wal.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { MVCCManager, MVCCTransaction } from './mvcc.js';
import { SSIManager } from './ssi.js';
import { HeapFile } from './page.js';
import { mkdirSync, existsSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

/**
 * TransactionalDatabase — full ACID SQL database with file-backed storage.
 * 
 * Usage:
 *   const db = TransactionalDatabase.open('/path/to/db');
 *   
 *   // Auto-commit mode:
 *   db.execute('CREATE TABLE users (id INT, name TEXT)');
 *   db.execute("INSERT INTO users VALUES (1, 'Alice')");
 *   
 *   // Explicit transactions via sessions:
 *   const s1 = db.session();
 *   s1.begin();
 *   s1.execute("INSERT INTO users VALUES (2, 'Bob')");
 *   s1.commit();
 */
export class TransactionalDatabase {
  static open(dirPath, { poolSize = 64, recover = true, isolationLevel = 'snapshot' } = {}) {
    if (!existsSync(dirPath)) {
      mkdirSync(dirPath, { recursive: true });
    }

    const catalogPath = join(dirPath, 'catalog.json');
    const walPath = join(dirPath, 'wal.log');
    const mvccStatePath = join(dirPath, 'mvcc-state.json');

    const wal = new FileWAL(walPath);
    const mvcc = isolationLevel === 'serializable' ? new SSIManager() : new MVCCManager();
    const diskManagers = new Map();
    const heaps = new Map();       // tableName → FileBackedHeap
    const versionMaps = new Map(); // tableName → Map<"pageId:slotIdx" → {xmin,xmax}>

    // Heap factory
    const heapFactory = (tableName) => {
      if (!tableName) tableName = '_unnamed_' + Date.now();
      const dbPath = join(dirPath, `${tableName}.db`);
      const dm = new DiskManager(dbPath);
      diskManagers.set(tableName, dm);
      const tableBp = new BufferPool(Math.max(8, Math.floor(poolSize / 4)));
      const fileHeap = new FileBackedHeap(tableName, dm, tableBp, wal);
      heaps.set(tableName, fileHeap);
      versionMaps.set(tableName, new Map());
      return fileHeap;
    };

    const db = new Database({ heapFactory });

    const tdb = new TransactionalDatabase(
      db, dirPath, wal, mvcc, diskManagers, heaps, versionMaps, catalogPath, poolSize
    );

    // Load catalog
    if (existsSync(catalogPath)) {
      const catalog = JSON.parse(readFileSync(catalogPath, 'utf8'));
      for (const table of catalog.tables) {
        tdb._createSqls.set(table.name, table.createSql);
        try { db.execute(table.createSql); } catch (e) { /* ignore */ }
      }
      // Crash recovery
      if (recover) {
        for (const [name, heap] of heaps) {
          recoverFromFileWAL(heap, wal);
        }
        
        // Try to load saved version maps from mvcc-state.json
        let savedVersionMaps = null;
        if (existsSync(mvccStatePath)) {
          try {
            const mvccState = JSON.parse(readFileSync(mvccStatePath, 'utf8'));
            if (mvccState.versionMaps) savedVersionMaps = mvccState.versionMaps;
          } catch {}
        }
        
        if (savedVersionMaps) {
          // Restore version maps from saved state
          for (const [name, entries] of Object.entries(savedVersionMaps)) {
            const vm = versionMaps.get(name);
            if (vm) {
              for (const [key, value] of entries) {
                vm.set(key, value);
              }
            }
          }
        } else {
          // No saved version maps — rebuild from heap (all rows visible)
          for (const [name, heap] of heaps) {
            const vm = versionMaps.get(name);
            for (const { pageId, slotIdx } of heap.scan()) {
              vm.set(`${pageId}:${slotIdx}`, { xmin: 0, xmax: 0 });
            }
          }
        }
        // Rebuild indexes from recovered heap data
        for (const [tableName, table] of db.tables || []) {
          if (!table.indexes || table.indexes.size === 0) continue;
          const heap = heaps.get(tableName);
          if (!heap) continue;
          for (const { pageId, slotIdx, values } of heap.scan()) {
            const row = {};
            for (let i = 0; i < table.schema.length; i++) {
              row[table.schema[i].name] = values[i];
            }
            for (const [colName, index] of table.indexes) {
              const colIdx = table.schema.findIndex(c => c.name === colName);
              if (colIdx >= 0 && values[colIdx] !== undefined) {
                index.insert(values[colIdx], { pageId, slotIdx });
              }
            }
          }
        }
      }
    }

    // Load MVCC state
    if (existsSync(mvccStatePath)) {
      try {
        const state = JSON.parse(readFileSync(mvccStatePath, 'utf8'));
        mvcc._nextTx = state.nextTxId || 1;
        if (state.committedTxns) {
          if (!mvcc.committedTxns) mvcc.committedTxns = new Set();
          for (const txId of state.committedTxns) {
            mvcc.committedTxns.add(txId);
          }
        }
      } catch (e) { /* best effort */ }
    }

    // Install MVCC scan/delete interceptors on recovered tables
    // Must happen AFTER catalog load and WAL recovery
    tdb._installScanInterceptors();

    return tdb;
  }

  constructor(db, dirPath, wal, mvcc, diskManagers, heaps, versionMaps, catalogPath, poolSize) {
    this._db = db;
    this._dirPath = dirPath;
    this._wal = wal;
    this._mvcc = mvcc;
    this._diskManagers = diskManagers;
    this._heaps = heaps;
    this._versionMaps = versionMaps;
    this._catalogPath = catalogPath;
    this._statsPath = join(dirPath, 'stats.json');
    this._mvccStatePath = join(dirPath, 'mvcc-state.json');
    this._createSqls = new Map();
    this._poolSize = poolSize;
    this._isolationLevel = mvcc instanceof SSIManager ? 'serializable' : 'snapshot';
    this._sessions = new Map();
    this._nextSessionId = 1;
    
    // Active transaction for the current execution context
    // This is set by session.execute() to provide MVCC context during heap scans
    this._activeTx = null;
    
    // Auto-checkpoint: trigger checkpoint every N WAL records
    this._autoCheckpointInterval = 1000; // default: every 1000 WAL entries
    this._walCountSinceCheckpoint = 0;
    
    // Monkey-patch each table's heap scan to apply MVCC visibility
    this._installScanInterceptors();
  }

  /**
   * Execute SQL in auto-commit mode.
   */
  execute(sql) {
    const trimmed = sql.trim().toUpperCase();
    
    // DDL and utility: bypass MVCC
    if (this._isDDL(trimmed)) {
      const result = this._db.execute(sql);
      if (trimmed.startsWith('CREATE TABLE') || trimmed.startsWith('CREATE INDEX')
          || trimmed.startsWith('CREATE UNIQUE') || trimmed.startsWith('CREATE VIEW') || trimmed.startsWith('CREATE MATERIALIZED') || trimmed.startsWith('CREATE TRIGGER')) {
        this._trackCreate(sql);
        this._installScanInterceptors(); // New table needs interceptor
      }
      return result;
    }
    
    // DML: wrap in auto-commit transaction
    const tx = this._mvcc.begin();
    this._activeTx = tx;
    this._setHeapTxId(tx.txId);
    try {
      const result = this._db.execute(sql);
      // Track new rows from INSERT
      this._trackNewRows(tx);
      // Write WAL delete records BEFORE commit (so recovery replays them)
      this._walLogDeletes(tx);
      this._mvcc.commit(tx.txId);
      this._wal.appendCommit(tx.txId);
      // Physicalize committed deletes (physical heap cleanup, no additional WAL)
      this._physicalizeDeletesNoWal(tx);
      this._activeTx = null;
      this._setHeapTxId(0);
      return result;
    } catch (e) {
      // Rollback: undo any heap modifications
      // Track new rows FIRST so rollback knows what to undo
      this._trackNewRows(tx);
      this._rollbackNewRows(tx);
      try { this._mvcc.rollback(tx.txId); } catch (e2) { /* ignore */ }
      this._wal.appendAbort(tx.txId);
      this._activeTx = null;
      this._setHeapTxId(0);
      throw e;
    }
  }

  /**
   * Create a new session for explicit transaction control.
   */
  session() {
    const id = this._nextSessionId++;
    const s = new TransactionSession(id, this);
    this._sessions.set(id, s);
    return s;
  }

  flush() {
    for (const heap of this._heaps.values()) heap.flush();
  }

  /**
   * Perform a checkpoint:
   * 1. Flush all dirty buffer pool pages to disk
   * 2. Write CHECKPOINT record to WAL
   * 3. Save catalog and MVCC state
   * 4. Optionally truncate old WAL entries
   */
  checkpoint({ truncateWal = false } = {}) {
    // Flush all heaps' buffer pools to disk
    for (const heap of this._heaps.values()) {
      heap.flush();
    }
    
    // Write checkpoint record to WAL
    const lsn = this._wal.checkpoint();
    
    // Persist catalog and MVCC state
    this._saveCatalog();
    this._saveMvccState();
    
    this._walCountSinceCheckpoint = 0;
    
    // Truncate WAL if requested
    let truncateResult = null;
    if (truncateWal && this._wal.truncate) {
      truncateResult = this._wal.truncate(lsn);
    }
    
    return { checkpointLsn: lsn, beginLsn: lsn, endLsn: lsn, truncation: truncateResult };
  }

  close() {
    for (const [id, session] of this._sessions) {
      if (session._tx) try { session.rollback(); } catch (e) { /* ignore */ }
    }
    this._saveCatalog();
    this._saveMvccState();
    this.flush();
    this._wal.close();
    for (const dm of this._diskManagers.values()) dm.close();
  }

  _setHeapTxId(txId) {
    for (const heap of this._heaps.values()) {
      heap._currentTxId = txId;
    }
  }

  /**
   * Write WAL DELETE records for all rows marked for deletion in this transaction.
   * Must be called BEFORE the COMMIT WAL record so recovery can replay them.
   */
  _walLogDeletes(tx) {
    for (const key of tx.writeSet) {
      if (!key.endsWith(':del')) continue;
      const parts = key.replace(/:del$/, '').split(':');
      const tableName = parts[0];
      const pageId = parseInt(parts[1]);
      const slotIdx = parseInt(parts[2]);
      if (isNaN(pageId) || isNaN(slotIdx)) continue;

      // Read the current data for the before-image
      const heap = this._heaps.get(tableName);
      if (!heap) continue;
      const values = heap.get ? heap.get(pageId, slotIdx) : null;
      this._wal.appendDelete(tx.txId, tableName, pageId, slotIdx, values);
    }
  }

  /**
   * After a transaction commits, physically delete rows if safe (no other active snapshots).
   * If other transactions are active, defer to VACUUM.
   * WAL records have already been written by _walLogDeletes.
   */
  _physicalizeDeletesNoWal(tx) {
    // Only safe to physicalize if no other active transactions could see these rows
    const hasOtherActive = this._mvcc.activeTxns.size > 0;
    
    for (const key of tx.writeSet) {
      if (!key.endsWith(':del')) continue;
      const parts = key.replace(/:del$/, '').split(':');
      const tableName = parts[0];
      const pageId = parseInt(parts[1]);
      const slotIdx = parseInt(parts[2]);
      if (isNaN(pageId) || isNaN(slotIdx)) continue;

      if (hasOtherActive) {
        // Other transactions active — can't physically delete yet.
        // Keep the version map entry so MVCC filtering still works.
        // VACUUM will clean up later.
        continue;
      }

      const tableObj = this._db.tables.get(tableName);
      if (!tableObj) continue;
      const heap = tableObj.heap;
      const origDelete = heap._origDelete;
      if (origDelete) {
        // Temporarily disable WAL on heap to avoid double-logging
        const fileHeap = this._heaps.get(tableName);
        const savedWal = fileHeap?._wal;
        if (fileHeap) fileHeap._wal = null;
        try { origDelete(pageId, slotIdx); } catch (e) { /* already deleted */ }
        if (fileHeap) fileHeap._wal = savedWal;
      }

      // Clean up version map
      const vm = this._versionMaps.get(tableName);
      if (vm) vm.delete(`${pageId}:${slotIdx}`);
    }
  }

  vacuum() {
    const horizon = this._mvcc.computeXminHorizon();
    const results = {};
    for (const [name, vm] of this._versionMaps) {
      let removed = 0;
      const dead = [];
      for (const [key, ver] of vm) {
        if (ver.xmax === 0) continue;
        // Only reclaim rows deleted by committed transactions
        // that are below the xmin horizon (no active snapshot can see them)
        if (ver.xmax < horizon && (this._mvcc.committedTxns.has(ver.xmax) || ver.xmax === -1)) {
          dead.push(key);
        }
      }
      for (const key of dead) {
        const [pageId, slotIdx] = key.split(':').map(Number);
        const tableObj = this._db.tables.get(name);
        if (!tableObj) continue;
        const heap = tableObj.heap;
        // Use original physical delete (bypass MVCC interceptor)
        const origDelete = heap._origDelete;
        if (origDelete) {
          // Temporarily disable WAL on heap (VACUUM doesn't need to log deletes)
          const fileHeap = this._heaps.get(name);
          const savedWal = fileHeap?._wal;
          if (fileHeap) fileHeap._wal = null;
          try { origDelete(pageId, slotIdx); } catch (e) { /* already deleted */ }
          if (fileHeap) fileHeap._wal = savedWal;
        }
        vm.delete(key);
        removed++;
      }
      results[name] = { deadTuplesRemoved: removed, xminHorizon: horizon };
    }
    return results;
  }

  // --- Scan interceptors ---
  
  _installScanInterceptors() {
    // For each table, intercept heap.scan() and heap.delete() for MVCC
    for (const [tableName, tableObj] of this._db.tables) {
      const heap = tableObj.heap;
      if (heap._mvccWrapped) continue;
      
      const origScan = heap.scan.bind(heap);
      const origDelete = heap.delete.bind(heap);
      const tdb = this;
      const name = tableName;
      
      // Intercept scan: filter by MVCC visibility
      heap.scan = function*() {
        const tx = tdb._activeTx;
        if (!tx) {
          // No active transaction — show all non-deleted rows with version entries
          for (const row of origScan()) {
            const vm = tdb._versionMaps.get(name);
            if (vm) {
              const key = `${row.pageId}:${row.slotIdx}`;
              const ver = vm.get(key);
              if (!ver) continue; // No version entry = orphaned/rolled-back row
              if (ver.xmax !== 0 && (tdb._mvcc.committedTxns.has(ver.xmax) || ver.xmax === -1 || ver.xmax === -2)) {
                continue;
              }
            }
            yield row;
          }
          return;
        }
        
        const vm = tdb._versionMaps.get(name);
        for (const row of origScan()) {
          if (!vm) { yield row; continue; }
          
          const key = `${row.pageId}:${row.slotIdx}`;
          const ver = vm.get(key);
          
          if (!ver) {
            // No version map entry: row exists physically but not tracked
            // During a transaction, only show rows that are in the version map
            // (untracked rows were created and rolled back, or are orphaned)
            continue;
          }
          
          const created = tdb._mvcc.isVisible(ver.xmin, tx);
          const deleted = ver.xmax === -2 || (ver.xmax !== 0 && tdb._mvcc.isVisible(ver.xmax, tx));
          
          if (created && !deleted) {
            // SSI tracking: record the read for serializable isolation
            if (tdb._mvcc.recordRead) {
              tdb._mvcc.recordRead(tx.txId, `${name}:${key}`, ver.xmin);
            }
            yield row;
          }
        }
      };
      
      // Intercept delete: in MVCC mode, mark xmax instead of physical delete
      heap.delete = function(pageId, slotIdx) {
        const tx = tdb._activeTx;
        if (!tx) {
          // No active transaction — physical delete + mark version
          const vm = tdb._versionMaps.get(name);
          if (vm) {
            const key = `${pageId}:${slotIdx}`;
            const ver = vm.get(key);
            if (ver) ver.xmax = -1; // Permanently deleted
          }
          return origDelete(pageId, slotIdx);
        }
        
        // MVCC delete: mark xmax, don't physically remove
        const vm = tdb._versionMaps.get(name);
        if (vm) {
          const key = `${pageId}:${slotIdx}`;
          const ver = vm.get(key);
          if (ver) {
            // Write-write conflict: another active tx is deleting this row
            if (ver.xmax !== 0 && ver.xmax !== tx.txId) {
              const otherTx = tx.manager.activeTxns.get(ver.xmax);
              if (otherTx && !otherTx.committed && !otherTx.aborted) {
                throw new Error(`Write-write conflict on ${name}:${key}`);
              }
            }
            const oldXmax = ver.xmax;
            ver.xmax = tx.txId;
            tx.writeSet.add(`${name}:${key}:del`);
            // SSI tracking: record the write for serializable isolation
            if (tdb._mvcc.recordWrite) {
              tdb._mvcc.recordWrite(tx.txId, `${name}:${key}`);
            }
            tx.undoLog.push(() => { ver.xmax = oldXmax; });
          }
        }
        // Don't physically delete — the row stays in the heap for other snapshots
      };
      
      heap._mvccWrapped = true;
      heap._origScan = origScan;
      heap._origDelete = origDelete;
    }
  }

  // Track newly inserted rows (rows in heap not yet in version map)
  _trackNewRows(tx) {
    const revokedRows = tx._revokedRows || new Set();
    for (const [tableName, tableObj] of this._db.tables) {
      const vm = this._versionMaps.get(tableName);
      if (!vm) continue;
      
      // Use the original (unwrapped) scan to see all physical rows
      const heap = tableObj.heap;
      const scan = heap._origScan || heap.scan.bind(heap);
      
      for (const { pageId, slotIdx } of scan()) {
        const key = `${pageId}:${slotIdx}`;
        if (!vm.has(key)) {
          // Check if this row was revoked by a savepoint rollback
          if (revokedRows.has(`${tableName}:${key}`)) continue;
          // New row — created by this transaction
          vm.set(key, { xmin: tx.txId, xmax: 0 });
          tx.writeSet.add(`${tableName}:${key}`);
          // SSI tracking
          if (this._mvcc.recordWrite) {
            this._mvcc.recordWrite(tx.txId, `${tableName}:${key}`);
          }
        }
      }
    }
  }

  // Rollback: undo new rows (physical delete) and undo xmax marks (via undo log)
  _rollbackNewRows(tx) {
    for (const key of tx.writeSet) {
      // Skip delete markers (handled by undo log)
      if (key.endsWith(':del')) continue;
      
      const parts = key.split(':');
      const tableName = parts[0];
      const pageId = parseInt(parts[1]);
      const slotIdx = parseInt(parts[2]);
      if (isNaN(pageId) || isNaN(slotIdx)) continue;
      
      const vm = this._versionMaps.get(tableName);
      if (vm) {
        const ver = vm.get(`${pageId}:${slotIdx}`);
        if (ver && ver.xmin === tx.txId) {
          // Row was created by this transaction — physically remove it
          const tableObj = this._db.tables.get(tableName);
          if (tableObj) {
            // Get values before deleting (for index cleanup)
            let values = null;
            try { values = tableObj.heap.get(pageId, slotIdx); } catch {}
            
            // Remove from indexes first
            if (values) {
              for (const [colName, index] of tableObj.indexes) {
                const colIdx = tableObj.schema.findIndex(c => c.name === colName);
                try { index.delete(values[colIdx], { pageId, slotIdx }); } catch {}
              }
            }
            
            // Remove from heap
            if (tableObj.heap._origDelete) {
              try { tableObj.heap._origDelete(pageId, slotIdx); } catch (e) { /* ignore */ }
            }
          }
          vm.delete(`${pageId}:${slotIdx}`);
        }
      }
    }
    // Execute undo log (restores xmax values for deletes)
    for (let i = tx.undoLog.length - 1; i >= 0; i--) {
      try { tx.undoLog[i](); } catch (e) { /* ignore */ }
    }
  }

  // Track DELETE operations by marking version xmax
  _markDeletedRows(tx, tableName, deletedRids) {
    const vm = this._versionMaps.get(tableName);
    if (!vm) return;
    for (const { pageId, slotIdx } of deletedRids) {
      const key = `${pageId}:${slotIdx}`;
      const ver = vm.get(key);
      if (ver) {
        ver.xmax = tx.txId;
        tx.writeSet.add(`${tableName}:${key}`);
      }
    }
  }

  _isDDL(trimmed) {
    return trimmed.startsWith('CREATE') || trimmed.startsWith('DROP') ||
           trimmed.startsWith('ALTER') || trimmed.startsWith('VACUUM') ||
           trimmed.startsWith('ANALYZE') || trimmed.startsWith('SHOW') ||
           trimmed.startsWith('DESCRIBE') || trimmed.startsWith('EXPLAIN') ||
           trimmed.startsWith('TRUNCATE') || trimmed.startsWith('RENAME');
  }

  _trackDrop(sql) {
    const match = sql.match(/DROP\s+(?:TABLE|INDEX|(?:MATERIALIZED\s+)?VIEW|TRIGGER|FUNCTION|PROCEDURE|SEQUENCE)\s+(?:IF\s+EXISTS\s+)?(\w+)/i);
    if (match) {
      const name = match[1];
      this._createSqls.delete(name);
      this._createSqls.delete(name.toLowerCase());
      this._versionMaps.delete(name);
      this._versionMaps.delete(name.toLowerCase());
      this._saveCatalog();
    }
  }

  _trackAlter(sql) {
    const match = sql.match(/ALTER\s+TABLE\s+(\w+)/i);
    if (match) {
      const tableName = match[1].toLowerCase();
      const table = this._db.tables.get(tableName);
      if (table) {
        // Reconstruct CREATE TABLE from current schema
        const cols = table.schema.map(c => {
          let def = `${c.name} ${c.type || 'TEXT'}`;
          if (c.primaryKey) def += ' PRIMARY KEY';
          if (c.notNull) def += ' NOT NULL';
          if (c.defaultValue !== undefined && c.defaultValue !== null) {
            def += ` DEFAULT ${typeof c.defaultValue === 'string' ? `'${c.defaultValue}'` : c.defaultValue}`;
          }
          if (c.check) def += ` CHECK (${c.check})`;
          return def;
        }).join(', ');
        const newCreateSql = `CREATE TABLE ${tableName} (${cols})`;
        this._createSqls.set(tableName, newCreateSql);
        this._saveCatalog();
      }
    }
  }

  _trackCreate(sql) {
    const match = sql.match(/CREATE\s+(?:TABLE|INDEX|(?:MATERIALIZED\s+)?VIEW|TRIGGER|FUNCTION|PROCEDURE|UNIQUE\s+INDEX|SEQUENCE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:UNIQUE\s+)?(\w+)/i);
    if (match) {
      this._createSqls.set(match[1], sql);
      const tableName = match[1].toLowerCase();
      if (!this._versionMaps.has(tableName) && !sql.match(/VIEW/i)) {
        this._versionMaps.set(tableName, new Map());
      }
    }
    this._saveCatalog();
  }

  _saveCatalog() {
    const tables = [];
    for (const [name, sql] of this._createSqls) {
      tables.push({ name, createSql: sql });
    }
    writeFileSync(this._catalogPath, JSON.stringify({ tables }, null, 2), 'utf8');
  }

  _saveMvccState() {
    // Save version maps along with MVCC state
    const versionMapsData = {};
    for (const [name, vm] of this._versionMaps) {
      versionMapsData[name] = [...vm.entries()].map(([k, v]) => [k, v]);
    }
    writeFileSync(this._mvccStatePath, JSON.stringify({
      nextTxId: this._mvcc.nextTxId,
      committedTxns: this._mvcc.committedTxns ? [...this._mvcc.committedTxns] : [],
      versionMaps: versionMapsData
    }), 'utf8');
  }
}

/**
 * TransactionSession — connection-level session with explicit transaction control.
 */
export class TransactionSession {
  constructor(id, tdb) {
    this.id = id;
    this._tdb = tdb;
    this._tx = null;
    this._closed = false;
    this._savepoints = new Map(); // name → { versionMapSnapshots, newRowSnapshots }
    this._revokedRows = new Set(); // Set of "tableName:pageId:slotIdx" keys rolled back by savepoints
  }

  begin() {
    if (this._tx) throw new Error('Transaction already in progress');
    this._tx = this._tdb._mvcc.begin();
    return { type: 'OK', message: 'BEGIN' };
  }

  commit() {
    if (!this._tx) throw new Error('No transaction in progress');
    // Track any new rows from this transaction (but not revoked ones)
    if (this._revokedRows.size > 0) {
      this._tx._revokedRows = this._revokedRows;
    }
    this._tdb._trackNewRows(this._tx);
    // Write WAL delete records before commit
    this._tdb._walLogDeletes(this._tx);
    this._tdb._mvcc.commit(this._tx.txId);
    this._tdb._wal.appendCommit(this._tx.txId);
    // Physicalize committed deletes (no additional WAL)
    this._tdb._physicalizeDeletesNoWal(this._tx);
    // Physically remove savepoint-revoked rows (xmax=-2) from heap
    this._physicalizeRevokedRows();
    this._tx = null;
    this._savepoints.clear();
    this._revokedRows.clear();
    return { type: 'OK', message: 'COMMIT' };
  }

  /** Remove savepoint-revoked rows (xmax=-2) from heap for persistence correctness */
  _physicalizeRevokedRows() {
    for (const [tableName, vm] of this._tdb._versionMaps) {
      const table = this._tdb._db.tables.get(tableName);
      if (!table) continue;
      const keysToDelete = [];
      for (const [key, ver] of vm) {
        if (ver.xmax === -2) {
          const [pageId, slotIdx] = key.split(':').map(Number);
          try { table.heap.delete(pageId, slotIdx); } catch {}
          keysToDelete.push(key);
        }
      }
      for (const key of keysToDelete) {
        vm.delete(key);
      }
    }
  }

  rollback() {
    if (!this._tx) throw new Error('No transaction in progress');
    this._tdb._rollbackNewRows(this._tx);
    this._tdb._mvcc.rollback(this._tx.txId);
    this._tdb._wal.appendAbort(this._tx.txId);
    this._tx = null;
    this._savepoints.clear();
    return { type: 'OK', message: 'ROLLBACK' };
  }

  savepoint(name) {
    if (!this._tx) throw new Error('No transaction in progress');
    // Deep-snapshot version maps (must copy values to avoid reference aliasing!)
    const vmSnapshots = new Map();
    for (const [tableName, vm] of this._tdb._versionMaps) {
      const snap = new Map();
      for (const [k, v] of vm) {
        snap.set(k, { xmin: v.xmin, xmax: v.xmax });
      }
      vmSnapshots.set(tableName, snap);
    }
    // Snapshot new rows tracking (if available)
    const newRowsSnapshot = this._tx.newRows ? new Map(
      [...this._tx.newRows].map(([k, v]) => [k, [...v]])
    ) : null;
    // Snapshot delete tracking
    const deletesSnapshot = this._tx.deletes ? new Map(
      [...this._tx.deletes].map(([k, v]) => [k, [...v]])
    ) : null;
    this._savepoints.set(name, { vmSnapshots, newRowsSnapshot, deletesSnapshot });
    return { type: 'OK', message: `SAVEPOINT ${name}` };
  }

  rollbackTo(name) {
    if (!this._tx) throw new Error('No transaction in progress');
    const sp = this._savepoints.get(name);
    if (!sp) throw new Error(`Savepoint '${name}' not found`);
    
    // Restore version maps to savepoint state
    for (const [tableName, snapshot] of sp.vmSnapshots) {
      const vm = this._tdb._versionMaps.get(tableName);
      if (!vm) continue;
      
      // Find keys in current vm but not in snapshot — mark as dead
      for (const [key, ver] of vm) {
        if (!snapshot.has(key)) {
          vm.set(key, { xmin: ver.xmin, xmax: -2 });
          this._revokedRows.add(`${tableName}:${key}`);
        }
      }
      // Restore all snapshot entries (overwriting any modifications like DELETE xmax changes)
      for (const [k, v] of snapshot) {
        vm.set(k, { ...v });
      }
    }
    
    // Restore new rows tracking
    if (sp.newRowsSnapshot && this._tx.newRows) {
      this._tx.newRows = new Map(
        [...sp.newRowsSnapshot].map(([k, v]) => [k, [...v]])
      );
    }
    
    // Restore deletes tracking
    if (sp.deletesSnapshot && this._tx.deletes) {
      this._tx.deletes = new Map(
        [...sp.deletesSnapshot].map(([k, v]) => [k, [...v]])
      );
    }
    
    // Remove savepoints created after this one
    let found = false;
    for (const [spName] of this._savepoints) {
      if (spName === name) { found = true; continue; }
      if (found) this._savepoints.delete(spName);
    }
    
    return { type: 'OK', message: `ROLLBACK TO ${name}` };
  }

  releaseSavepoint(name) {
    if (!this._tx) throw new Error('No transaction in progress');
    if (!this._savepoints.has(name)) throw new Error(`Savepoint '${name}' not found`);
    this._savepoints.delete(name);
    return { type: 'OK', message: `RELEASE ${name}` };
  }

  execute(sql) {
    if (this._closed) throw new Error('Session is closed');
    const trimmed = sql.trim().toUpperCase();
    
    if (trimmed === 'BEGIN' || trimmed === 'BEGIN TRANSACTION' || trimmed === 'START TRANSACTION') {
      return this.begin();
    }
    if (trimmed === 'COMMIT') return this.commit();
    if (trimmed === 'ROLLBACK' || trimmed === 'ABORT') return this.rollback();
    
    // SAVEPOINT <name>
    const spMatch = trimmed.match(/^SAVEPOINT\s+(\w+)$/);
    if (spMatch) return this.savepoint(spMatch[1].toLowerCase());
    
    // ROLLBACK TO [SAVEPOINT] <name>
    const rbMatch = trimmed.match(/^ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?(\w+)$/);
    if (rbMatch) return this.rollbackTo(rbMatch[1].toLowerCase());
    
    // RELEASE [SAVEPOINT] <name>
    const relMatch = trimmed.match(/^RELEASE\s+(?:SAVEPOINT\s+)?(\w+)$/);
    if (relMatch) return this.releaseSavepoint(relMatch[1].toLowerCase());
    
    // DDL: bypass MVCC
    if (this._tdb._isDDL(trimmed)) {
      const result = this._tdb._db.execute(sql);
      if (trimmed.startsWith('CREATE ')) {
        this._tdb._trackCreate(sql);
        this._tdb._installScanInterceptors();
      }
      return result;
    }
    
    // DML with transaction context
    if (this._tx) {
      // Use explicit transaction
      const prevTx = this._tdb._activeTx;
      this._tdb._activeTx = this._tx;
      this._tdb._setHeapTxId(this._tx.txId);
      // Make revoked rows available to _trackNewRows
      if (this._revokedRows.size > 0) {
        this._tx._revokedRows = this._revokedRows;
      }
      try {
        const result = this._tdb._db.execute(sql);
        // Track new rows immediately
        this._tdb._trackNewRows(this._tx);
        return result;
      } finally {
        this._tdb._activeTx = prevTx;
        this._tdb._setHeapTxId(prevTx ? prevTx.txId : 0);
      }
    }
    
    // No explicit transaction — auto-commit
    return this._tdb.execute(sql);
  }

  close() {
    if (this._tx) try { this.rollback(); } catch (e) { /* ignore */ }
    this._tdb._sessions.delete(this.id);
    this._closed = true;
  }
}
