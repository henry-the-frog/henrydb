// transactional-db.js - TransactionalDatabase: ACID-compliant database engine
// Integrates: PersistentDatabase + MVCCManager for full transactional support
//
// Architecture: Instead of reimplementing DML, we intercept heap scans to apply
// MVCC visibility. The regular Database engine runs queries normally, but sees
// only rows visible to the current transaction's snapshot.

import { Database } from './db.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { TableVisibilityMap } from './visibility-map.js';
import { FileWAL, recoverFromFileWAL } from './file-wal.js';
import { RECORD_TYPES } from './wal.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { MVCCManager, MVCCHeap, MVCCTransaction } from './mvcc.js';
import { SSIManager } from './ssi.js';
import { HeapFile } from './page.js';
import { mkdirSync, existsSync, readFileSync, writeFileSync, renameSync } from 'node:fs';
import { join } from 'node:path';

/**
 * TransactionalDatabase - full ACID SQL database with file-backed storage.
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
  static open(dirPath, { poolSize = 64, recover = true, isolationLevel = 'snapshot', syncMode } = {}) {
    if (!existsSync(dirPath)) {
      mkdirSync(dirPath, { recursive: true });
    }

    const catalogPath = join(dirPath, 'catalog.json');
    const walPath = join(dirPath, 'wal.log');
    const mvccStatePath = join(dirPath, 'mvcc-state.json');

    const walOpts = syncMode ? { syncMode } : {};
    const wal = new FileWAL(walPath, walOpts);
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
      // Restore views from catalog
      if (catalog.views) {
        for (const view of catalog.views) {
          try { db.execute(view.sql); 
            // Store the SQL on the view def for re-persistence
            const viewDef = db.views.get(view.name);
            if (viewDef) viewDef.sql = view.sql;
          } catch (e) { /* ignore view creation errors — underlying table may not exist yet */ }
        }
      }
      // Restore triggers from catalog
      if (catalog.triggers && Array.isArray(catalog.triggers)) {
        db.triggers = catalog.triggers;
      }
      // Restore sequences from catalog
      if (catalog.sequences) {
        for (const [name, seq] of Object.entries(catalog.sequences)) {
          db.sequences.set(name, { ...seq });
        }
      }
      // Crash recovery
      if (recover) {
        // Phase 1: Replay DDL WAL records (ALTER TABLE, CREATE INDEX, etc.)
        // These modify the schema and must be replayed BEFORE per-heap DML recovery.
        // NOTE: We only modify schema, NOT heap data — heap data will be corrected by
        // per-heap DML recovery in Phase 2. Using db.execute() would double-apply changes.
        const allWalRecords = wal.readFromStable(0);
        for (const r of allWalRecords) {
          if (r.type === RECORD_TYPES.DDL && r.after?.sql) {
            const sql = r.after.sql;
            try {
              // For ALTER TABLE, we need schema-only replay (no heap modification)
              const trimmedUpper = sql.trim().toUpperCase();
              if (trimmedUpper.startsWith('ALTER TABLE')) {
                tdb._replayDDLSchemaOnly(db, sql);
              } else if (trimmedUpper.startsWith('CREATE TRIGGER')) {
                // Deduplicate: only add trigger if not already loaded from catalog
                const nameMatch = sql.match(/CREATE\s+TRIGGER\s+(\w+)/i);
                if (nameMatch && !db.triggers.some(t => t.name === nameMatch[1])) {
                  db.execute(sql);
                }
              } else if (trimmedUpper.startsWith('CREATE SEQUENCE')) {
                // Deduplicate: only add sequence if not already loaded from catalog
                const nameMatch = sql.match(/CREATE\s+SEQUENCE\s+(\w+)/i);
                if (nameMatch && !db.sequences.has(nameMatch[1])) {
                  db.execute(sql);
                }
              } else if (trimmedUpper.startsWith('CREATE VIEW') || trimmedUpper.startsWith('CREATE OR REPLACE VIEW')) {
                // Deduplicate: skip if view already exists from catalog
                const nameMatch = sql.match(/CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)/i);
                if (nameMatch && !db.views.has(nameMatch[1])) {
                  db.execute(sql);
                  const viewDef = db.views.get(nameMatch[1]);
                  if (viewDef) viewDef.sql = sql;
                }
              } else {
                // For CREATE TABLE, CREATE INDEX, DROP TABLE, DROP INDEX, etc., full replay is safe
                db.execute(sql);
              }
            } catch { /* ignore replay errors for idempotent DDL */ }
          }
        }
        // Update catalog with any DDL changes
        for (const [name, tableObj] of db.tables) {
          tdb._createSqls.set(name, tdb._reconstructCreateSQL(name, tableObj.schema));
        }
        // Remove tables from _createSqls that were dropped during DDL replay
        for (const name of [...tdb._createSqls.keys()]) {
          if (!db.tables.has(name)) tdb._createSqls.delete(name);
        }
        // Handle heap rekeying for renamed tables
        // DDL replay may have renamed tables — need to match heaps to new names
        const ddlRenames = new Map(); // oldName → newName
        for (const r of allWalRecords) {
          if (r.type === RECORD_TYPES.DDL && r.after?.sql) {
            const m = r.after.sql.match(/ALTER\s+TABLE\s+(\w+)\s+RENAME\s+TO\s+(\w+)/i);
            if (m) ddlRenames.set(m[1], m[2]);
          }
        }
        for (const [oldName, newName] of ddlRenames) {
          if (heaps.has(oldName) && !heaps.has(newName)) {
            // The heap was created from stale catalog with old_name.db (new empty file)
            // But actual data is in new_name.db (renamed by original session before crash)
            // Create a fresh heap pointing at new_name.db
            const dbPath = join(dirPath, `${newName}.db`);
            if (existsSync(dbPath)) {
              const dm = new DiskManager(dbPath);
              const tableBp = new BufferPool(Math.max(8, Math.floor(poolSize / 4)));
              const fileHeap = new FileBackedHeap(newName, dm, tableBp, wal);
              heaps.delete(oldName);
              heaps.set(newName, fileHeap);
              diskManagers.delete(oldName);
              diskManagers.set(newName, dm);
              if (versionMaps.has(oldName)) {
                versionMaps.set(newName, versionMaps.get(oldName));
                versionMaps.delete(oldName);
              } else {
                versionMaps.set(newName, new Map());
              }
              // Update the table object to use the new heap
              const tableObj = db.tables.get(newName);
              if (tableObj) tableObj.heap = fileHeap;
            } else {
              // new_name.db doesn't exist — just rekey the old heap
              const heap = heaps.get(oldName);
              heaps.delete(oldName);
              heap.name = newName;
              heaps.set(newName, heap);
              // Update the table object to use this heap
              const tableObj = db.tables.get(newName);
              if (tableObj) tableObj.heap = heap;
              if (diskManagers.has(oldName)) {
                diskManagers.set(newName, diskManagers.get(oldName));
                diskManagers.delete(oldName);
              }
              if (versionMaps.has(oldName)) {
                versionMaps.set(newName, versionMaps.get(oldName));
                versionMaps.delete(oldName);
              }
            }
          }
        }
        
        // Remove catalog entries for tables that no longer exist (dropped or renamed)
        for (const name of [...tdb._createSqls.keys()]) {
          if (!db.tables.has(name)) {
            tdb._createSqls.delete(name);
            heaps.delete(name);
            diskManagers.delete(name);
            versionMaps.delete(name);
          }
        }
        // Ensure heaps exist for any newly created/renamed tables
        for (const [name, tableObj] of db.tables) {
          if (!heaps.has(name)) {
            const dbPath = join(dirPath, `${name}.db`);
            const dm = new DiskManager(dbPath);
            diskManagers.set(name, dm);
            const tableBp = new BufferPool(Math.max(8, Math.floor(poolSize / 4)));
            const fileHeap = new FileBackedHeap(name, dm, tableBp, wal);
            heaps.set(name, fileHeap);
            versionMaps.set(name, new Map());
            // Wire heap into table object
            tableObj.heap = fileHeap;
          }
        }
        
        // Phase 2: Per-heap DML recovery (INSERT/UPDATE/DELETE replay)
        // Only replay records AFTER the last checkpoint — earlier records are already applied
        const checkpointLsn = wal.lastCheckpointLsn || 0;
        for (const [name, heap] of heaps) {
          recoverFromFileWAL(heap, wal, checkpointLsn);
        }
        // Rebuild version maps: all recovered rows are always-visible
        // Use xmin=1 (a "bootstrap" txId that's always committed)
        for (const [name, heap] of heaps) {
          const vm = versionMaps.get(name);
          for (const { pageId, slotIdx } of heap.scan()) {
            vm.set(`${pageId}:${slotIdx}`, { xmin: 1, xmax: 0 });
          }
        }
        // Mark txId=1 as committed so recovered rows are visible
        mvcc.committedTxns.add(1);
        if (mvcc._nextTx <= 1) mvcc._nextTx = 2;

        // Rebuild primary key indexes from heap data
        for (const [tableName, tableObj] of db.tables) {
          const { heap, schema, indexes } = tableObj;
          if (!indexes || indexes.size === 0) continue;
          const pkCol = schema.find(c => c.primaryKey);
          if (!pkCol) continue;
          const pkIndex = indexes.get(pkCol.name);
          if (!pkIndex) continue;
          const pkColIdx = schema.findIndex(c => c.name === pkCol.name);
          for (const { pageId, slotIdx, values } of heap.scan()) {
            if (values && values.length > pkColIdx) {
              try { pkIndex.insert(values[pkColIdx], { pageId, slotIdx }); } catch {}
            }
          }
        }
      }
    }

    // Load MVCC state
    if (existsSync(mvccStatePath)) {
      try {
        const state = JSON.parse(readFileSync(mvccStatePath, 'utf8'));
        mvcc.nextTxId = state.nextTxId || 1;
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
    // Patch logDDL onto the inner Database's WAL so ALTER TABLE/CREATE INDEX/DROP INDEX
    // get logged to the FileWAL. DML WAL calls continue through the existing no-op WAL.
    if (db.wal) {
      db.wal.logDDL = (sql) => wal.logDDL(sql);
    }
    this._mvcc = mvcc;
    this._visibilityMap = new TableVisibilityMap();
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

    // Auto-checkpoint: trigger when WAL exceeds this size (bytes)
    // Default: 16MB. Set to 0 to disable.
    this._autoCheckpointBytes = 16 * 1024 * 1024;
    this._checkpointInProgress = false;

    // Monkey-patch each table's heap scan to apply MVCC visibility
    this._installScanInterceptors();
  }

  /** Expose inner Database's tables map for EXPLAIN, server introspection, etc. */
  get tables() { return this._db.tables; }

  /**
   * Execute SQL in auto-commit mode.
   */
  execute(sql) {
    const trimmed = sql.trim().toUpperCase();

    // DDL and utility: bypass MVCC
    if (this._isDDL(trimmed)) {
      const result = this._db.execute(sql);
      // Log DDL to WAL for crash recovery with stale catalog
      // NOTE: ALTER TABLE and CREATE/DROP INDEX are already logged by the inner Database
      // via the patched db.wal.logDDL interceptor. Only log DDL types NOT covered there.
      const isReadOnly = trimmed.startsWith('SHOW') || trimmed.startsWith('DESCRIBE') || 
                         trimmed.startsWith('EXPLAIN') || trimmed.startsWith('VACUUM') ||
                         trimmed.startsWith('ANALYZE');
      const alreadyLoggedByInner = trimmed.startsWith('ALTER TABLE') || trimmed.startsWith('RENAME') ||
                                    trimmed.startsWith('CREATE INDEX') || trimmed.startsWith('DROP INDEX');
      if (!isReadOnly && !alreadyLoggedByInner && this._wal && this._wal.logDDL) {
        this._wal.logDDL(sql);
      }
      if (trimmed.startsWith('CREATE TABLE') || trimmed.startsWith('CREATE INDEX')) {
        this._trackCreate(sql);
        this._installScanInterceptors(); // New table needs interceptor
      } else if (trimmed.startsWith('ALTER TABLE') || trimmed.startsWith('RENAME')) {
        // ALTER TABLE changes schema — update catalog to match current state
        this._updateCatalogAfterAlter(sql);
        // Checkpoint after ALTER TABLE ADD/DROP COLUMN to establish a clean WAL boundary.
        // Without this, recovery replays old INSERT records with pre-alter tuple widths,
        // creating duplicate rows with wrong column counts.
        if (trimmed.startsWith('ALTER TABLE') && 
            (trimmed.includes('ADD COLUMN') || trimmed.includes('DROP COLUMN'))) {
          try { this.checkpoint(); } catch (e) { /* checkpoint best-effort — fails if txs open */ }
        }
      } else if (trimmed.startsWith('DROP TABLE')) {
        const match = sql.match(/DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(\w+)/i);
        if (match) this._createSqls.delete(match[1]);
        this._saveCatalog();
      } else if (trimmed.startsWith('CREATE VIEW') || trimmed.startsWith('CREATE OR REPLACE VIEW')) {
        // Store original SQL for catalog persistence
        const viewMatch = sql.match(/CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)/i);
        if (viewMatch) {
          const viewDef = this._db.views.get(viewMatch[1]);
          if (viewDef) viewDef.sql = sql;
        }
        this._saveCatalog();
      } else if (trimmed.startsWith('DROP VIEW')) {
        this._saveCatalog();
      } else if (trimmed.startsWith('CREATE TRIGGER') || trimmed.startsWith('DROP TRIGGER') ||
                 trimmed.startsWith('CREATE SEQUENCE') || trimmed.startsWith('DROP SEQUENCE') ||
                 trimmed.startsWith('CREATE MATERIALIZED') || trimmed.startsWith('REFRESH MATERIALIZED')) {
        this._saveCatalog();
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
      this._physicalizeDeletes(tx);
      this._activeTx = null;
      this._setHeapTxId(0);
      // Invalidate result cache — DML changed data, cached SELECTs may be stale
      if (this._db._resultCache) this._db._resultCache.clear();
      return result;
    } catch (e) {
      // Rollback: undo any heap modifications
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
   * Checkpoint: flush all dirty pages to data files, save catalog/MVCC state,
   * then truncate the WAL. After checkpoint, recovery will start from scratch
   * (no WAL replay needed since all data is on disk).
   *
   * Only safe when no transactions are in-progress.
   * Returns the WAL size before truncation.
   */
  checkpoint() {
    // Verify no active transactions
    for (const [, session] of this._sessions) {
      if (session._tx) {
        throw new Error('Cannot checkpoint while transactions are in progress');
      }
    }

    // 1. Flush all heap pages to disk
    this.flush();

    // 2. Save catalog and MVCC state
    this._saveCatalog();
    this._saveMvccState();

    // 3. Write checkpoint record to WAL
    this._wal.checkpoint();

    // 4. Get WAL size before truncation
    const walSize = this._wal.fileSize;

    // 5. Truncate WAL (all data is safely on disk)
    this._wal.truncate();
    
    // 6. Re-write checkpoint marker so recovery knows where to start
    this._wal.checkpoint();

    return { walSizeBefore: walSize };
  }

  _maybeAutoCheckpoint() {
    if (!this._autoCheckpointBytes || this._checkpointInProgress) return;
    try {
      const walSize = this._wal.fileSize;
      if (walSize >= this._autoCheckpointBytes) {
        // Check no other sessions have active transactions
        for (const [, session] of this._sessions) {
          if (session._tx) return; // Can't checkpoint now
        }
        this._checkpointInProgress = true;
        try {
          this.checkpoint();
        } finally {
          this._checkpointInProgress = false;
        }
      }
    } catch (e) {
      // Auto-checkpoint failures are non-fatal
      this._checkpointInProgress = false;
    }
  }

  close() {
    for (const [id, session] of this._sessions) {
      if (session._tx) try { session.rollback(); } catch (e) { /* ignore */ }
    }

    // Clean up dead rows before persisting - remove old MVCC versions
    // that have been superseded by committed updates/deletes
    this._compactDeadRows();

    this._saveCatalog();
    this._saveMvccState();

    // Update lastAppliedLSN after flush
    this.flush();
    const maxLSN = this._wal._flushedLsn || 0;
    for (const dm of this._diskManagers.values()) {
      if (maxLSN > dm.lastAppliedLSN) {
        dm.lastAppliedLSN = maxLSN;
      }
    }
    this._saveCatalog(); // Re-save with updated LSNs

    this._wal.close();
    for (const dm of this._diskManagers.values()) dm.close();
  }

  _compactDeadRows() {
    const committedTxns = this._mvcc.committedTxns || new Set();

    for (const [tableName, vm] of this._versionMaps) {
      const heap = this._heaps.get(tableName);
      if (!heap) continue;

      const deadSlots = [];
      for (const [key, ver] of vm) {
        // A row is dead if its xmax is committed (it was deleted or superseded)
        if (ver.xmax > 0 && committedTxns.has(ver.xmax)) {
          const [pageId, slotIdx] = key.split(':').map(Number);
          deadSlots.push({ pageId, slotIdx });
        }
      }

      // Delete dead rows from heap
      for (const { pageId, slotIdx } of deadSlots) {
        try {
          heap.delete(pageId, slotIdx);
          vm.delete(`${pageId}:${slotIdx}`);
        } catch {
          // Slot might already be empty
        }
      }
    }
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
  _physicalizeDeletes(tx) {
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
        // Other transactions active - can't physically delete yet.
        // Keep the version map entry so MVCC filtering still works.
        // VACUUM will clean up later.
        continue;
      }

      const tableObj = this._db.tables.get(tableName);
      if (!tableObj) continue;
      const heap = tableObj.heap;
      const origDelete = heap._origDelete;
      if (origDelete) {
        // Keep WAL enabled so pageLSN advances — ensures crash recovery
        // doesn't replay stale INSERT records over physicalized deletes
        try { origDelete(pageId, slotIdx); } catch (e) { /* already deleted */ }
      }

      // Clean up version map
      const vm = this._versionMaps.get(tableName);
      if (vm) vm.delete(`${pageId}:${slotIdx}`);
    }
  }

  vacuum() {
    // First, run MVCC garbage collection on version chains
    const gcResult = this._mvcc.gc();

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
          // Keep WAL enabled so pageLSN advances — ensures recovery doesn't
          // replay stale INSERT records over vacuum'd pages
          try { origDelete(pageId, slotIdx); } catch (e) { /* already deleted */ }
        }
        vm.delete(key);
        removed++;
      }
      results[name] = { deadTuplesRemoved: removed, xminHorizon: horizon };

      // Update visibility map: mark pages as all-visible if no remaining dead tuples
      const dirtyPages = new Set();
      for (const [key, ver] of vm) {
        if (ver.xmax !== 0) {
          const [pid] = key.split(':').map(Number);
          dirtyPages.add(pid);
        }
      }
      // All pages NOT in dirtyPages are all-visible
      const tableObj2 = this._db.tables.get(name);
      if (tableObj2?.heap?._origScan || tableObj2?.heap?.scan) {
        const scan = tableObj2.heap._origScan || tableObj2.heap.scan.bind(tableObj2.heap);
        const allPages = new Set();
        for (const { pageId } of scan()) allPages.add(pageId);
        for (const pid of allPages) {
          if (!dirtyPages.has(pid)) {
            this._visibilityMap.setAllVisible(name, pid);
          }
        }
      }
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
          // No active transaction - show all non-deleted rows
          for (const row of origScan()) {
            const vm = tdb._versionMaps.get(name);
            if (vm) {
              const key = `${row.pageId}:${row.slotIdx}`;
              const ver = vm.get(key);
              if (ver && ver.xmax !== 0 && (tdb._mvcc.committedTxns.has(ver.xmax) || ver.xmax === -1)) {
                continue;
              }
            }
            yield row;
          }
          return;
        }

        const vm = tdb._versionMaps.get(name);
        const visMap = tdb._visibilityMap;
        for (const row of origScan()) {
          if (!vm) { yield row; continue; }

          // Visibility map optimization: if page is all-visible, skip MVCC check
          if (visMap.isAllVisible(name, row.pageId)) {
            // Page is known to be all-visible - yield directly
            // Still record read for SSI tracking
            if (tdb._mvcc.recordRead && !tx.suppressReadTracking) {
              const key = `${row.pageId}:${row.slotIdx}`;
              tdb._mvcc.recordRead(tx.txId, `${name}:${key}`, 0);
            }
            yield row;
            continue;
          }

          const key = `${row.pageId}:${row.slotIdx}`;
          const ver = vm.get(key);

          if (!ver) {
            // SSI tracking: record read even for rows without version info
            if (tdb._mvcc.recordRead && !tx.suppressReadTracking) {
              tdb._mvcc.recordRead(tx.txId, `${name}:${key}`, 0);
            }
            yield row;
            continue;
          }

          const created = tdb._mvcc.isVisible(ver.xmin, tx);
          const deleted = ver.xmax !== 0 && tdb._mvcc.isVisible(ver.xmax, tx);

          if (created && !deleted) {
            // SSI tracking: record the read for serializable isolation
            if (tdb._mvcc.recordRead && !tx.suppressReadTracking) {
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
          // No active transaction - physical delete + mark version
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
            // Invalidate visibility map for this page
            tdb._visibilityMap.onPageModified(name, pageId);
            // SSI tracking: record the write for serializable isolation
            if (tdb._mvcc.recordWrite) {
              tdb._mvcc.recordWrite(tx.txId, `${name}:${key}`);
            }
            tx.undoLog.push(() => { ver.xmax = oldXmax; });
          } else {
            // No version entry (e.g., restored by undo) — create one and mark deleted
            const newVer = { xmin: 1, xmax: tx.txId };
            vm.set(key, newVer);
            tx.writeSet.add(`${name}:${key}:del`);
            tdb._visibilityMap.onPageModified(name, pageId);
            if (tdb._mvcc.recordWrite) {
              tdb._mvcc.recordWrite(tx.txId, `${name}:${key}`);
            }
            tx.undoLog.push(() => { vm.delete(key); });
          }
        }
        // Don't physically delete - the row stays in the heap for other snapshots
      };

      // Intercept findByPK: check MVCC visibility on the result
      if (typeof heap.findByPK === 'function') {
        const origFindByPK = heap.findByPK.bind(heap);
        heap._origFindByPK = origFindByPK;
        heap.findByPK = function(pkValue) {
          const values = origFindByPK(pkValue);
          if (!values) {
            // B+tree doesn't have this key — but under MVCC, an older version
            // might exist in the heap (e.g., row was updated and B+tree now points
            // to the new version). Fall back to scan for the PK.
            const pkIndices = heap.pkIndices || [0];
            for (const row of heap.scan()) {
              const rowValues = row.values || row;
              const pk = pkIndices.length === 1 ? rowValues[pkIndices[0]] : 
                pkIndices.map(i => String(rowValues[i])).join('\0');
              if (pk === pkValue) return rowValues;
            }
            return null;
          }
          
          // Check version map for the B+tree result
          const vm = tdb._versionMaps.get(name);
          if (!vm) return values;
          
          // Look up the version map by pageId:slotIdx
          const pkToRid = heap._pkToRid;
          if (pkToRid) {
            const rid = pkToRid.get(pkValue);
            if (rid !== undefined) {
              const ridNum = typeof rid === 'number' ? rid : (rid.pageId * (heap._syntheticPageSize || 1000) + rid.slotIdx);
              const pageId = Math.floor(ridNum / (heap._syntheticPageSize || 1000));
              const slotIdx = ridNum % (heap._syntheticPageSize || 1000);
              const key = `${pageId}:${slotIdx}`;
              const ver = vm.get(key);
              
              if (ver) {
                const tx = tdb._activeTx;
                if (tx) {
                  // In transaction — check visibility
                  const created = tdb._mvcc.isVisible(ver.xmin, tx);
                  const deleted = ver.xmax !== 0 && tdb._mvcc.isVisible(ver.xmax, tx);
                  if (!created || deleted) {
                    // Current B+tree version not visible — scan for an older version
                    const pkIndices = heap.pkIndices || [0];
                    for (const row of heap.scan()) {
                      const rowValues = row.values || row;
                      const pk = pkIndices.length === 1 ? rowValues[pkIndices[0]] : 
                        pkIndices.map(i => String(rowValues[i])).join('\0');
                      if (pk === pkValue) return rowValues;
                    }
                    return null;
                  }
                } else {
                  // No transaction — check committed state
                  if (ver.xmax !== 0 && tdb._mvcc.committedTxns.has(ver.xmax)) return null;
                }
              }
            }
          }
          return values;
        };
      }

      // Intercept get(pageId, slotIdx): check MVCC visibility
      if (typeof heap.get === 'function') {
        const origGet = heap.get.bind(heap);
        heap._origGet = origGet;
        heap.get = function(pageId, slotIdx) {
          const values = origGet(pageId, slotIdx);
          if (!values) return null;
          
          const vm = tdb._versionMaps.get(name);
          if (!vm) return values;
          
          const key = `${pageId}:${slotIdx}`;
          const ver = vm.get(key);
          if (ver) {
            const tx = tdb._activeTx;
            if (tx) {
              const created = tdb._mvcc.isVisible(ver.xmin, tx);
              const deleted = ver.xmax !== 0 && tdb._mvcc.isVisible(ver.xmax, tx);
              if (!created || deleted) return null;
            } else {
              if (ver.xmax !== 0 && tdb._mvcc.committedTxns.has(ver.xmax)) return null;
            }
          }
          return values;
        };
      }

      heap._mvccWrapped = true;
      heap._origScan = origScan;
      heap._origDelete = origDelete;
    }
  }

  // Track newly inserted rows (rows in heap not yet in version map)
  _trackNewRows(tx) {
    for (const [tableName, tableObj] of this._db.tables) {
      const vm = this._versionMaps.get(tableName);
      if (!vm) continue;

      // Use the original (unwrapped) scan to see all physical rows
      const heap = tableObj.heap;
      const scan = heap._origScan || heap.scan.bind(heap);

      for (const { pageId, slotIdx } of scan()) {
        const key = `${pageId}:${slotIdx}`;
        if (!vm.has(key)) {
          // New row - created by this transaction
          vm.set(key, { xmin: tx.txId, xmax: 0 });
          tx.writeSet.add(`${tableName}:${key}`);
          // Invalidate visibility map for this page
          this._visibilityMap.onPageModified(tableName, pageId);
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
          // Row was created by this transaction - physically remove it
          const tableObj = this._db.tables.get(tableName);
          if (tableObj && tableObj.heap._origDelete) {
            try { tableObj.heap._origDelete(pageId, slotIdx); } catch (e) { /* ignore */ }
          }
          vm.delete(`${pageId}:${slotIdx}`);
        }
      }
    }
    // Execute undo log (restores xmax values for deletes and UPDATE rollbacks)
    for (let i = tx.undoLog.length - 1; i >= 0; i--) {
      try { tx.undoLog[i](); } catch (e) { /* ignore */ }
    }
    // Clear undo log to prevent double-execution (MVCC.rollback also runs it)
    tx.undoLog.length = 0;
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

  _trackCreate(sql) {
    const match = sql.match(/CREATE\s+(?:TABLE|INDEX)\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)/i);
    if (match) {
      this._createSqls.set(match[1], sql);
      const tableName = match[1].toLowerCase();
      if (!this._versionMaps.has(tableName)) {
        this._versionMaps.set(tableName, new Map());
      }
    }
    this._saveCatalog();
  }

  /**
   * After ALTER TABLE, reconstruct the CREATE TABLE SQL from the current in-memory schema
   * and update the catalog so it persists across close()/open() cycles.
   */
  _updateCatalogAfterAlter(sql) {
    // Handle RENAME TABLE: update the key in _createSqls
    const renameMatch = sql.match(/ALTER\s+TABLE\s+(\w+)\s+RENAME\s+TO\s+(\w+)/i);
    if (renameMatch) {
      const oldName = renameMatch[1];
      const newName = renameMatch[2];
      // Reconstruct from current schema under new name
      const tableObj = this._db.tables.get(newName);
      if (tableObj) {
        this._createSqls.delete(oldName);
        this._createSqls.set(newName, this._reconstructCreateSQL(newName, tableObj.schema));
      }
      // Rename the physical heap file
      const oldPath = join(this._dirPath, `${oldName}.db`);
      const newPath = join(this._dirPath, `${newName}.db`);
      try {
        if (existsSync(oldPath)) renameSync(oldPath, newPath);
      } catch { /* ignore rename errors */ }
      // Update internal maps
      if (this._heaps.has(oldName)) {
        const heap = this._heaps.get(oldName);
        this._heaps.delete(oldName);
        this._heaps.set(newName, heap);
        heap.name = newName;
      }
      if (this._diskManagers.has(oldName)) {
        const dm = this._diskManagers.get(oldName);
        this._diskManagers.delete(oldName);
        this._diskManagers.set(newName, dm);
      }
      if (this._versionMaps.has(oldName)) {
        const vm = this._versionMaps.get(oldName);
        this._versionMaps.delete(oldName);
        this._versionMaps.set(newName, vm);
      }
      this._saveCatalog();
      return;
    }

    // Handle ALTER TABLE ADD/DROP/RENAME COLUMN
    const alterMatch = sql.match(/ALTER\s+TABLE\s+(\w+)/i);
    if (alterMatch) {
      const tableName = alterMatch[1];
      const tableObj = this._db.tables.get(tableName);
      if (tableObj) {
        this._createSqls.set(tableName, this._reconstructCreateSQL(tableName, tableObj.schema));
      }
      this._saveCatalog();
    }
  }

  /**
   * Reconstruct CREATE TABLE SQL from current in-memory schema.
   */
  _reconstructCreateSQL(tableName, schema) {
    const cols = schema.map(col => {
      let def = `${col.name} ${col.type || 'TEXT'}`;
      if (col.primaryKey) def += ' PRIMARY KEY';
      if (col.notNull) def += ' NOT NULL';
      if (col.unique) def += ' UNIQUE';
      const dv = col.default !== undefined && col.default !== null ? col.default : col.defaultValue;
      if (dv !== undefined && dv !== null) {
        const dvStr = typeof dv === 'string' ? `'${dv}'` : dv;
        def += ` DEFAULT ${dvStr}`;
      }
      return def;
    });
    return `CREATE TABLE ${tableName} (${cols.join(', ')})`;
  }

  /**
   * Replay ALTER TABLE DDL during crash recovery — modifies schema only, NOT heap data.
   * Heap data is handled by per-heap DML recovery.
   */
  _replayDDLSchemaOnly(db, sql) {
    const trimmed = sql.trim();
    
    // ALTER TABLE t ADD COLUMN name TYPE [DEFAULT val]
    const addMatch = trimmed.match(/ALTER\s+TABLE\s+(\w+)\s+ADD\s+COLUMN\s+(\w+)\s+(\w+)(?:\s+DEFAULT\s+(.+))?/i);
    if (addMatch) {
      const [, tableName, colName, colType, defaultStr] = addMatch;
      const table = db.tables.get(tableName);
      if (!table) return;
      if (table.schema.find(c => c.name === colName)) return; // Already exists (idempotent)
      const colDef = { name: colName, type: colType.toUpperCase(), primaryKey: false };
      if (defaultStr) {
        const dv = defaultStr.replace(/^'|'$/g, '');
        colDef.defaultValue = isNaN(Number(dv)) ? dv : Number(dv);
      }
      table.schema.push(colDef);
      return;
    }
    
    // ALTER TABLE t DROP COLUMN name
    const dropMatch = trimmed.match(/ALTER\s+TABLE\s+(\w+)\s+DROP\s+COLUMN\s+(\w+)/i);
    if (dropMatch) {
      const [, tableName, colName] = dropMatch;
      const table = db.tables.get(tableName);
      if (!table) return;
      const idx = table.schema.findIndex(c => c.name === colName);
      if (idx >= 0) table.schema.splice(idx, 1);
      return;
    }
    
    // ALTER TABLE t RENAME COLUMN old TO new
    const renameColMatch = trimmed.match(/ALTER\s+TABLE\s+(\w+)\s+RENAME\s+COLUMN\s+(\w+)\s+TO\s+(\w+)/i);
    if (renameColMatch) {
      const [, tableName, oldName, newName] = renameColMatch;
      const table = db.tables.get(tableName);
      if (!table) return;
      const col = table.schema.find(c => c.name === oldName);
      if (col) col.name = newName;
      return;
    }
    
    // ALTER TABLE t RENAME TO new_name
    const renameMatch = trimmed.match(/ALTER\s+TABLE\s+(\w+)\s+RENAME\s+TO\s+(\w+)/i);
    if (renameMatch) {
      const [, oldName, newName] = renameMatch;
      const table = db.tables.get(oldName);
      if (!table) return;
      db.tables.delete(oldName);
      db.tables.set(newName, table);
      table.name = newName;
      // Also need to reassociate the heap with the new table name
      // The heap file may be under either old or new name on disk
      if (table.heap) table.heap.name = newName;
      return;
    }
    
    // Fallback: try full execute for unrecognized ALTER patterns
    try { db.execute(sql); } catch {}
  }

  _saveCatalog() {
    const tables = [];
    for (const [name, sql] of this._createSqls) {
      tables.push({ name, createSql: sql });
    }
    // Save view definitions (non-CTE, non-materialized views)
    const views = [];
    for (const [name, viewDef] of this._db.views) {
      if (!viewDef.isCTE && viewDef.sql) {
        views.push({ name, sql: viewDef.sql });
      }
    }
    // Save triggers
    const triggers = this._db.triggers || [];
    // Save sequences
    const sequences = {};
    for (const [name, seq] of (this._db.sequences || new Map())) {
      sequences[name] = {
        current: seq.current,
        increment: seq.increment,
        min: seq.min,
        max: seq.max,
        cycle: seq.cycle,
        ownedBy: seq.ownedBy,
      };
    }
    writeFileSync(this._catalogPath, JSON.stringify({ tables, views, triggers, sequences }, null, 2), 'utf8');
  }

  _saveMvccState() {
    writeFileSync(this._mvccStatePath, JSON.stringify({
      nextTxId: this._mvcc.nextTxId
    }), 'utf8');
  }
}

/**
 * TransactionSession - connection-level session with explicit transaction control.
 */
export class TransactionSession {
  constructor(id, tdb) {
    this.id = id;
    this._tdb = tdb;
    this._tx = null;
    this._closed = false;
  }

  begin() {
    if (this._tx) throw new Error('Transaction already in progress');
    this._tx = this._tdb._mvcc.begin();
    this._savepoints = new Map(); // name → { undoLogLen, writeSetSnapshot }
    return { type: 'OK', message: 'BEGIN' };
  }

  savepoint(name) {
    if (!this._tx) throw new Error('No transaction in progress');
    this._savepoints.set(name, {
      undoLogLen: this._tx.undoLog.length,
      writeSetSnapshot: new Set(this._tx.writeSet),
    });
    return { type: 'OK', message: `SAVEPOINT ${name}` };
  }

  rollbackToSavepoint(name) {
    if (!this._tx) throw new Error('No transaction in progress');
    const sp = this._savepoints.get(name);
    if (!sp) throw new Error(`Savepoint "${name}" does not exist`);

    // Replay undo log from current position back to savepoint
    for (let i = this._tx.undoLog.length - 1; i >= sp.undoLogLen; i--) {
      try { this._tx.undoLog[i](); } catch (e) { /* ignore */ }
    }
    this._tx.undoLog.length = sp.undoLogLen;

    // Physically remove rows added after savepoint (new inserts/updates)
    const addedKeys = new Set();
    for (const key of this._tx.writeSet) {
      if (!sp.writeSetSnapshot.has(key) && !key.endsWith(':del')) {
        addedKeys.add(key);
      }
    }
    for (const key of addedKeys) {
      const parts = key.split(':');
      const tableName = parts[0];
      const pageId = parseInt(parts[1]);
      const slotIdx = parseInt(parts[2]);
      if (isNaN(pageId) || isNaN(slotIdx)) continue;

      const vm = this._tdb._versionMaps.get(tableName);
      if (vm) {
        const ver = vm.get(`${pageId}:${slotIdx}`);
        if (ver && ver.xmin === this._tx.txId) {
          const tableObj = this._tdb._db.tables.get(tableName);
          if (tableObj && tableObj.heap._origDelete) {
            try {
              tableObj.heap._origDelete(pageId, slotIdx);
              // Write a WAL DELETE record so recovery doesn't replay the INSERT
              if (this._tdb._wal) {
                const walTxId = this._tdb._wal.allocateTxId();
                this._tdb._wal.beginTransaction(walTxId);
                this._tdb._wal.appendDelete(walTxId, tableName, pageId, slotIdx);
                this._tdb._wal.appendCommit(walTxId);
              }
            } catch (e) { /* ignore */ }
          }
          vm.delete(`${pageId}:${slotIdx}`);
        }
      }
    }

    // Restore writeSet to savepoint state
    this._tx.writeSet = new Set(sp.writeSetSnapshot);

    // Remove any savepoints created after this one
    const names = [...this._savepoints.keys()];
    let found = false;
    for (const n of names) {
      if (n === name) { found = true; continue; }
      if (found) this._savepoints.delete(n);
    }

    return { type: 'OK', message: `ROLLBACK TO SAVEPOINT ${name}` };
  }

  releaseSavepoint(name) {
    if (!this._tx) throw new Error('No transaction in progress');
    if (!this._savepoints.has(name)) throw new Error(`Savepoint "${name}" does not exist`);
    this._savepoints.delete(name);
    return { type: 'OK', message: `RELEASE SAVEPOINT ${name}` };
  }

  commit() {
    if (!this._tx) throw new Error('No transaction in progress');
    // Track any new rows from this transaction
    this._tdb._trackNewRows(this._tx);
    // Write WAL delete records before commit
    this._tdb._walLogDeletes(this._tx);
    this._tdb._mvcc.commit(this._tx.txId);
    this._tdb._wal.appendCommit(this._tx.txId);
    // Physicalize committed deletes (no additional WAL)
    this._tdb._physicalizeDeletes(this._tx);
    this._tx = null;
    // Invalidate result cache — committed data may change query results
    if (this._tdb._db._resultCache) this._tdb._db._resultCache.clear();

    // Auto-checkpoint if WAL exceeds threshold
    this._tdb._maybeAutoCheckpoint();

    return { type: 'OK', message: 'COMMIT' };
  }

  rollback() {
    if (!this._tx) throw new Error('No transaction in progress');
    this._tdb._rollbackNewRows(this._tx);
    this._tdb._mvcc.rollback(this._tx.txId);
    this._tdb._wal.appendAbort(this._tx.txId);
    this._tx = null;
    return { type: 'OK', message: 'ROLLBACK' };
  }

  execute(sql) {
    if (this._closed) throw new Error('Session is closed');
    const trimmed = sql.trim().toUpperCase();

    if (trimmed === 'BEGIN' || trimmed === 'BEGIN TRANSACTION' || trimmed === 'START TRANSACTION') {
      return this.begin();
    }
    if (trimmed === 'COMMIT') return this.commit();
    if (trimmed === 'ROLLBACK' || trimmed === 'ABORT') return this.rollback();

    // Savepoint support
    if (trimmed.startsWith('SAVEPOINT ')) {
      const name = sql.trim().replace(/^SAVEPOINT\s+/i, '').replace(/;$/, '').trim();
      return this.savepoint(name);
    }
    if (trimmed.startsWith('ROLLBACK TO SAVEPOINT ') || trimmed.startsWith('ROLLBACK TO ')) {
      const name = sql.trim().replace(/^ROLLBACK\s+TO\s+(?:SAVEPOINT\s+)?/i, '').replace(/;$/, '').trim();
      return this.rollbackToSavepoint(name);
    }
    if (trimmed.startsWith('RELEASE SAVEPOINT ') || trimmed.startsWith('RELEASE ')) {
      const name = sql.trim().replace(/^RELEASE\s+(?:SAVEPOINT\s+)?/i, '').replace(/;$/, '').trim();
      return this.releaseSavepoint(name);
    }

    // DDL: bypass MVCC
    if (this._tdb._isDDL(trimmed)) {
      const result = this._tdb._db.execute(sql);
      if (trimmed.startsWith('CREATE TABLE') || trimmed.startsWith('CREATE INDEX')) {
        this._tdb._trackCreate(sql);
        this._tdb._installScanInterceptors();
      }
      return result;
    }

    // DML with transaction context
    if (this._tx) {
      // FOR UPDATE: mark selected rows in writeSet
      const isForUpdate = trimmed.includes('FOR UPDATE') || trimmed.includes('FOR SHARE');

      // Use explicit transaction
      const prevTx = this._tdb._activeTx;
      this._tdb._activeTx = this._tx;
      this._tdb._setHeapTxId(this._tx.txId);
      // Set txId on inner Database to bypass result cache during transactions
      const prevDbTxId = this._tdb._db._currentTxId;
      this._tdb._db._currentTxId = this._tx.txId;
      // For UPDATE/DELETE, suppress read tracking during scan (WHERE filter
      // causes false SSI dependencies on rows that don't match)
      const isModify = trimmed.startsWith('UPDATE') || trimmed.startsWith('DELETE');
      if (isModify) this._tx.suppressReadTracking = true;
      // For UPDATE: save old row values for rollback
      const isUpdate = trimmed.startsWith('UPDATE');
      let updateSnapshot = null;
      if (isUpdate) {
        updateSnapshot = this._snapshotUpdateRows(sql);
      }
      try {
        const result = this._tdb._db.execute(sql);
        // Track new rows immediately
        this._tdb._trackNewRows(this._tx);
        // For UPDATE: save undo information to restore old values on rollback
        if (isUpdate && updateSnapshot && updateSnapshot.rows.length > 0) {
          const snap = updateSnapshot;
          this._tx.undoLog.push(() => {
            const table = this._tdb._db.tables.get(snap.tableName);
            if (!table) return;
            // Clear result cache — stale results from during the transaction
            if (this._tdb._db._resultCache) {
              this._tdb._db._resultCache.clear();
            }
            const vm = this._tdb._versionMaps.get(snap.tableName);
            // Use origScan to bypass MVCC filtering during undo
            const origScan = table.heap._origScan || table.heap.scan.bind(table.heap);
            const origDelete = table.heap._origDelete || table.heap.delete.bind(table.heap);
            // Clear all current rows from the table (physical, bypass MVCC)
            const toDelete = [];
            for (const { pageId, slotIdx } of origScan()) {
              toDelete.push({ pageId, slotIdx });
            }
            for (const { pageId, slotIdx } of toDelete) {
              try { 
                origDelete(pageId, slotIdx);
                // Also clean up version map for deleted rows
                if (vm) vm.delete(`${pageId}:${slotIdx}`);
              } catch (e) { /* ignore */ }
            }
            // Re-insert all old rows (bypass MVCC interceptors)
            const origInsert = table.heap._origInsert || table.heap.insert.bind(table.heap);
            for (const values of snap.rows) {
              const rid = origInsert ? table.heap.insert(values) : table.heap.insert(values);
              // Don't add version map entry — rows without entries are visible by default
              // (the scan interceptor yields rows with no version entry)
              for (const [colName, index] of table.indexes) {
                const colIdx = snap.schema.findIndex(c => c.name === colName);
                if (colIdx >= 0) {
                  index.insert(values[colIdx], rid);
                }
              }
            }
          });
        }
        // For UPDATE/DELETE, record reads only for actually modified rows
        if (isModify && this._tdb._mvcc.recordRead) {
          this._tx.suppressReadTracking = false;
          // Record reads for any keys in the write set that were added during this statement
          for (const key of this._tx.writeSet) {
            this._tdb._mvcc.recordRead(this._tx.txId, key, this._tx.txId);
          }
        }
        // Check for row lock conflicts: if this tx modified rows locked by another tx
        if (isModify) {
          for (const [, otherSession] of this._tdb._sessions) {
            if (otherSession._tx && otherSession._tx !== this._tx && otherSession._tx.rowLocks) {
              for (const [lockKey] of otherSession._tx.rowLocks) {
                const baseKey = lockKey;
                if (this._tx.writeSet.has(baseKey) || this._tx.writeSet.has(baseKey + ':del')) {
                  throw new Error(`Write-write conflict on ${baseKey} (row locked by another transaction)`);
                }
              }
            }
          }
        }
        // FOR UPDATE: lock selected rows to prevent concurrent modification
        if (isForUpdate && result && result.rows) {
          const tableName = this._extractTableName(sql);
          if (tableName) {
            const vm = this._tdb._versionMaps.get(tableName);
            if (vm) {
              // Use a separate lock table (not xmax) to avoid visibility issues
              if (!this._tx.rowLocks) this._tx.rowLocks = new Map();
              for (const [key, ver] of vm) {
                if (ver.xmax === 0) {
                  const lockKey = `${tableName}:${key}`;
                  // Check if another transaction already locked this row
                  for (const [, otherSession] of this._tdb._sessions) {
                    if (otherSession._tx && otherSession._tx !== this._tx && otherSession._tx.rowLocks?.has(lockKey)) {
                      throw new Error(`Row lock conflict on ${lockKey}`);
                    }
                  }
                  this._tx.rowLocks.set(lockKey, this._tx.txId);
                  this._tx.writeSet.add(lockKey);
                }
              }
            }
          }
        }
        return result;
      } finally {
        this._tdb._activeTx = prevTx;
        this._tdb._setHeapTxId(prevTx ? prevTx.txId : 0);
        this._tdb._db._currentTxId = prevDbTxId;
      }
    }

    // No explicit transaction - auto-commit
    return this._tdb.execute(sql);
  }

  close() {
    if (this._tx) try { this.rollback(); } catch (e) { /* ignore */ }
    this._tdb._sessions.delete(this.id);
    this._closed = true;
  }

  _snapshotUpdateRows(sql) {
    // Parse UPDATE table to find table name
    const tableMatch = sql.match(/UPDATE\s+(\w+)/i);
    if (!tableMatch) return null;
    const tableName = tableMatch[1];
    const table = this._tdb._db.tables.get(tableName);
    if (!table) return null;

    // Snapshot VISIBLE rows (through MVCC) that existed BEFORE this transaction
    const txId = this._tx.txId;
    const vm = this._tdb._versionMaps.get(tableName);
    const snapshot = [];
    // Use the regular (MVCC-filtered) scan to only get visible rows
    for (const { pageId, slotIdx, values } of table.heap.scan()) {
      if (vm) {
        const key = `${pageId}:${slotIdx}`;
        const ver = vm.get(key);
        // Skip rows created by this transaction - they'll be rolled back separately
        if (ver && ver.xmin === txId) continue;
      }
      snapshot.push([...values]);
    }
    return { tableName, rows: snapshot, schema: table.schema };
  }

  _extractTableName(sql) {
    const match = sql.match(/FROM\s+(\w+)/i);
    return match ? match[1].toLowerCase() : null;
  }
}
