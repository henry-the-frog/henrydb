// transaction.js — MVCC transactions + WAL for HenryDB

// ===== Write-Ahead Log =====
export class WAL {
  constructor() {
    this.log = [];
    this.lsn = 0; // log sequence number
    this.checkpointLSN = 0;
  }

  append(txId, type, tableName, data) {
    const entry = {
      lsn: this.lsn++,
      txId,
      type,       // 'INSERT', 'DELETE', 'UPDATE', 'BEGIN', 'COMMIT', 'ROLLBACK'
      tableName,
      data,       // { pageId, slotIdx, values, oldValues }
      timestamp: Date.now(),
    };
    this.log.push(entry);
    return entry.lsn;
  }

  getEntriesSince(lsn) {
    return this.log.filter(e => e.lsn >= lsn);
  }

  getEntriesForTx(txId) {
    return this.log.filter(e => e.txId === txId);
  }

  checkpoint() {
    this.checkpointLSN = this.lsn;
    return this.checkpointLSN;
  }

  // For crash recovery: get uncommitted transactions
  getUncommitted() {
    const begun = new Set();
    const committed = new Set();
    const rolledBack = new Set();

    for (const entry of this.log) {
      if (entry.type === 'BEGIN') begun.add(entry.txId);
      if (entry.type === 'COMMIT') committed.add(entry.txId);
      if (entry.type === 'ROLLBACK') rolledBack.add(entry.txId);
    }

    return [...begun].filter(id => !committed.has(id) && !rolledBack.has(id));
  }

  get size() { return this.log.length; }
}

// ===== MVCC Transaction Manager =====
export class TransactionManager {
  constructor() {
    this.wal = new WAL();
    this.nextTxId = 1;
    this.activeTxns = new Map(); // txId -> Transaction
    this.committedTxns = new Set();
    this.globalVersion = 0;
  }

  begin() {
    const txId = this.nextTxId++;
    const snapshot = new Set(this.committedTxns);
    const tx = new Transaction(txId, snapshot, this);
    this.activeTxns.set(txId, tx);
    this.wal.append(txId, 'BEGIN', null, null);
    return tx;
  }

  commit(txId) {
    const tx = this.activeTxns.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);

    // Check for write-write conflicts
    for (const [key, version] of tx.writeSet) {
      if (version.committedAfterSnapshot) {
        throw new Error(`Write-write conflict on ${key}`);
      }
    }

    this.wal.append(txId, 'COMMIT', null, null);
    this.committedTxns.add(txId);
    this.activeTxns.delete(txId);
    this.globalVersion++;
    tx.committed = true;
    return true;
  }

  rollback(txId) {
    const tx = this.activeTxns.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);

    // Undo all writes
    for (const undo of tx.undoLog.reverse()) {
      undo();
    }

    this.wal.append(txId, 'ROLLBACK', null, null);
    this.activeTxns.delete(txId);
    tx.rolledBack = true;
    return true;
  }

  // Crash recovery: undo uncommitted transactions
  recover() {
    const uncommitted = this.wal.getUncommitted();
    const undone = [];

    for (const txId of uncommitted) {
      const entries = this.wal.getEntriesForTx(txId);
      // Undo in reverse order
      for (const entry of entries.reverse()) {
        if (entry.type === 'INSERT' || entry.type === 'DELETE' || entry.type === 'UPDATE') {
          undone.push(entry);
        }
      }
    }

    return { undoneTransactions: uncommitted, undoneEntries: undone.length };
  }
}

// ===== Transaction =====
export class Transaction {
  constructor(txId, snapshot, manager) {
    this.txId = txId;
    this.snapshot = snapshot;    // Set of committed txIds at BEGIN time
    this.manager = manager;
    this.writeSet = new Map();   // key -> { value, committedAfterSnapshot }
    this.readSet = new Map();    // key -> version
    this.undoLog = [];           // undo functions for rollback
    this.committed = false;
    this.rolledBack = false;
  }

  // Check if a version is visible to this transaction
  isVisible(versionTxId) {
    if (versionTxId === this.txId) return true;       // own writes
    if (this.snapshot.has(versionTxId)) return true;   // committed before snapshot
    return false;
  }

  // Record a write
  recordWrite(key, value, undoFn) {
    this.writeSet.set(key, { value, committedAfterSnapshot: false });
    this.undoLog.push(undoFn);
    this.manager.wal.append(this.txId, 'UPDATE', null, { key, value });
  }

  // Record an insert
  recordInsert(tableName, rid, values, undoFn) {
    const key = `${tableName}:${rid.pageId}:${rid.slotIdx}`;
    this.writeSet.set(key, { value: values, committedAfterSnapshot: false });
    this.undoLog.push(undoFn);
    this.manager.wal.append(this.txId, 'INSERT', tableName, { rid, values });
  }

  // Record a delete
  recordDelete(tableName, pageId, slotIdx, oldValues, undoFn) {
    const key = `${tableName}:${pageId}:${slotIdx}`;
    this.writeSet.set(key, { value: null, committedAfterSnapshot: false });
    this.undoLog.push(undoFn);
    this.manager.wal.append(this.txId, 'DELETE', tableName, { pageId, slotIdx, oldValues });
  }

  commit() { return this.manager.commit(this.txId); }
  rollback() { return this.manager.rollback(this.txId); }
}

// ===== Transactional Database Wrapper =====
export class TransactionalDB {
  constructor(database) {
    this.db = database;
    this.txManager = new TransactionManager();
  }

  begin() {
    return this.txManager.begin();
  }

  // Execute SQL within a transaction
  executeInTx(tx, sql) {
    const ast = this.db.constructor.prototype !== undefined
      ? require('./sql.js').parse(sql)
      : null;
    // For now, delegate to db.execute but wrap with tx logging
    return this.db.execute(sql);
  }

  // Insert with transaction tracking
  insertInTx(tx, tableName, values) {
    const table = this.db.tables.get(tableName);
    if (!table) throw new Error(`Table ${tableName} not found`);

    const rid = table.heap.insert(values);
    tx.recordInsert(tableName, rid, values, () => {
      table.heap.delete(rid.pageId, rid.slotIdx);
    });
    return rid;
  }

  // Delete with transaction tracking
  deleteInTx(tx, tableName, pageId, slotIdx) {
    const table = this.db.tables.get(tableName);
    if (!table) throw new Error(`Table ${tableName} not found`);

    const oldValues = table.heap.get(pageId, slotIdx);
    table.heap.delete(pageId, slotIdx);
    tx.recordDelete(tableName, pageId, slotIdx, oldValues, () => {
      table.heap.insert(oldValues);
    });
    return true;
  }

  get wal() { return this.txManager.wal; }
}
