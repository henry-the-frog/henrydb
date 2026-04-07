// mvcc.js — Multi-Version Concurrency Control for HenryDB
// Implements snapshot isolation with version chains

// ===== MVCC Transaction Manager =====

export class MVCCManager {
  constructor() {
    this.nextTxId = 1;
    this.activeTxns = new Map();  // txId → MVCCTransaction
    this.committedTxns = new Set();
    this.abortedTxns = new Set();
    
    // WAL for durability
    this.wal = [];
    this.walLSN = 0;
  }

  begin() {
    const txId = this.nextTxId++;
    // Snapshot: all committed txns at this point
    const snapshot = new Set(this.committedTxns);
    const tx = new MVCCTransaction(txId, snapshot, this);
    this.activeTxns.set(txId, tx);
    this._walAppend(txId, 'BEGIN');
    return tx;
  }

  commit(txId) {
    const tx = this.activeTxns.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);
    if (tx.aborted) throw new Error(`Transaction ${txId} already aborted`);

    // Write-write conflict detection (first-writer-wins)
    for (const key of tx.writeSet) {
      for (const [otherTxId, otherTx] of this.activeTxns) {
        if (otherTxId !== txId && otherTx.writeSet.has(key) && this.committedTxns.has(otherTxId)) {
          // Another transaction committed a write to the same key after our snapshot
          throw new Error(`Write-write conflict on ${key}`);
        }
      }
    }

    this._walAppend(txId, 'COMMIT');
    this.committedTxns.add(txId);
    this.activeTxns.delete(txId);
    tx.committed = true;
  }

  rollback(txId) {
    const tx = this.activeTxns.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);

    // Execute undo actions in reverse
    for (let i = tx.undoLog.length - 1; i >= 0; i--) {
      tx.undoLog[i]();
    }

    this._walAppend(txId, 'ROLLBACK');
    this.abortedTxns.add(txId);
    this.activeTxns.delete(txId);
    tx.aborted = true;
  }

  // Is a given txId visible to a reader transaction?
  isVisible(versionTxId, readerTx) {
    // txId 0 = always visible (recovered rows, auto-committed without transaction)
    if (versionTxId === 0) return true;
    // Own writes are visible
    if (versionTxId === readerTx.txId) return true;
    // Must be committed AND in our snapshot
    if (readerTx.snapshot.has(versionTxId)) return true;
    // Not visible
    return false;
  }

  // Is a row deleted for this reader?
  isDeleted(xmax, readerTx) {
    if (xmax === 0) return false; // Not deleted
    // The deletion is visible if the deleting tx is committed and in our snapshot
    return this.isVisible(xmax, readerTx);
  }

  _walAppend(txId, type, data) {
    this.wal.push({ lsn: this.walLSN++, txId, type, data, ts: Date.now() });
  }

  // Compute the xmin horizon: the smallest txId that any active transaction might need to see
  // Dead tuples with xmax < xmin_horizon can be safely removed
  computeXminHorizon() {
    if (this.activeTxns.size === 0) {
      // No active transactions — everything committed is visible, everything deleted is reclaimable
      return this.nextTxId;
    }
    // The horizon is the minimum snapshot boundary among all active transactions
    let minTxId = this.nextTxId;
    for (const [txId, tx] of this.activeTxns) {
      // The oldest txId this transaction might need to see
      // is the smallest committed txId NOT in its snapshot... actually:
      // the snapshot contains all committed txIds at BEGIN time.
      // Any txId < txId is potentially needed. The horizon is the min txId of all active txns.
      if (txId < minTxId) minTxId = txId;
    }
    return minTxId;
  }
}

// ===== MVCC Transaction =====

export class MVCCTransaction {
  constructor(txId, snapshot, manager) {
    this.txId = txId;
    this.snapshot = snapshot;
    this.manager = manager;
    this.writeSet = new Set();   // keys written
    this.undoLog = [];           // undo functions
    this.committed = false;
    this.aborted = false;
  }

  commit() { return this.manager.commit(this.txId); }
  rollback() { return this.manager.rollback(this.txId); }
}

// ===== MVCC Heap — wraps HeapFile with version metadata =====

export class MVCCHeap {
  constructor(heapFile) {
    this.heap = heapFile;
    // Version metadata: Map<"pageId:slotIdx" → { xmin, xmax }>
    this.versions = new Map();
  }

  // Insert a row within a transaction
  insert(values, tx) {
    const rid = this.heap.insert(values);
    const key = `${rid.pageId}:${rid.slotIdx}`;
    this.versions.set(key, { xmin: tx.txId, xmax: 0 });
    tx.writeSet.add(key);
    tx.undoLog.push(() => {
      this.heap.delete(rid.pageId, rid.slotIdx);
      this.versions.delete(key);
    });
    tx.manager._walAppend(tx.txId, 'INSERT', { table: this.heap.name, rid, values });
    return rid;
  }

  // Delete a row within a transaction (marks xmax)
  delete(pageId, slotIdx, tx) {
    const key = `${pageId}:${slotIdx}`;
    const ver = this.versions.get(key);
    if (!ver) throw new Error(`No version info for ${key}`);
    
    // Check: can only delete if we can see the row
    if (!tx.manager.isVisible(ver.xmin, tx)) {
      throw new Error(`Row ${key} not visible to transaction ${tx.txId}`);
    }
    if (ver.xmax !== 0 && tx.manager.isVisible(ver.xmax, tx)) {
      throw new Error(`Row ${key} already deleted`);
    }
    
    // Write-write conflict: if another active tx has set xmax
    if (ver.xmax !== 0 && ver.xmax !== tx.txId) {
      const otherTx = tx.manager.activeTxns.get(ver.xmax);
      if (otherTx && !otherTx.committed && !otherTx.aborted) {
        throw new Error(`Write-write conflict: row ${key} being deleted by tx ${ver.xmax}`);
      }
    }

    const oldXmax = ver.xmax;
    ver.xmax = tx.txId;
    tx.writeSet.add(key);
    tx.undoLog.push(() => { ver.xmax = oldXmax; });
    tx.manager._walAppend(tx.txId, 'DELETE', { table: this.heap.name, pageId, slotIdx });
  }

  // Update = delete old + insert new (in MVCC, updates create new versions)
  update(pageId, slotIdx, newValues, tx) {
    this.delete(pageId, slotIdx, tx);
    return this.insert(newValues, tx);
  }

  // Scan visible rows for a transaction
  *scan(tx) {
    for (const { pageId, slotIdx, values } of this.heap.scan()) {
      const key = `${pageId}:${slotIdx}`;
      const ver = this.versions.get(key);
      if (!ver) continue; // No version info — shouldn't happen

      // Visibility check
      const created = tx.manager.isVisible(ver.xmin, tx);
      const deleted = ver.xmax !== 0 && tx.manager.isVisible(ver.xmax, tx);

      if (created && !deleted) {
        yield { pageId, slotIdx, values };
      }
    }
  }

  // Scan without transaction context (for non-transactional queries)
  *scanAll() {
    for (const { pageId, slotIdx, values } of this.heap.scan()) {
      const key = `${pageId}:${slotIdx}`;
      const ver = this.versions.get(key);
      // Show rows that are either unversioned or have xmax=0
      if (!ver || ver.xmax === 0) {
        yield { pageId, slotIdx, values };
      }
    }
  }

  // Insert without transaction (for backward compat)
  insertDirect(values) {
    const rid = this.heap.insert(values);
    const key = `${rid.pageId}:${rid.slotIdx}`;
    // Mark as committed by pseudo-tx 0 (always visible)
    this.versions.set(key, { xmin: 0, xmax: 0 });
    return rid;
  }

  // Delete without transaction
  deleteDirect(pageId, slotIdx) {
    const key = `${pageId}:${slotIdx}`;
    const ver = this.versions.get(key);
    if (ver) ver.xmax = -1; // Special: always-deleted
    return this.heap.delete(pageId, slotIdx);
  }

  get(pageId, slotIdx) {
    return this.heap.get(pageId, slotIdx);
  }

  get pageCount() { return this.heap.pageCount; }
  get tupleCount() { return this.heap.tupleCount; }

  // ===== VACUUM: Remove dead tuples =====

  vacuum(manager) {
    const horizon = manager.computeXminHorizon();
    let deadCount = 0;
    let freedBytes = 0;
    const deadSlots = []; // { pageId, slotIdx }

    // Phase 1: Identify dead tuples
    for (const [key, ver] of this.versions) {
      if (ver.xmax === 0) continue; // Not deleted
      if (ver.xmax === -1) {
        // Directly deleted (non-transactional) — always reclaimable
        deadSlots.push(key);
        deadCount++;
        continue;
      }
      // Dead if: xmax < horizon AND xmax is committed (or aborted creator)
      if (ver.xmax < horizon && (manager.committedTxns.has(ver.xmax) || manager.abortedTxns.has(ver.xmin))) {
        deadSlots.push(key);
        deadCount++;
      }
    }

    // Phase 2: Remove dead tuples from heap and version map
    for (const key of deadSlots) {
      const [pageId, slotIdx] = key.split(':').map(Number);
      // Get the tuple size before deletion
      const page = this.heap.pages.find(p => p.id === pageId);
      if (page) {
        const tuple = page.getTuple(slotIdx);
        if (tuple) freedBytes += tuple.length;
        page.deleteTuple(slotIdx);
      }
      this.versions.delete(key);
    }

    // Phase 3: Page compaction — defragment pages
    let pagesCompacted = 0;
    for (const page of this.heap.pages) {
      if (this._compactPage(page)) pagesCompacted++;
      // Update FSM with new free space
      if (this.heap.fsm) this.heap.fsm.update(page.id, page.freeSpace());
    }

    return {
      deadTuplesRemoved: deadCount,
      bytesFreed: freedBytes,
      pagesCompacted,
      xminHorizon: horizon,
    };
  }

  // Compact a page: remove gaps left by deleted tuples
  // Rebuilds the page by re-inserting all live tuples
  _compactPage(page) {
    const liveTuples = [];
    const n = page.getNumSlots();
    let hasGaps = false;

    for (let i = 0; i < n; i++) {
      const tuple = page.getTuple(i);
      if (tuple) {
        liveTuples.push({ slotIdx: i, data: new Uint8Array(tuple) });
      } else {
        hasGaps = true;
      }
    }

    if (!hasGaps) return false;

    // Rebuild page: reset free space, re-insert live tuples
    const pageId = page.id;
    page.setNumSlots(0);
    page.setFreeSpaceEnd(4096); // PAGE_SIZE

    const slotMapping = new Map(); // oldSlotIdx → newSlotIdx

    for (const { slotIdx: oldIdx, data } of liveTuples) {
      const newIdx = page.insertTuple(data);
      slotMapping.set(oldIdx, newIdx);
    }

    // Update version map with new slot indices
    const updates = [];
    for (const [key, ver] of this.versions) {
      const [pid, sid] = key.split(':').map(Number);
      if (pid === pageId && slotMapping.has(sid)) {
        const newSid = slotMapping.get(sid);
        if (newSid !== sid) {
          updates.push({ oldKey: key, newKey: `${pid}:${newSid}`, ver });
        }
      }
    }

    for (const { oldKey, newKey, ver } of updates) {
      this.versions.delete(oldKey);
      this.versions.set(newKey, ver);
    }

    return true;
  }
}
