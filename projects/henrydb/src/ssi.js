// ssi.js — Serializable Snapshot Isolation for HenryDB
// Extends MVCC to detect and prevent serialization anomalies.
//
// Based on: "Serializable Snapshot Isolation in PostgreSQL"
// (Ports & Grittner, VLDB 2012) and the original theory
// (Cahill, Röhm, Fekete, SIGMOD 2008)
//
// Key insight: A serialization anomaly requires a "dangerous structure"
// where two consecutive rw-antidependency edges form a cycle.
// SSI tracks these dependencies and aborts one transaction when detected.

import { MVCCStore } from './mvcc.js';

/**
 * rw-antidependency: T1 reads data that T2 later writes.
 * Represented as an edge T1 →rw→ T2 in the dependency graph.
 */
class RWDependency {
  constructor(reader, writer, key) {
    this.reader = reader;   // txId that read
    this.writer = writer;   // txId that wrote
    this.key = key;         // the data item
    this.timestamp = Date.now();
  }
}

/**
 * SSI Manager — extends MVCC with serialization anomaly detection.
 * 
 * Tracks:
 * - inConflicts: for each tx, who has an rw-dep INTO this tx (someone read what this tx wrote)
 * - outConflicts: for each tx, who has an rw-dep OUT of this tx (this tx read what someone else wrote)
 * 
 * Dangerous structure: tx has both inConflict and outConflict with different transactions.
 * This means T_in →rw→ tx →rw→ T_out, which is the pattern that causes anomalies.
 */
export class SSIManager extends MVCCStore {
  constructor() {
    super();
    
    // rw-dependency tracking
    // txId → Set<txId> — transactions that have rw-dependency INTO this tx
    this.inConflicts = new Map();
    // txId → Set<txId> — transactions that have rw-dependency OUT of this tx
    this.outConflicts = new Map();
    
    // Read tracking: what each transaction has read
    // txId → Map<key, version> — read set with version info
    this.readSets = new Map();
    
    // Track committed transaction info for conflict checking
    this.committedInfo = new Map(); // txId → { commitTime, writeSet }
  }

  begin() {
    const tx = super.begin();
    this.inConflicts.set(tx.txId, new Set());
    this.outConflicts.set(tx.txId, new Set());
    this.readSets.set(tx.txId, new Map());
    return tx;
  }

  /**
   * Record a read operation for SSI tracking.
   * Called by the MVCC scan interceptor when a transaction reads a row.
   */
  recordRead(txId, key, readVersion) {
    const readSet = this.readSets.get(txId);
    if (readSet) {
      readSet.set(key, readVersion);
    }
    
    // Check if any committed transaction wrote to this key
    for (const [committedTxId, info] of this.committedInfo) {
      if (info.writeSet.has(key) && !this._wasVisibleInSnapshot(committedTxId, this.activeTxns.get(txId)?.snapshot)) {
        this._addRWDependency(txId, committedTxId);
      }
    }
    
    // Also check active transactions that have already written this key
    for (const [otherTxId, otherTx] of this.activeTxns) {
      if (otherTxId === txId) continue;
      if (otherTx.writeSet.has(key)) {
        // We're reading something that another active tx has written → us →rw→ them
        this._addRWDependency(txId, otherTxId);
      }
    }
  }

  /**
   * Record a write operation for SSI tracking.
   * Called when a transaction writes to a key.
   */
  recordWrite(txId, key) {
    // Check if any active or recently committed (but overlapping) transaction read this key
    // If so, they have an rw-antidependency: them →rw→ us
    const currentTx = this.activeTxns.get(txId);
    for (const [otherTxId, readSet] of this.readSets) {
      if (otherTxId === txId) continue;
      if (readSet.has(key)) {
        // Only create dependency if otherTx was concurrent with us
        // (i.e., otherTx started before our snapshot or was active when we began)
        // Skip if otherTx committed before we started (truly sequential)
        if (currentTx && this._wasVisibleInSnapshot(otherTxId, currentTx.snapshot)) {
          continue; // otherTx committed before our snapshot — no conflict possible
        }
        // otherTx read this key, and we're writing it
        // rw-antidependency: otherTx →rw→ us
        this._addRWDependency(otherTxId, txId);
      }
    }
  }

  /**
   * Check if a transaction was visible in a snapshot.
   * A txId is visible if it was committed before the snapshot was taken.
   */
  _wasVisibleInSnapshot(txId, snapshot) {
    if (!snapshot) return false;
    // Below xmin: was committed before our snapshot (visible)
    if (txId < snapshot.xmin) return true;
    // At or above xmax: started after our snapshot (invisible)
    if (txId >= snapshot.xmax) return false;
    // Between xmin and xmax: was it active at snapshot time?
    // If in activeSet, it was in-progress → invisible
    if (snapshot.activeSet && snapshot.activeSet.has(txId)) return false;
    // Between xmin and xmax but not active → was committed → visible
    return true;
  }

  /**
   * Add an rw-antidependency edge: reader →rw→ writer
   */
  _addRWDependency(readerTxId, writerTxId) {
    // reader has an outConflict to writer
    const outSet = this.outConflicts.get(readerTxId);
    if (outSet) outSet.add(writerTxId);
    
    // writer has an inConflict from reader
    const inSet = this.inConflicts.get(writerTxId);
    if (inSet) inSet.add(readerTxId);
    
    // Check for dangerous structure immediately
    this._checkDangerousStructure(readerTxId);
    this._checkDangerousStructure(writerTxId);
  }

  /**
   * Check if transaction txId is the pivot of a dangerous structure.
   * Dangerous: T_in →rw→ txId →rw→ T_out
   * where T_in committed before txId's snapshot (or txId committed before T_out's snapshot)
   */
  _checkDangerousStructure(txId) {
    const inSet = this.inConflicts.get(txId);
    const outSet = this.outConflicts.get(txId);
    
    if (!inSet || !outSet || inSet.size === 0 || outSet.size === 0) return;
    
    // txId has both incoming and outgoing rw-dependencies.
    // This is a dangerous structure: T_in →rw→ txId →rw→ T_out
    // One of the three transactions must be aborted.
    
    // Check if any in-conflict tx is committed and any out-conflict tx is committed
    // If both are committed and txId is trying to commit, abort txId
    // If txId is committed and one of them is trying to commit, abort the one trying
    
    // For now, mark the transaction as having a dangerous structure
    const tx = this.activeTxns.get(txId);
    if (tx) {
      tx._ssiDangerous = true;
      tx._ssiInConflict = [...inSet];
      tx._ssiOutConflict = [...outSet];
    }
  }

  /**
   * Override commit to check for dangerous structures.
   * If the committing transaction is the pivot of a dangerous structure,
   * and both the in-conflict and out-conflict transactions are committed,
   * then abort this transaction.
   */
  commit(txId) {
    const tx = this.activeTxns.get(txId);
    if (!tx) throw new Error(`Transaction ${txId} not found`);
    
    // Check for dangerous structure at commit time
    const inSet = this.inConflicts.get(txId) || new Set();
    const outSet = this.outConflicts.get(txId) || new Set();
    
    if (inSet.size > 0 && outSet.size > 0) {
      // This transaction is the pivot of a dangerous structure.
      // Check if any of the conflicting transactions are committed.
      const hasCommittedIn = [...inSet].some(id => this.committedTxns.has(id));
      const hasCommittedOut = [...outSet].some(id => this.committedTxns.has(id));
      
      if (hasCommittedIn && hasCommittedOut) {
        // Both sides are committed — this transaction would create a non-serializable execution
        this.rollback(txId);
        throw new Error(`Serialization failure: transaction ${txId} would create a non-serializable schedule (dangerous structure detected)`);
      }
    }
    
    // Also check: is this transaction the out-conflict of a pivot that's already committed?
    for (const inTxId of inSet) {
      if (this.committedTxns.has(inTxId)) {
        const inOutSet = this.outConflicts.get(inTxId);
        if (inOutSet) {
          for (const outTxId of inOutSet) {
            if (outTxId !== txId && this.committedTxns.has(outTxId)) {
              // inTxId (committed) →rw→ txId →rw→ outTxId (committed)
              // But inTxId is the pivot here, and it already committed.
              // We need to check if inTxId's in-conflicts include someone relevant.
              // For safety, abort this transaction.
              const inIn = this.inConflicts.get(inTxId);
              if (inIn && inIn.size > 0) {
                this.rollback(txId);
                throw new Error(`Serialization failure: transaction ${txId} creates cycle via ${inTxId}`);
              }
            }
          }
        }
      }
    }
    
    // Store write set info before committing
    this.committedInfo.set(txId, { 
      commitTime: Date.now(), 
      writeSet: new Set(tx.writeSet) 
    });
    
    // Call parent commit (which does write-write conflict check)
    super.commit(txId);
    
    // Clean up old committed info (keep recent ones for conflict checking)
    this._cleanupOldInfo();
  }

  rollback(txId) {
    super.rollback(txId);
    this._cleanupTx(txId);
  }

  _cleanupTx(txId) {
    // Remove from conflict maps
    this.inConflicts.delete(txId);
    this.outConflicts.delete(txId);
    this.readSets.delete(txId);
    
    // Remove references to this tx from other conflict sets
    for (const [, inSet] of this.inConflicts) {
      inSet.delete(txId);
    }
    for (const [, outSet] of this.outConflicts) {
      outSet.delete(txId);
    }
  }

  _cleanupOldInfo() {
    // Keep only recently committed tx info (for active transactions to check against)
    const activeMinTx = Math.min(...[...this.activeTxns.keys()], this.nextTxId);
    for (const [txId] of this.committedInfo) {
      if (txId < activeMinTx - 100) { // Keep some buffer
        this.committedInfo.delete(txId);
      }
    }
  }
  
  /**
   * Compute the minimum active transaction ID for VACUUM horizon.
   */
  computeXminHorizon() {
    if (this.activeTxns.size === 0) return this.nextTxId;
    return Math.min(...this.activeTxns.keys());
  }
}
