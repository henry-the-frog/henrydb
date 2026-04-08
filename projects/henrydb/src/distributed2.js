// distributed2.js — 2PC coordinator, epoch reclamation, BRIN index

/**
 * Two-Phase Commit Coordinator — distributed atomicity.
 */
export class TwoPhaseCommitCoordinator {
  constructor() {
    this._txns = new Map(); // txnId → {participants, votes, state}
    this._nextId = 0;
  }

  begin(participants) {
    const txnId = ++this._nextId;
    this._txns.set(txnId, {
      participants: new Set(participants),
      votes: new Map(),
      state: 'INIT', // INIT → PREPARING → COMMITTED/ABORTED
    });
    return txnId;
  }

  /** Phase 1: Prepare — collect votes from participants */
  prepare(txnId) {
    const txn = this._txns.get(txnId);
    if (!txn) return { ok: false, reason: 'unknown txn' };
    txn.state = 'PREPARING';
    return { ok: true, participants: [...txn.participants] };
  }

  /** Record a participant's vote */
  vote(txnId, participant, voteYes) {
    const txn = this._txns.get(txnId);
    if (!txn) return;
    txn.votes.set(participant, voteYes);
  }

  /** Phase 2: Decide — commit if all voted yes, abort otherwise */
  decide(txnId) {
    const txn = this._txns.get(txnId);
    if (!txn) return { decision: 'ABORT', reason: 'unknown txn' };
    
    // Check if all votes received
    if (txn.votes.size < txn.participants.size) {
      return { decision: 'WAITING', received: txn.votes.size, expected: txn.participants.size };
    }
    
    // All voted yes?
    const allYes = [...txn.votes.values()].every(v => v);
    txn.state = allYes ? 'COMMITTED' : 'ABORTED';
    return { decision: txn.state };
  }

  getState(txnId) { return this._txns.get(txnId)?.state || 'UNKNOWN'; }
}

/**
 * Epoch-Based Reclamation — safe memory reclamation.
 */
export class EpochManager {
  constructor() {
    this._globalEpoch = 0;
    this._threads = new Map(); // threadId → {active, epoch}
    this._retired = []; // [{data, epoch}]
  }

  enter(threadId) {
    this._threads.set(threadId, { active: true, epoch: this._globalEpoch });
  }

  exit(threadId) {
    const thread = this._threads.get(threadId);
    if (thread) thread.active = false;
  }

  /** Advance global epoch */
  advance() {
    this._globalEpoch++;
    return this._globalEpoch;
  }

  /** Schedule data for reclamation */
  retire(data) {
    this._retired.push({ data, epoch: this._globalEpoch });
  }

  /** Reclaim data from epochs no thread is reading */
  reclaim() {
    const minEpoch = this._minActiveEpoch();
    const reclaimable = this._retired.filter(r => r.epoch < minEpoch);
    this._retired = this._retired.filter(r => r.epoch >= minEpoch);
    return reclaimable.map(r => r.data);
  }

  _minActiveEpoch() {
    let min = this._globalEpoch;
    for (const thread of this._threads.values()) {
      if (thread.active && thread.epoch < min) min = thread.epoch;
    }
    return min;
  }

  get currentEpoch() { return this._globalEpoch; }
  get retiredCount() { return this._retired.length; }
}

/**
 * BRIN Index — Block Range Index for sorted/clustered data.
 */
export class BRINIndex {
  constructor(blockSize = 128) {
    this.blockSize = blockSize;
    this._entries = []; // [{blockId, minVal, maxVal, offset}]
  }

  /** Build from sorted data */
  static build(data, keyFn, blockSize = 128) {
    const idx = new BRINIndex(blockSize);
    for (let i = 0; i < data.length; i += blockSize) {
      const block = data.slice(i, i + blockSize);
      const keys = block.map(keyFn);
      idx._entries.push({
        blockId: Math.floor(i / blockSize),
        minVal: Math.min(...keys),
        maxVal: Math.max(...keys),
        offset: i,
        count: block.length,
      });
    }
    return idx;
  }

  /** Find blocks that might contain a value */
  lookup(value) {
    return this._entries.filter(e => value >= e.minVal && value <= e.maxVal);
  }

  /** Find blocks overlapping a range */
  rangeBlocks(lo, hi) {
    return this._entries.filter(e => e.maxVal >= lo && e.minVal <= hi);
  }

  get blockCount() { return this._entries.length; }
  
  /** Selectivity estimate */
  selectivity(lo, hi) {
    const matching = this.rangeBlocks(lo, hi);
    return this._entries.length > 0 ? matching.length / this._entries.length : 0;
  }
}
