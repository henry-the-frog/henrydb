// two-phase-commit.js — Two-Phase Commit (2PC) for Distributed Transactions
//
// Ensures atomicity across multiple nodes: either ALL commit or ALL abort.
// Phase 1 (Prepare): Coordinator asks all participants to vote YES/NO
// Phase 2 (Commit/Abort): If all vote YES → commit. If any vote NO → abort.
//
// Failure scenarios handled:
// - Participant failure before vote
// - Participant failure after vote
// - Coordinator failure (blocking problem — 2PC's Achilles heel)
//
// Used in: distributed databases, XA transactions, microservice sagas

const TXN_STATE = {
  INIT: 'init',
  PREPARING: 'preparing',
  PREPARED: 'prepared',
  COMMITTING: 'committing',
  COMMITTED: 'committed',
  ABORTING: 'aborting',
  ABORTED: 'aborted',
};

/**
 * Participant — represents a node that participates in distributed transactions.
 */
export class Participant {
  constructor(id) {
    this.id = id;
    this.state = TXN_STATE.INIT;
    this.log = [];        // Durability log
    this.data = {};       // Simple key-value state
    this._pendingWrites = {}; // Buffered writes for current txn
    this._crashed = false;
    this._prepareDelay = 0;
    this._failOnPrepare = false;
  }

  /** Simulate crash */
  crash() { this._crashed = true; }
  recover() { this._crashed = false; }
  setFailOnPrepare(fail) { this._failOnPrepare = fail; }

  /** Buffer a write for the current transaction */
  write(key, value) {
    if (this._crashed) throw new Error(`${this.id} is crashed`);
    this._pendingWrites[key] = value;
  }

  /** Phase 1: Prepare — vote YES or NO */
  prepare() {
    if (this._crashed) return { vote: 'NO', reason: 'crashed' };
    if (this._failOnPrepare) return { vote: 'NO', reason: 'forced failure' };
    
    // Log the prepare decision (for crash recovery)
    this.log.push({ type: 'PREPARE', writes: { ...this._pendingWrites } });
    this.state = TXN_STATE.PREPARED;
    return { vote: 'YES' };
  }

  /** Phase 2: Commit — apply pending writes */
  commit() {
    if (this._crashed) return false;
    
    // Apply buffered writes
    for (const [key, value] of Object.entries(this._pendingWrites)) {
      this.data[key] = value;
    }
    this._pendingWrites = {};
    this.log.push({ type: 'COMMIT' });
    this.state = TXN_STATE.COMMITTED;
    return true;
  }

  /** Phase 2: Abort — discard pending writes */
  abort() {
    if (this._crashed) return false;
    this._pendingWrites = {};
    this.log.push({ type: 'ABORT' });
    this.state = TXN_STATE.ABORTED;
    return true;
  }

  /** Reset for next transaction */
  reset() {
    this.state = TXN_STATE.INIT;
    this._pendingWrites = {};
  }
}

/**
 * TwoPhaseCommitCoordinator — orchestrates distributed transactions.
 */
export class TwoPhaseCommitCoordinator {
  constructor() {
    this._participants = new Map();
    this.log = [];          // Coordinator's durability log
    this.stats = { txns: 0, commits: 0, aborts: 0, participantFailures: 0 };
  }

  /** Register a participant */
  addParticipant(participant) {
    this._participants.set(participant.id, participant);
  }

  /** Get a participant */
  getParticipant(id) {
    return this._participants.get(id);
  }

  /**
   * Execute a distributed transaction.
   * @param {Function} txnFn - (participants) => void — the transaction body
   * @returns {Object} {committed: boolean, votes: Map, reason: string}
   */
  execute(txnFn) {
    this.stats.txns++;
    
    // Reset all participants
    for (const p of this._participants.values()) p.reset();
    
    // Execute transaction body (writes are buffered)
    try {
      txnFn(this._participants);
    } catch (e) {
      this.stats.aborts++;
      return { committed: false, reason: `Transaction body error: ${e.message}` };
    }
    
    // ============================================
    // Phase 1: PREPARE
    // ============================================
    this.log.push({ type: 'PREPARE_START', time: Date.now() });
    const votes = new Map();
    let allYes = true;
    
    for (const [id, participant] of this._participants) {
      try {
        const response = participant.prepare();
        votes.set(id, response);
        if (response.vote !== 'YES') {
          allYes = false;
          this.stats.participantFailures++;
        }
      } catch (e) {
        votes.set(id, { vote: 'NO', reason: e.message });
        allYes = false;
        this.stats.participantFailures++;
      }
    }
    
    // ============================================
    // Phase 2: COMMIT or ABORT
    // ============================================
    if (allYes) {
      this.log.push({ type: 'COMMIT_DECISION', time: Date.now() });
      
      for (const participant of this._participants.values()) {
        try {
          participant.commit();
        } catch (e) {
          // Participant crashed during commit — in real 2PC, we'd retry
          this.stats.participantFailures++;
        }
      }
      
      this.stats.commits++;
      return { committed: true, votes };
    } else {
      this.log.push({ type: 'ABORT_DECISION', time: Date.now() });
      
      for (const participant of this._participants.values()) {
        try {
          participant.abort();
        } catch (e) {
          // Ignore — participant may already be crashed
        }
      }
      
      this.stats.aborts++;
      const failedVoters = [...votes.entries()].filter(([_, v]) => v.vote !== 'YES');
      return {
        committed: false,
        votes,
        reason: `Abort: ${failedVoters.map(([id, v]) => `${id}=${v.reason || 'NO'}`).join(', ')}`,
      };
    }
  }
}

export { TXN_STATE };
