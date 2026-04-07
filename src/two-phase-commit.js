// two-phase-commit.js — Two-Phase Commit (2PC) protocol for distributed transactions
//
// The classic distributed transaction coordination protocol.
// Ensures all-or-nothing atomicity across multiple database nodes.
//
// Phase 1 (PREPARE): Coordinator asks all participants if they can commit.
//   - Each participant either votes YES (prepared) or NO (abort)
//   - Prepared state is durable — survives participant crash
//
// Phase 2 (COMMIT/ABORT): Coordinator decides based on votes.
//   - All YES → COMMIT (coordinator logs decision, tells all to commit)
//   - Any NO → ABORT (coordinator tells all to abort)
//   - Decision is durable — survives coordinator crash
//
// Recovery: After coordinator crash, participants in PREPARED state
// must wait for the coordinator to recover and re-send the decision.
// This is the fundamental weakness of 2PC (blocking protocol).

/**
 * Transaction states in 2PC
 */
export const TxState = {
  INITIAL: 'initial',     // Transaction started, work in progress
  PREPARING: 'preparing', // Coordinator sent PREPARE
  PREPARED: 'prepared',   // Participant voted YES
  COMMITTING: 'committing', // Coordinator decided COMMIT
  COMMITTED: 'committed', // Final state: committed
  ABORTING: 'aborting',   // Coordinator decided ABORT (or participant voted NO)
  ABORTED: 'aborted'      // Final state: aborted
};

/**
 * Two-Phase Commit Coordinator
 * 
 * Coordinates a distributed transaction across multiple participants.
 * The coordinator is the single point of decision — if it crashes,
 * prepared participants must wait for recovery.
 */
export class TwoPhaseCoordinator {
  constructor(txId, participants, { log } = {}) {
    this.txId = txId;
    this.participants = participants; // Array of Participant instances
    this.state = TxState.INITIAL;
    this.votes = new Map();           // participantId → 'yes'|'no'
    this.log = log || new InMemoryLog(); // WAL for coordinator decisions
    this.decision = null;             // 'commit' or 'abort'
    this.startTime = Date.now();
    this.timeoutMs = 5000;            // Timeout for participant responses
  }

  /**
   * Execute the full 2PC protocol.
   * Returns { decision: 'commit'|'abort', txId }
   */
  async execute() {
    try {
      // Phase 1: PREPARE
      const allPrepared = await this.prepare();
      
      if (allPrepared) {
        // Phase 2: COMMIT
        await this.commit();
        return { decision: 'commit', txId: this.txId };
      } else {
        // Phase 2: ABORT
        await this.abort();
        return { decision: 'abort', txId: this.txId };
      }
    } catch (e) {
      // Any error → abort
      await this.abort();
      return { decision: 'abort', txId: this.txId, error: e.message };
    }
  }

  /**
   * Phase 1: Send PREPARE to all participants, collect votes.
   * Returns true if all voted YES.
   */
  async prepare() {
    this.state = TxState.PREPARING;
    this.log.append({ txId: this.txId, type: 'prepare-start', participants: this.participants.map(p => p.id) });
    
    const votePromises = this.participants.map(async (participant) => {
      try {
        const vote = await Promise.race([
          participant.prepare(this.txId),
          this._timeout(this.timeoutMs)
        ]);
        this.votes.set(participant.id, vote);
        return vote;
      } catch (e) {
        // Timeout or error → treat as NO
        this.votes.set(participant.id, 'no');
        return 'no';
      }
    });
    
    const votes = await Promise.all(votePromises);
    const allYes = votes.every(v => v === 'yes');
    
    // Log the decision BEFORE sending phase 2 messages
    // This is the critical WAL write — the decision must be durable
    this.decision = allYes ? 'commit' : 'abort';
    this.log.append({ txId: this.txId, type: 'decision', decision: this.decision, votes: Object.fromEntries(this.votes) });
    
    return allYes;
  }

  /**
   * Phase 2: COMMIT — tell all participants to commit.
   */
  async commit() {
    this.state = TxState.COMMITTING;
    
    const commitPromises = this.participants.map(async (participant) => {
      try {
        await participant.commit(this.txId);
      } catch (e) {
        // Participant will need to recover later
        this.log.append({ txId: this.txId, type: 'commit-retry-needed', participantId: participant.id });
      }
    });
    
    await Promise.all(commitPromises);
    this.state = TxState.COMMITTED;
    this.log.append({ txId: this.txId, type: 'committed' });
  }

  /**
   * Phase 2: ABORT — tell all participants to abort.
   */
  async abort() {
    this.state = TxState.ABORTING;
    
    const abortPromises = this.participants.map(async (participant) => {
      try {
        await participant.abort(this.txId);
      } catch (e) {
        // Best effort — participant will timeout and abort on its own
      }
    });
    
    await Promise.all(abortPromises);
    this.state = TxState.ABORTED;
    this.log.append({ txId: this.txId, type: 'aborted' });
  }

  _timeout(ms) {
    return new Promise((_, reject) => setTimeout(() => reject(new Error('timeout')), ms));
  }

  /**
   * Recover from coordinator crash.
   * Reads the WAL to find the last decision and re-sends it.
   */
  static async recover(txId, log, participants) {
    const entries = log.entriesFor(txId);
    const decisionEntry = entries.find(e => e.type === 'decision');
    
    if (!decisionEntry) {
      // No decision was made — safe to abort
      const coordinator = new TwoPhaseCoordinator(txId, participants, { log });
      await coordinator.abort();
      return { decision: 'abort', recovered: true };
    }
    
    if (decisionEntry.decision === 'commit') {
      // Re-send commit to all participants
      const commitPromises = participants.map(p => p.commit(txId).catch(() => {}));
      await Promise.all(commitPromises);
      log.append({ txId, type: 'committed' });
      return { decision: 'commit', recovered: true };
    } else {
      // Re-send abort
      const abortPromises = participants.map(p => p.abort(txId).catch(() => {}));
      await Promise.all(abortPromises);
      log.append({ txId, type: 'aborted' });
      return { decision: 'abort', recovered: true };
    }
  }
}

/**
 * Two-Phase Commit Participant
 * 
 * Represents a database node participating in a distributed transaction.
 * Participants vote YES/NO during prepare, then follow the coordinator's decision.
 */
export class TwoPhaseParticipant {
  constructor(id, { canPrepare = true, prepareDelay = 0, commitDelay = 0, failOnPrepare = false, failOnCommit = false } = {}) {
    this.id = id;
    this.state = TxState.INITIAL;
    this.transactions = new Map(); // txId → { state, data }
    this._canPrepare = canPrepare;
    this._prepareDelay = prepareDelay;
    this._commitDelay = commitDelay;
    this._failOnPrepare = failOnPrepare;
    this._failOnCommit = failOnCommit;
    this.log = new InMemoryLog();
  }

  /**
   * Phase 1: Receive PREPARE request from coordinator.
   * Returns 'yes' if the participant can commit, 'no' otherwise.
   */
  async prepare(txId) {
    if (this._prepareDelay > 0) {
      await new Promise(r => setTimeout(r, this._prepareDelay));
    }
    
    if (this._failOnPrepare) {
      throw new Error(`Participant ${this.id} failed during prepare`);
    }
    
    if (!this._canPrepare) {
      this.state = TxState.ABORTED;
      this.log.append({ txId, type: 'vote-no', participantId: this.id });
      return 'no';
    }
    
    // Prepare: make the transaction durable (but not committed)
    // In a real implementation, this would flush WAL records
    this.state = TxState.PREPARED;
    this.log.append({ txId, type: 'vote-yes', participantId: this.id });
    this.transactions.set(txId, { state: 'prepared', preparedAt: Date.now() });
    return 'yes';
  }

  /**
   * Phase 2: Receive COMMIT from coordinator.
   */
  async commit(txId) {
    if (this._commitDelay > 0) {
      await new Promise(r => setTimeout(r, this._commitDelay));
    }
    
    if (this._failOnCommit) {
      throw new Error(`Participant ${this.id} failed during commit`);
    }
    
    this.state = TxState.COMMITTED;
    this.log.append({ txId, type: 'committed', participantId: this.id });
    const txData = this.transactions.get(txId);
    if (txData) txData.state = 'committed';
    return true;
  }

  /**
   * Phase 2: Receive ABORT from coordinator.
   */
  async abort(txId) {
    this.state = TxState.ABORTED;
    this.log.append({ txId, type: 'aborted', participantId: this.id });
    const txData = this.transactions.get(txId);
    if (txData) txData.state = 'aborted';
    return true;
  }
}

/**
 * Simple in-memory WAL for 2PC coordinator/participant logs.
 */
export class InMemoryLog {
  constructor() {
    this.entries = [];
  }

  append(entry) {
    this.entries.push({ ...entry, timestamp: Date.now() });
  }

  entriesFor(txId) {
    return this.entries.filter(e => e.txId === txId);
  }

  lastEntry() {
    return this.entries[this.entries.length - 1] || null;
  }
}
