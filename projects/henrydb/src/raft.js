// raft.js — Raft consensus algorithm for distributed HenryDB
// Simplified implementation focusing on the core protocol:
// 1. Leader election with randomized timeouts
// 2. Log replication with AppendEntries
// 3. Commit protocol with majority agreement
//
// This is an in-memory simulation — no actual networking.

const State = { FOLLOWER: 'follower', CANDIDATE: 'candidate', LEADER: 'leader' };

/**
 * Log entry in the Raft replicated log.
 */
class LogEntry {
  constructor(term, command) {
    this.term = term;
    this.command = command; // The SQL command or operation
  }
}

/**
 * Raft node — a single server in the consensus cluster.
 */
export class RaftNode {
  constructor(id, cluster) {
    this.id = id;
    this.cluster = cluster; // Reference to the cluster for "sending" messages
    
    // Persistent state
    this.currentTerm = 0;
    this.votedFor = null;
    this.log = []; // Array of LogEntry
    
    // Volatile state
    this.commitIndex = -1;
    this.lastApplied = -1;
    this.state = State.FOLLOWER;
    
    // Leader-only volatile state
    this.nextIndex = {};  // For each node: index of next entry to send
    this.matchIndex = {}; // For each node: highest entry known replicated
    
    // Applied commands (the "state machine")
    this.appliedCommands = [];
  }

  /**
   * Start an election (called when election timeout fires).
   */
  startElection() {
    this.state = State.CANDIDATE;
    this.currentTerm++;
    this.votedFor = this.id;
    
    let votes = 1; // Vote for self
    const lastLogIndex = this.log.length - 1;
    const lastLogTerm = lastLogIndex >= 0 ? this.log[lastLogIndex].term : 0;
    
    // Request votes from all other nodes
    const others = this.cluster.getOtherNodes(this.id);
    for (const node of others) {
      const granted = node.handleRequestVote(this.currentTerm, this.id, lastLogIndex, lastLogTerm);
      if (granted) votes++;
    }
    
    // Check if we won majority
    const majority = Math.floor(this.cluster.size / 2) + 1;
    if (votes >= majority) {
      this._becomeLeader();
      return true;
    }
    
    // Election failed, revert to follower
    this.state = State.FOLLOWER;
    return false;
  }

  /**
   * Handle a RequestVote RPC.
   * Returns true if vote is granted.
   */
  handleRequestVote(term, candidateId, lastLogIndex, lastLogTerm) {
    // Rule 1: Reply false if term < currentTerm
    if (term < this.currentTerm) return false;
    
    // Update term if we see a newer one
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.state = State.FOLLOWER;
    }
    
    // Rule 2: Vote if not already voted for someone else AND candidate's log is up-to-date
    if (this.votedFor === null || this.votedFor === candidateId) {
      const myLastIndex = this.log.length - 1;
      const myLastTerm = myLastIndex >= 0 ? this.log[myLastIndex].term : 0;
      
      // Candidate's log must be at least as up-to-date as ours
      if (lastLogTerm > myLastTerm || (lastLogTerm === myLastTerm && lastLogIndex >= myLastIndex)) {
        this.votedFor = candidateId;
        return true;
      }
    }
    
    return false;
  }

  /**
   * Append a new command (leader only).
   */
  appendCommand(command) {
    if (this.state !== State.LEADER) {
      throw new Error(`Node ${this.id} is not the leader`);
    }
    
    // Append to our log
    this.log.push(new LogEntry(this.currentTerm, command));
    
    // Replicate to followers
    this._replicateToFollowers();
    
    // Check if we can advance commit index
    this._advanceCommitIndex();
    
    // Apply committed entries
    this._applyCommitted();
    
    // Replicate again with updated commitIndex so followers can advance
    this._replicateToFollowers();
    
    return this.log.length - 1; // Return log index
  }

  /**
   * Handle AppendEntries RPC (from leader).
   */
  handleAppendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit) {
    // Rule 1: Reply false if term < currentTerm
    if (term < this.currentTerm) return { success: false, term: this.currentTerm };
    
    // Update term
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
    }
    this.state = State.FOLLOWER;
    
    // Rule 2: Reply false if log doesn't contain entry at prevLogIndex with prevLogTerm
    if (prevLogIndex >= 0) {
      if (prevLogIndex >= this.log.length) return { success: false, term: this.currentTerm };
      if (this.log[prevLogIndex].term !== prevLogTerm) return { success: false, term: this.currentTerm };
    }
    
    // Rule 3: Append new entries (remove conflicting ones first)
    for (let i = 0; i < entries.length; i++) {
      const logIdx = prevLogIndex + 1 + i;
      if (logIdx < this.log.length) {
        if (this.log[logIdx].term !== entries[i].term) {
          // Conflict: truncate from here
          this.log.length = logIdx;
        }
      }
      if (logIdx >= this.log.length) {
        this.log.push(entries[i]);
      }
    }
    
    // Rule 5: Update commit index
    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(leaderCommit, this.log.length - 1);
      this._applyCommitted();
    }
    
    return { success: true, term: this.currentTerm };
  }

  _becomeLeader() {
    this.state = State.LEADER;
    // Initialize leader state
    const others = this.cluster.getOtherNodes(this.id);
    for (const node of others) {
      this.nextIndex[node.id] = this.log.length;
      this.matchIndex[node.id] = -1;
    }
  }

  _replicateToFollowers() {
    const others = this.cluster.getOtherNodes(this.id);
    for (const node of others) {
      const nextIdx = this.nextIndex[node.id] || 0;
      const prevLogIndex = nextIdx - 1;
      const prevLogTerm = prevLogIndex >= 0 ? this.log[prevLogIndex].term : 0;
      const entries = this.log.slice(nextIdx);
      
      const result = node.handleAppendEntries(
        this.currentTerm, this.id,
        prevLogIndex, prevLogTerm,
        entries, this.commitIndex
      );
      
      if (result.success) {
        this.nextIndex[node.id] = this.log.length;
        this.matchIndex[node.id] = this.log.length - 1;
      } else {
        // Decrement nextIndex and retry (simplified)
        this.nextIndex[node.id] = Math.max(0, (this.nextIndex[node.id] || 1) - 1);
      }
    }
  }

  _advanceCommitIndex() {
    // Find the highest N such that a majority has matchIndex >= N
    const majority = Math.floor(this.cluster.size / 2) + 1;
    
    for (let n = this.log.length - 1; n > this.commitIndex; n--) {
      if (this.log[n].term !== this.currentTerm) continue;
      
      let replicated = 1; // Count self
      for (const [, matchIdx] of Object.entries(this.matchIndex)) {
        if (matchIdx >= n) replicated++;
      }
      
      if (replicated >= majority) {
        this.commitIndex = n;
        break;
      }
    }
  }

  _applyCommitted() {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      this.appliedCommands.push(this.log[this.lastApplied].command);
    }
  }
}

/**
 * Raft cluster — manages a group of Raft nodes.
 */
export class RaftCluster {
  constructor(nodeCount = 3) {
    this.nodes = [];
    for (let i = 0; i < nodeCount; i++) {
      this.nodes.push(new RaftNode(i, this));
    }
  }

  get size() { return this.nodes.length; }

  getNode(id) { return this.nodes[id]; }

  getOtherNodes(id) {
    return this.nodes.filter(n => n.id !== id);
  }

  /**
   * Elect a leader (simulate election).
   */
  electLeader(nodeId = 0) {
    return this.nodes[nodeId].startElection();
  }

  /**
   * Get the current leader.
   */
  getLeader() {
    return this.nodes.find(n => n.state === State.LEADER) || null;
  }
}

export { State, LogEntry };
