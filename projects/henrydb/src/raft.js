// raft.js — Raft Consensus Protocol
//
// Implements the core Raft protocol for distributed consensus:
// - Leader election with randomized timeouts
// - Log replication (AppendEntries)
// - Safety guarantees (election restriction, commit rules)
//
// This is a simulation — no real networking. Nodes communicate via message passing.
//
// Reference: Ongaro & Ousterhout, "In Search of an Understandable Consensus Algorithm" (2014)

const STATE = { FOLLOWER: 'follower', CANDIDATE: 'candidate', LEADER: 'leader' };

/**
 * Log entry.
 */
class LogEntry {
  constructor(term, command) {
    this.term = term;
    this.command = command;
  }
}

/**
 * Raft node — a single participant in the consensus cluster.
 */
export class RaftNode {
  constructor(id, cluster) {
    this.id = id;
    this.cluster = cluster; // Reference to the RaftCluster for message passing
    
    // Persistent state
    this.currentTerm = 0;
    this.votedFor = null;
    this.log = []; // Array of LogEntry (1-indexed conceptually, 0-indexed in array)
    
    // Volatile state
    this.state = STATE.FOLLOWER;
    this.commitIndex = -1;    // Highest committed log index
    this.lastApplied = -1;    // Highest applied log index
    this.appliedCommands = []; // Applied commands (state machine)
    
    // Leader-only state
    this.nextIndex = {};   // nodeId → next log index to send
    this.matchIndex = {};  // nodeId → highest replicated index
    
    // Election state
    this.votesReceived = new Set();
    this.electionTimeout = 0;
    this._resetElectionTimeout();
    
    // Stats
    this.stats = { elections: 0, votesGranted: 0, entriesReplicated: 0, heartbeats: 0 };
  }

  /**
   * Process a timer tick. Handles election timeouts and heartbeats.
   */
  tick() {
    this.electionTimeout--;
    
    if (this.state === STATE.LEADER) {
      // Send heartbeats
      this._sendHeartbeats();
    } else if (this.electionTimeout <= 0) {
      // Start election
      this._startElection();
    }
  }

  /**
   * Client request: propose a command to the cluster.
   * Only the leader can accept client requests.
   */
  propose(command) {
    if (this.state !== STATE.LEADER) return false;
    this.log.push(new LogEntry(this.currentTerm, command));
    // Replicate to followers
    this._replicateEntries();
    return true;
  }

  // ============================================================
  // RPC Handlers
  // ============================================================

  /**
   * Handle RequestVote RPC.
   */
  handleRequestVote(candidateId, term, lastLogIndex, lastLogTerm) {
    let voteGranted = false;
    
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.state = STATE.FOLLOWER;
      this.votedFor = null;
    }
    
    if (term >= this.currentTerm &&
        (this.votedFor === null || this.votedFor === candidateId) &&
        this._isLogUpToDate(lastLogIndex, lastLogTerm)) {
      voteGranted = true;
      this.votedFor = candidateId;
      this._resetElectionTimeout();
      this.stats.votesGranted++;
    }
    
    return { term: this.currentTerm, voteGranted };
  }

  /**
   * Handle AppendEntries RPC (heartbeat + log replication).
   */
  handleAppendEntries(leaderId, term, prevLogIndex, prevLogTerm, entries, leaderCommit) {
    if (term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }
    
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
    }
    
    this.state = STATE.FOLLOWER;
    this._resetElectionTimeout();
    
    // Check log consistency
    if (prevLogIndex >= 0) {
      if (prevLogIndex >= this.log.length) {
        return { term: this.currentTerm, success: false };
      }
      if (this.log[prevLogIndex].term !== prevLogTerm) {
        // Delete conflicting entries
        this.log.splice(prevLogIndex);
        return { term: this.currentTerm, success: false };
      }
    }
    
    // Append new entries
    for (let i = 0; i < entries.length; i++) {
      const logIdx = prevLogIndex + 1 + i;
      if (logIdx < this.log.length) {
        if (this.log[logIdx].term !== entries[i].term) {
          this.log.splice(logIdx);
          this.log.push(entries[i]);
        }
      } else {
        this.log.push(entries[i]);
      }
    }
    
    // Update commit index
    if (leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(leaderCommit, this.log.length - 1);
      this._applyCommitted();
    }
    
    return { term: this.currentTerm, success: true };
  }

  // ============================================================
  // Internal methods
  // ============================================================

  _startElection() {
    this.state = STATE.CANDIDATE;
    this.currentTerm++;
    this.votedFor = this.id;
    this.votesReceived = new Set([this.id]);
    this.stats.elections++;
    this._resetElectionTimeout();
    
    const lastLogIndex = this.log.length - 1;
    const lastLogTerm = lastLogIndex >= 0 ? this.log[lastLogIndex].term : 0;
    
    // Request votes from all other nodes
    for (const nodeId of this.cluster.getNodeIds()) {
      if (nodeId === this.id) continue;
      const response = this.cluster.sendRequestVote(
        nodeId, this.id, this.currentTerm, lastLogIndex, lastLogTerm
      );
      if (response && response.voteGranted) {
        this.votesReceived.add(nodeId);
      }
      if (response && response.term > this.currentTerm) {
        this.currentTerm = response.term;
        this.state = STATE.FOLLOWER;
        this.votedFor = null;
        return;
      }
    }
    
    // Check if we won
    if (this.votesReceived.size > this.cluster.getNodeIds().length / 2) {
      this._becomeLeader();
    }
  }

  _becomeLeader() {
    this.state = STATE.LEADER;
    // Initialize nextIndex and matchIndex for all followers
    for (const nodeId of this.cluster.getNodeIds()) {
      if (nodeId === this.id) continue;
      this.nextIndex[nodeId] = this.log.length;
      this.matchIndex[nodeId] = -1;
    }
  }

  _sendHeartbeats() {
    this.stats.heartbeats++;
    this._replicateEntries();
  }

  _replicateEntries() {
    for (const nodeId of this.cluster.getNodeIds()) {
      if (nodeId === this.id) continue;
      
      const nextIdx = this.nextIndex[nodeId] || 0;
      const prevLogIndex = nextIdx - 1;
      const prevLogTerm = prevLogIndex >= 0 && prevLogIndex < this.log.length
        ? this.log[prevLogIndex].term : 0;
      const entries = this.log.slice(nextIdx);
      
      const response = this.cluster.sendAppendEntries(
        nodeId, this.id, this.currentTerm,
        prevLogIndex, prevLogTerm, entries, this.commitIndex
      );
      
      if (response && response.success) {
        this.nextIndex[nodeId] = this.log.length;
        this.matchIndex[nodeId] = this.log.length - 1;
        this.stats.entriesReplicated += entries.length;
      } else if (response && !response.success) {
        // Decrement nextIndex and retry
        if (this.nextIndex[nodeId] > 0) this.nextIndex[nodeId]--;
      }
      
      if (response && response.term > this.currentTerm) {
        this.currentTerm = response.term;
        this.state = STATE.FOLLOWER;
        this.votedFor = null;
        return;
      }
    }
    
    // Update commit index: find N such that majority has matchIndex >= N
    this._updateCommitIndex();
  }

  _updateCommitIndex() {
    for (let n = this.log.length - 1; n > this.commitIndex; n--) {
      if (this.log[n].term !== this.currentTerm) continue;
      let count = 1; // Count self
      for (const nodeId of this.cluster.getNodeIds()) {
        if (nodeId === this.id) continue;
        if ((this.matchIndex[nodeId] || -1) >= n) count++;
      }
      if (count > this.cluster.getNodeIds().length / 2) {
        this.commitIndex = n;
        this._applyCommitted();
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

  _isLogUpToDate(lastLogIndex, lastLogTerm) {
    const myLastIndex = this.log.length - 1;
    const myLastTerm = myLastIndex >= 0 ? this.log[myLastIndex].term : 0;
    if (lastLogTerm !== myLastTerm) return lastLogTerm >= myLastTerm;
    return lastLogIndex >= myLastIndex;
  }

  _resetElectionTimeout() {
    this.electionTimeout = 5 + Math.floor(Math.random() * 10); // 5-14 ticks
  }
}

/**
 * RaftCluster — simulated cluster for synchronous message passing.
 */
export class RaftCluster {
  constructor(nodeCount) {
    this.nodes = new Map();
    this._partitioned = new Set(); // Set of node IDs that can't communicate
    
    for (let i = 0; i < nodeCount; i++) {
      const node = new RaftNode(i, this);
      this.nodes.set(i, node);
    }
  }

  getNodeIds() { return [...this.nodes.keys()]; }
  getNode(id) { return this.nodes.get(id); }
  getLeader() {
    let best = null;
    for (const node of this.nodes.values()) {
      if (node.state === STATE.LEADER) {
        if (!best || node.currentTerm > best.currentTerm) best = node;
      }
    }
    return best;
  }

  /** Partition a node (can't send or receive) */
  partition(nodeId) { this._partitioned.add(nodeId); }
  /** Heal partition */
  heal(nodeId) { this._partitioned.delete(nodeId); }

  /** Tick all nodes */
  tick() {
    for (const node of this.nodes.values()) {
      node.tick();
    }
  }

  /** Run multiple ticks until a leader is elected */
  electLeader(maxTicks = 100) {
    for (let i = 0; i < maxTicks; i++) {
      this.tick();
      const leader = this.getLeader();
      if (leader) return leader;
    }
    return null;
  }

  sendRequestVote(targetId, candidateId, term, lastLogIndex, lastLogTerm) {
    if (this._partitioned.has(targetId) || this._partitioned.has(candidateId)) return null;
    const target = this.nodes.get(targetId);
    if (!target) return null;
    return target.handleRequestVote(candidateId, term, lastLogIndex, lastLogTerm);
  }

  sendAppendEntries(targetId, leaderId, term, prevLogIndex, prevLogTerm, entries, leaderCommit) {
    if (this._partitioned.has(targetId) || this._partitioned.has(leaderId)) return null;
    const target = this.nodes.get(targetId);
    if (!target) return null;
    return target.handleAppendEntries(leaderId, term, prevLogIndex, prevLogTerm, entries, leaderCommit);
  }
}

export { STATE };
