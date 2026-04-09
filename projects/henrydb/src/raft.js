// raft.js — Raft consensus algorithm (simplified)
// Leader election + log replication. Each node is follower/candidate/leader.
// Leader sends heartbeats. Followers promote to candidate on timeout.
// Majority vote wins election. Log entries replicated through AppendEntries.

export class RaftNode {
  constructor(id, peers = []) {
    this.id = id;
    this.peers = peers;
    this.state = 'follower'; // follower | candidate | leader
    this.currentTerm = 0;
    this.votedFor = null;
    this.log = []; // [{term, command}]
    this.commitIndex = -1;
    this.lastApplied = -1;
    // Leader state
    this.nextIndex = {}; // peerId → next log index to send
    this.matchIndex = {}; // peerId → highest replicated index
    this.votesReceived = new Set();
    this.stats = { elections: 0, appendEntries: 0, commits: 0 };
  }

  /**
   * Start election (follower → candidate).
   */
  startElection() {
    this.state = 'candidate';
    this.currentTerm++;
    this.votedFor = this.id;
    this.votesReceived = new Set([this.id]);
    this.stats.elections++;

    return {
      type: 'RequestVote',
      term: this.currentTerm,
      candidateId: this.id,
      lastLogIndex: this.log.length - 1,
      lastLogTerm: this.log.length > 0 ? this.log[this.log.length - 1].term : 0,
    };
  }

  /**
   * Handle a RequestVote RPC.
   */
  handleRequestVote(request) {
    if (request.term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }

    if (request.term > this.currentTerm) {
      this.currentTerm = request.term;
      this.state = 'follower';
      this.votedFor = null;
    }

    const logOk = request.lastLogTerm > this._lastLogTerm() ||
      (request.lastLogTerm === this._lastLogTerm() && request.lastLogIndex >= this.log.length - 1);

    if ((this.votedFor === null || this.votedFor === request.candidateId) && logOk) {
      this.votedFor = request.candidateId;
      return { term: this.currentTerm, voteGranted: true };
    }

    return { term: this.currentTerm, voteGranted: false };
  }

  /**
   * Handle vote response (as candidate).
   */
  handleVoteResponse(response) {
    if (response.term > this.currentTerm) {
      this.currentTerm = response.term;
      this.state = 'follower';
      return;
    }

    if (this.state !== 'candidate') return;

    if (response.voteGranted) {
      this.votesReceived.add(response.from || `peer_${this.votesReceived.size}`);
    }

    // Check majority
    const majority = Math.floor((this.peers.length + 1) / 2) + 1;
    if (this.votesReceived.size >= majority) {
      this._becomeLeader();
    }
  }

  _becomeLeader() {
    this.state = 'leader';
    for (const peer of this.peers) {
      this.nextIndex[peer] = this.log.length;
      this.matchIndex[peer] = -1;
    }
  }

  /**
   * Client request: append command to log (leader only).
   */
  clientRequest(command) {
    if (this.state !== 'leader') return { ok: false, reason: 'not leader' };
    this.log.push({ term: this.currentTerm, command });
    return { ok: true, index: this.log.length - 1 };
  }

  /**
   * Create AppendEntries RPC for a peer.
   */
  createAppendEntries(peerId) {
    const nextIdx = this.nextIndex[peerId] || 0;
    const prevLogIndex = nextIdx - 1;
    const prevLogTerm = prevLogIndex >= 0 ? this.log[prevLogIndex].term : 0;

    return {
      type: 'AppendEntries',
      term: this.currentTerm,
      leaderId: this.id,
      prevLogIndex,
      prevLogTerm,
      entries: this.log.slice(nextIdx),
      leaderCommit: this.commitIndex,
    };
  }

  /**
   * Handle AppendEntries RPC (as follower).
   */
  handleAppendEntries(request) {
    if (request.term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    this.currentTerm = request.term;
    this.state = 'follower';

    // Check log consistency
    if (request.prevLogIndex >= 0) {
      if (request.prevLogIndex >= this.log.length || 
          this.log[request.prevLogIndex].term !== request.prevLogTerm) {
        return { term: this.currentTerm, success: false };
      }
    }

    // Append new entries
    for (let i = 0; i < request.entries.length; i++) {
      const idx = request.prevLogIndex + 1 + i;
      if (idx < this.log.length) {
        this.log[idx] = request.entries[i]; // Overwrite conflicting
      } else {
        this.log.push(request.entries[i]);
      }
    }

    if (request.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(request.leaderCommit, this.log.length - 1);
    }

    this.stats.appendEntries++;
    return { term: this.currentTerm, success: true, matchIndex: this.log.length - 1 };
  }

  /**
   * Handle AppendEntries response (as leader).
   */
  handleAppendResponse(peerId, response) {
    if (response.success) {
      this.nextIndex[peerId] = (response.matchIndex || 0) + 1;
      this.matchIndex[peerId] = response.matchIndex || 0;
      this._advanceCommitIndex();
    } else {
      this.nextIndex[peerId] = Math.max(0, (this.nextIndex[peerId] || 1) - 1);
    }
  }

  _advanceCommitIndex() {
    for (let n = this.log.length - 1; n > this.commitIndex; n--) {
      if (this.log[n].term !== this.currentTerm) continue;
      let count = 1; // Self
      for (const peer of this.peers) {
        if ((this.matchIndex[peer] || -1) >= n) count++;
      }
      if (count > (this.peers.length + 1) / 2) {
        this.commitIndex = n;
        this.stats.commits++;
        break;
      }
    }
  }

  _lastLogTerm() {
    return this.log.length > 0 ? this.log[this.log.length - 1].term : 0;
  }
}

export class RaftCluster {
  constructor(size) {
    this.nodes = Array.from({ length: size }, (_, i) => new RaftNode(i, size));
    // Connect nodes
    for (const node of this.nodes) {
      node.peers = this.nodes.filter(n => n.id !== node.id);
    }
  }
  getLeader() { return this.nodes.find(n => n.state === 'leader'); }
  electLeader() {
    // Simple election: first node becomes leader
    const candidate = this.nodes[0];
    candidate.state = 'leader';
    candidate.term++;
    return candidate;
  }
}
