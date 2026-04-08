// raft.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RaftNode } from './raft.js';

describe('Raft Consensus', () => {
  it('starts as follower', () => {
    const node = new RaftNode('n1', ['n2', 'n3']);
    assert.equal(node.state, 'follower');
    assert.equal(node.currentTerm, 0);
  });

  it('election: candidate requests votes', () => {
    const n1 = new RaftNode('n1', ['n2', 'n3']);
    const rv = n1.startElection();
    assert.equal(n1.state, 'candidate');
    assert.equal(rv.type, 'RequestVote');
    assert.equal(rv.term, 1);
  });

  it('follower grants vote', () => {
    const n2 = new RaftNode('n2', ['n1', 'n3']);
    const rv = { term: 1, candidateId: 'n1', lastLogIndex: -1, lastLogTerm: 0 };
    const response = n2.handleRequestVote(rv);
    assert.ok(response.voteGranted);
    assert.equal(n2.votedFor, 'n1');
  });

  it('wins election with majority', () => {
    const n1 = new RaftNode('n1', ['n2', 'n3']);
    n1.startElection(); // Votes for self
    // Receive vote from n2
    n1.handleVoteResponse({ voteGranted: true, from: 'n2', term: 1 });
    assert.equal(n1.state, 'leader'); // 2/3 = majority
  });

  it('leader appends entries', () => {
    const leader = new RaftNode('n1', ['n2']);
    leader.startElection();
    leader.handleVoteResponse({ voteGranted: true, from: 'n2', term: 1 });
    
    const result = leader.clientRequest('SET x 1');
    assert.ok(result.ok);
    assert.equal(leader.log.length, 1);
  });

  it('follower replicates log', () => {
    const leader = new RaftNode('n1', ['n2']);
    leader.state = 'leader';
    leader.currentTerm = 1;
    leader.nextIndex = { n2: 0 };
    leader.matchIndex = { n2: -1 };
    
    leader.clientRequest('SET x 1');
    leader.clientRequest('SET y 2');
    
    const follower = new RaftNode('n2', ['n1']);
    const ae = leader.createAppendEntries('n2');
    const response = follower.handleAppendEntries(ae);
    
    assert.ok(response.success);
    assert.equal(follower.log.length, 2);
    assert.equal(follower.log[0].command, 'SET x 1');
  });

  it('rejects vote for older term', () => {
    const n1 = new RaftNode('n1', ['n2']);
    n1.currentTerm = 5;
    const response = n1.handleRequestVote({ term: 3, candidateId: 'n2', lastLogIndex: -1, lastLogTerm: 0 });
    assert.ok(!response.voteGranted);
  });

  it('steps down on higher term', () => {
    const n1 = new RaftNode('n1', ['n2']);
    n1.state = 'leader';
    n1.currentTerm = 1;
    
    n1.handleAppendEntries({ term: 2, leaderId: 'n2', prevLogIndex: -1, prevLogTerm: 0, entries: [], leaderCommit: -1 });
    assert.equal(n1.state, 'follower');
    assert.equal(n1.currentTerm, 2);
  });

  it('non-leader rejects client requests', () => {
    const follower = new RaftNode('n1', ['n2']);
    assert.ok(!follower.clientRequest('SET x 1').ok);
  });
});
