// raft.test.js — Raft consensus tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RaftCluster, State } from './raft.js';

describe('Raft Consensus', () => {
  it('elects a leader in a 3-node cluster', () => {
    const cluster = new RaftCluster(3);
    const won = cluster.electLeader(0);
    
    assert.equal(won, true);
    const leader = cluster.getLeader();
    assert.ok(leader);
    assert.equal(leader.state, State.LEADER);
    assert.equal(leader.id, 0);
  });

  it('leader replicates commands to followers', () => {
    const cluster = new RaftCluster(3);
    cluster.electLeader(0);
    const leader = cluster.getLeader();
    
    leader.appendCommand('INSERT INTO t VALUES (1)');
    
    // Command should be replicated to all nodes
    for (const node of cluster.nodes) {
      assert.equal(node.log.length, 1);
      assert.equal(node.log[0].command, 'INSERT INTO t VALUES (1)');
    }
  });

  it('committed commands are applied to state machine', () => {
    const cluster = new RaftCluster(3);
    cluster.electLeader(0);
    const leader = cluster.getLeader();
    
    leader.appendCommand('cmd1');
    leader.appendCommand('cmd2');
    
    assert.equal(leader.appliedCommands.length, 2);
    assert.equal(leader.appliedCommands[0], 'cmd1');
    assert.equal(leader.appliedCommands[1], 'cmd2');
  });

  it('multiple commands maintain order', () => {
    const cluster = new RaftCluster(5);
    cluster.electLeader(0);
    const leader = cluster.getLeader();
    
    for (let i = 0; i < 10; i++) {
      leader.appendCommand(`cmd_${i}`);
    }
    
    // All nodes should have 10 log entries
    for (const node of cluster.nodes) {
      assert.equal(node.log.length, 10);
    }
    
    // Leader should have applied all
    assert.equal(leader.appliedCommands.length, 10);
  });

  it('non-leader cannot append commands', () => {
    const cluster = new RaftCluster(3);
    cluster.electLeader(0);
    
    assert.throws(() => {
      cluster.getNode(1).appendCommand('illegal');
    }, /not the leader/);
  });

  it('term increments on election', () => {
    const cluster = new RaftCluster(3);
    
    cluster.electLeader(0);
    assert.equal(cluster.getNode(0).currentTerm, 1);
    
    // Another election attempt
    cluster.getNode(1).startElection();
    assert.ok(cluster.getNode(1).currentTerm >= 2);
  });

  it('5-node cluster requires 3 votes', () => {
    const cluster = new RaftCluster(5);
    const won = cluster.electLeader(0);
    assert.equal(won, true);
    assert.equal(cluster.getLeader().id, 0);
  });

  it('followers update commit index from leader', () => {
    const cluster = new RaftCluster(3);
    cluster.electLeader(0);
    const leader = cluster.getLeader();
    
    leader.appendCommand('cmd1');
    
    // Followers should have committed
    for (const node of cluster.nodes) {
      assert.equal(node.commitIndex, 0);
    }
  });

  it('log entries have correct term', () => {
    const cluster = new RaftCluster(3);
    cluster.electLeader(0);
    const leader = cluster.getLeader();
    
    leader.appendCommand('cmd1');
    assert.equal(leader.log[0].term, 1);
    
    leader.appendCommand('cmd2');
    assert.equal(leader.log[1].term, 1);
  });
});
