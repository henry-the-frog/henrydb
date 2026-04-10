// raft.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RaftCluster, RaftNode, STATE } from './raft.js';

describe('Raft — Leader Election', () => {
  it('elects a leader in a 3-node cluster', () => {
    const cluster = new RaftCluster(3);
    const leader = cluster.electLeader();
    assert.ok(leader, 'Should elect a leader');
    assert.equal(leader.state, STATE.LEADER);
    console.log(`    Leader: node ${leader.id}, term ${leader.currentTerm}`);
  });

  it('elects a leader in a 5-node cluster', () => {
    const cluster = new RaftCluster(5);
    const leader = cluster.electLeader();
    assert.ok(leader, 'Should elect a leader');
    
    // Only one leader
    let leaderCount = 0;
    for (const node of cluster.nodes.values()) {
      if (node.state === STATE.LEADER) leaderCount++;
    }
    assert.equal(leaderCount, 1, 'Should have exactly one leader');
  });

  it('all nodes agree on the term', () => {
    const cluster = new RaftCluster(5);
    cluster.electLeader();
    // Run a few more ticks to stabilize
    for (let i = 0; i < 20; i++) cluster.tick();
    
    const leader = cluster.getLeader();
    for (const node of cluster.nodes.values()) {
      assert.equal(node.currentTerm, leader.currentTerm, `Node ${node.id} should be in leader's term`);
    }
  });
});

describe('Raft — Log Replication', () => {
  it('replicates entries to followers', () => {
    const cluster = new RaftCluster(3);
    const leader = cluster.electLeader();
    
    // Propose a command
    const ok = leader.propose('SET x = 1');
    assert.ok(ok, 'Leader should accept proposals');
    
    // Tick to replicate
    for (let i = 0; i < 10; i++) cluster.tick();
    
    // All nodes should have the entry
    for (const node of cluster.nodes.values()) {
      assert.equal(node.log.length, 1, `Node ${node.id} should have 1 log entry`);
      assert.equal(node.log[0].command, 'SET x = 1');
    }
  });

  it('commits entries when majority acknowledges', () => {
    const cluster = new RaftCluster(3);
    const leader = cluster.electLeader();
    
    leader.propose('SET x = 1');
    leader.propose('SET y = 2');
    
    for (let i = 0; i < 20; i++) cluster.tick();
    
    // Leader should have committed
    assert.equal(leader.commitIndex, 1, 'Leader should commit both entries');
    assert.equal(leader.appliedCommands.length, 2);
    assert.equal(leader.appliedCommands[0], 'SET x = 1');
    assert.equal(leader.appliedCommands[1], 'SET y = 2');
  });

  it('followers apply committed entries', () => {
    const cluster = new RaftCluster(3);
    const leader = cluster.electLeader();
    
    leader.propose('cmd1');
    leader.propose('cmd2');
    leader.propose('cmd3');
    
    for (let i = 0; i < 30; i++) cluster.tick();
    
    for (const node of cluster.nodes.values()) {
      assert.equal(node.appliedCommands.length, 3, `Node ${node.id} should apply all 3 commands`);
    }
  });

  it('non-leaders reject proposals', () => {
    const cluster = new RaftCluster(3);
    cluster.electLeader();
    
    // Find a follower
    let follower;
    for (const node of cluster.nodes.values()) {
      if (node.state === STATE.FOLLOWER) { follower = node; break; }
    }
    
    const ok = follower.propose('should fail');
    assert.ok(!ok, 'Follower should reject proposals');
  });
});

describe('Raft — Network Partitions', () => {
  it('new leader elected when old leader is partitioned', () => {
    const cluster = new RaftCluster(5);
    const leader1 = cluster.electLeader();
    const leader1Id = leader1.id;
    
    // Partition the leader
    cluster.partition(leader1Id);
    
    // Run ticks until new leader elected
    for (let i = 0; i < 50; i++) cluster.tick();
    
    // Should have a new leader (different from partitioned one)
    const leader2 = cluster.getLeader();
    assert.ok(leader2, 'New leader should be elected');
    assert.notEqual(leader2.id, leader1Id, 'New leader should be different');
    
    // Heal partition
    cluster.heal(leader1Id);
    for (let i = 0; i < 30; i++) cluster.tick();
    
    // Old leader should step down (its term is lower)
    const oldLeader = cluster.getNode(leader1Id);
    assert.equal(oldLeader.state, STATE.FOLLOWER, 'Old leader should become follower');
  });

  it('committed entries survive partition', () => {
    const cluster = new RaftCluster(5);
    const leader = cluster.electLeader();
    
    // Commit some entries
    leader.propose('persistent1');
    leader.propose('persistent2');
    for (let i = 0; i < 20; i++) cluster.tick();
    
    // Partition leader
    cluster.partition(leader.id);
    for (let i = 0; i < 50; i++) cluster.tick();
    
    // New leader should have the committed entries
    const newLeader = cluster.getLeader();
    assert.ok(newLeader);
    assert.ok(newLeader.log.length >= 2, 'New leader should have committed entries');
  });
});

describe('Raft — Stress', () => {
  it('100 commands across leader changes', () => {
    const cluster = new RaftCluster(5);
    let proposed = 0;
    
    for (let round = 0; round < 10; round++) {
      // Elect leader
      cluster.electLeader(50);
      const leader = cluster.getLeader();
      if (!leader) continue;
      
      // Propose commands
      for (let i = 0; i < 10; i++) {
        if (leader.propose(`cmd-${proposed}`)) proposed++;
      }
      
      // Replicate
      for (let i = 0; i < 20; i++) cluster.tick();
      
      // Partition leader every other round
      if (round % 3 === 0) cluster.partition(leader.id);
    }
    
    // Heal all
    for (const id of cluster.getNodeIds()) cluster.heal(id);
    cluster.electLeader(100);
    for (let i = 0; i < 50; i++) cluster.tick();
    
    console.log(`    Proposed: ${proposed} commands`);
    const leader = cluster.getLeader();
    if (leader) {
      console.log(`    Leader applied: ${leader.appliedCommands.length}`);
      console.log(`    Leader term: ${leader.currentTerm}`);
    }
    
    assert.ok(proposed > 50, 'Should propose at least 50 commands');
  });
});
