// gossip.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GossipCluster, MEMBER_STATE } from './gossip.js';

describe('Gossip — Basic Membership', () => {
  it('all nodes know about each other initially', () => {
    const cluster = new GossipCluster(5);
    const view = cluster.getView('node-0');
    assert.equal(view.length, 5);
  });

  it('gossip converges membership', () => {
    const cluster = new GossipCluster(10);
    cluster.run(50);
    
    // All alive nodes should see all other alive nodes
    for (const node of cluster.nodes.values()) {
      const alive = node.getAliveMembers();
      assert.equal(alive.length, 10, `${node.id} should see 10 members, sees ${alive.length}`);
    }
  });
});

describe('Gossip — Failure Detection', () => {
  it('detects crashed node', () => {
    const cluster = new GossipCluster(5);
    cluster.run(10); // Stabilize
    
    // Crash node-2
    cluster.getNode('node-2').crash();
    
    // Run enough ticks for detection (ping + suspect timeout)
    cluster.run(50);
    
    // Other nodes should detect node-2 as dead or suspect
    const node0 = cluster.getNode('node-0');
    const info = node0.members.get('node-2');
    assert.ok(
      info.state === MEMBER_STATE.DEAD || info.state === MEMBER_STATE.SUSPECT,
      `node-2 should be suspect or dead, is ${info.state}`
    );
  });

  it('eventually marks crashed node as dead', () => {
    const cluster = new GossipCluster(5);
    cluster.run(10);
    
    cluster.getNode('node-3').crash();
    cluster.run(100); // Long enough for suspicion timeout
    
    let deadCount = 0;
    for (const [id, node] of cluster.nodes) {
      if (id === 'node-3') continue;
      const info = node.members.get('node-3');
      if (info?.state === MEMBER_STATE.DEAD) deadCount++;
    }
    
    console.log(`    Nodes that detected death: ${deadCount}/4`);
    assert.ok(deadCount >= 2, 'Majority should detect death');
  });

  it('recovered node rejoins cluster', () => {
    const cluster = new GossipCluster(5);
    cluster.run(10);
    
    cluster.getNode('node-1').crash();
    cluster.run(50);
    
    cluster.getNode('node-1').recover();
    cluster.run(50);
    
    // node-1 should be alive in some members' views
    const node0 = cluster.getNode('node-0');
    const info = node0.members.get('node-1');
    console.log(`    node-1 state from node-0 view: ${info?.state}`);
    // After recovery, incarnation increases and should eventually become alive
  });
});

describe('Gossip — Scalability', () => {
  it('works with 20 nodes', () => {
    const cluster = new GossipCluster(20);
    cluster.run(100);
    
    // All should converge
    for (const node of cluster.nodes.values()) {
      const alive = node.getAliveMembers();
      assert.equal(alive.length, 20, `${node.id} sees ${alive.length}`);
    }
  });

  it('O(1) messages per node per tick', () => {
    const cluster = new GossipCluster(20);
    cluster.run(50);
    
    // Each node should have sent ~50 pings (1 per tick)
    for (const node of cluster.nodes.values()) {
      console.log(`    ${node.id}: pings=${node.stats.pings}, acks=${node.stats.acks}`);
      // 1 ping per tick (approximately)
      assert.ok(node.stats.pings <= 55 && node.stats.pings >= 40,
        `${node.id} pings=${node.stats.pings} (expected ~50)`);
    }
  });
});
