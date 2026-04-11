// consistent-hash.test.js — Tests for consistent hashing ring
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConsistentHashRing } from './consistent-hash.js';

describe('ConsistentHashRing', () => {
  it('basic: add nodes and lookup keys', () => {
    const ring = new ConsistentHashRing(10);
    ring.addNode('node-A');
    ring.addNode('node-B');
    ring.addNode('node-C');
    
    assert.equal(ring.nodeCount, 3);
    assert.equal(ring.ringSize, 30); // 3 × 10 vnodes
    
    const node = ring.getNode('my-key');
    assert.ok(['node-A', 'node-B', 'node-C'].includes(node));
  });

  it('deterministic: same key always maps to same node', () => {
    const ring = new ConsistentHashRing(50);
    ring.addNode('a');
    ring.addNode('b');
    ring.addNode('c');
    
    const node1 = ring.getNode('test-key');
    const node2 = ring.getNode('test-key');
    assert.equal(node1, node2);
  });

  it('empty ring returns null', () => {
    const ring = new ConsistentHashRing();
    assert.equal(ring.getNode('key'), null);
  });

  it('single node handles all keys', () => {
    const ring = new ConsistentHashRing(10);
    ring.addNode('only-node');
    
    for (let i = 0; i < 100; i++) {
      assert.equal(ring.getNode(`key-${i}`), 'only-node');
    }
  });

  it('adding a node minimizes key redistribution', () => {
    const ring = new ConsistentHashRing(150);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    const before = new Map();
    for (const k of keys) before.set(k, ring.getNode(k));
    
    // Add a 4th node
    ring.addNode('D');
    
    let moved = 0;
    for (const k of keys) {
      if (ring.getNode(k) !== before.get(k)) moved++;
    }
    
    const pctMoved = (moved / keys.length) * 100;
    // With 4 nodes, ~25% should move (1/N)
    assert.ok(pctMoved < 35, `Too many keys moved: ${pctMoved.toFixed(1)}%`);
    assert.ok(pctMoved > 15, `Too few keys moved: ${pctMoved.toFixed(1)}% (suspicious)`);
    console.log(`    Adding 4th node: ${pctMoved.toFixed(1)}% keys moved (ideal: 25%)`);
  });

  it('removing a node redistributes only its keys', () => {
    const ring = new ConsistentHashRing(150);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    ring.addNode('D');
    
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    const before = new Map();
    for (const k of keys) before.set(k, ring.getNode(k));
    
    ring.removeNode('D');
    
    let moved = 0;
    for (const k of keys) {
      if (ring.getNode(k) !== before.get(k)) moved++;
    }
    
    const pctMoved = (moved / keys.length) * 100;
    // Only D's keys should move
    assert.ok(pctMoved < 35, `Too many keys moved: ${pctMoved.toFixed(1)}%`);
    console.log(`    Removing node: ${pctMoved.toFixed(1)}% keys moved`);
  });

  it('load balance with virtual nodes', () => {
    const ring = new ConsistentHashRing(150);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    ring.addNode('D');
    ring.addNode('E');
    
    const keys = Array.from({ length: 50000 }, (_, i) => `key-${i}`);
    const dist = ring.getDistribution(keys);
    
    const expected = 50000 / 5; // 10000 per node
    for (const [node, count] of dist) {
      const deviation = Math.abs(count - expected) / expected * 100;
      assert.ok(deviation < 20, `Node ${node}: ${count} keys, ${deviation.toFixed(1)}% off balance`);
    }
    
    console.log('    Distribution:', [...dist.entries()].map(([n, c]) => `${n}:${c}`).join(', '));
  });

  it('replication: getNodes returns distinct physical nodes', () => {
    const ring = new ConsistentHashRing(50);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    
    const replicas = ring.getNodes('my-key', 3);
    assert.equal(replicas.length, 3);
    assert.equal(new Set(replicas).size, 3); // all distinct
    assert.ok(replicas.every(n => ['A', 'B', 'C'].includes(n)));
  });

  it('replication: requesting more replicas than nodes', () => {
    const ring = new ConsistentHashRing(50);
    ring.addNode('A');
    ring.addNode('B');
    
    const replicas = ring.getNodes('key', 5);
    assert.equal(replicas.length, 2); // capped at node count
  });

  it('simulateAddNode predicts redistribution', () => {
    const ring = new ConsistentHashRing(150);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    const result = ring.simulateAddNode('D', keys);
    
    assert.equal(result.total, 10000);
    assert.ok(result.moved > 1000 && result.moved < 4000);
    console.log(`    Simulated: ${result.percent}% would move`);
  });

  it('duplicate addNode is idempotent', () => {
    const ring = new ConsistentHashRing(10);
    ring.addNode('A');
    ring.addNode('A'); // duplicate
    
    assert.equal(ring.nodeCount, 1);
    assert.equal(ring.ringSize, 10);
  });
});
