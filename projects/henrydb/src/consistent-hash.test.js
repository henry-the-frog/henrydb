// consistent-hash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConsistentHashRing } from './consistent-hash.js';

describe('Consistent Hash Ring', () => {
  it('routes keys to nodes', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('node1');
    ring.addNode('node2');
    ring.addNode('node3');
    
    const node = ring.getNode('key1');
    assert.ok(['node1', 'node2', 'node3'].includes(node));
  });

  it('same key always routes to same node', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('node1');
    ring.addNode('node2');
    
    const node1 = ring.getNode('test_key');
    const node2 = ring.getNode('test_key');
    assert.equal(node1, node2);
  });

  it('distributes keys roughly evenly', () => {
    const ring = new ConsistentHashRing(150);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    
    const keys = [];
    for (let i = 0; i < 3000; i++) keys.push(`key_${i}`);
    
    const dist = ring.getDistribution(keys);
    // Each node should have roughly 1000 ± 300 keys (generous margin)
    for (const count of Object.values(dist)) {
      assert.ok(count > 500 && count < 1500, `Uneven distribution: ${JSON.stringify(dist)}`);
    }
  });

  it('adding node moves minimal keys', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('A');
    ring.addNode('B');
    
    const keys = [];
    for (let i = 0; i < 1000; i++) keys.push(`key_${i}`);
    
    // Record initial assignments
    const before = {};
    for (const key of keys) before[key] = ring.getNode(key);
    
    // Add new node
    ring.addNode('C');
    
    // Count how many keys moved
    let moved = 0;
    for (const key of keys) {
      if (ring.getNode(key) !== before[key]) moved++;
    }
    
    // Ideally ~1/3 of keys move (from 2 nodes to 3)
    // Allow generous margin
    assert.ok(moved < 600, `Too many keys moved: ${moved}/1000`);
    assert.ok(moved > 100, `Too few keys moved: ${moved}/1000`);
  });

  it('removing node reassigns keys to remaining', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    
    ring.removeNode('B');
    
    // All keys should now route to A or C
    for (let i = 0; i < 100; i++) {
      const node = ring.getNode(`key_${i}`);
      assert.ok(node === 'A' || node === 'C');
    }
  });

  it('getNodes returns multiple for replication', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('A');
    ring.addNode('B');
    ring.addNode('C');
    
    const nodes = ring.getNodes('key', 2);
    assert.equal(nodes.length, 2);
    assert.notEqual(nodes[0], nodes[1]); // Different nodes
  });

  it('empty ring returns null', () => {
    const ring = new ConsistentHashRing();
    assert.equal(ring.getNode('key'), null);
  });

  it('single node handles all keys', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('only');
    
    for (let i = 0; i < 100; i++) {
      assert.equal(ring.getNode(`key_${i}`), 'only');
    }
  });
});
