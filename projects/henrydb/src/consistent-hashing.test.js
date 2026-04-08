// consistent-hashing.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConsistentHashRing } from './consistent-hashing.js';

describe('ConsistentHashRing', () => {
  it('basic key routing', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('node1');
    ring.addNode('node2');
    ring.addNode('node3');
    
    const node = ring.getNode('mykey');
    assert.ok(['node1', 'node2', 'node3'].includes(node));
  });

  it('deterministic', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('A'); ring.addNode('B');
    assert.equal(ring.getNode('key1'), ring.getNode('key1'));
  });

  it('minimal disruption on add', () => {
    const ring = new ConsistentHashRing(100);
    ring.addNode('A'); ring.addNode('B');
    
    const before = {};
    for (let i = 0; i < 1000; i++) before[i] = ring.getNode(`key_${i}`);
    
    ring.addNode('C');
    let moved = 0;
    for (let i = 0; i < 1000; i++) {
      if (ring.getNode(`key_${i}`) !== before[i]) moved++;
    }
    // Ideally ~1/3 keys move to new node
    console.log(`    Added node: ${moved}/1000 keys moved (ideal: ~333)`);
    assert.ok(moved < 700);
  });

  it('minimal disruption on remove', () => {
    const ring = new ConsistentHashRing(150);
    
    const before = {};
    for (let i = 0; i < 1000; i++) before[i] = ring.getNode(`key_${i}`);
    
    ring.removeNode('B');
    let moved = 0;
    for (let i = 0; i < 1000; i++) {
      if (ring.getNode(`key_${i}`) !== before[i]) moved++;
    }
    console.log(`    Removed node: ${moved}/1000 keys moved (ideal: ~333)`);
    assert.ok(moved < 800);
  });

  it('replication: getNodes returns N unique nodes', () => {
    const ring = new ConsistentHashRing(50);
    ring.addNode('A'); ring.addNode('B'); ring.addNode('C');
    
    const nodes = ring.getNodes('mykey', 2);
    assert.equal(nodes.length, 2);
    assert.notEqual(nodes[0], nodes[1]);
  });

  it('balanced distribution', () => {
    const ring = new ConsistentHashRing(150);
    ring.addNode('A'); ring.addNode('B'); ring.addNode('C');
    
    const counts = { A: 0, B: 0, C: 0 };
    for (let i = 0; i < 10000; i++) counts[ring.getNode(`key_${i}`)]++;
    
    console.log(`    Distribution: A=${counts.A}, B=${counts.B}, C=${counts.C}`);
    // Each should get roughly 33% ± 15%
    for (const node of ['A', 'B', 'C']) {
      assert.ok(counts[node] > 1000, `${node} too low: ${counts[node]}`);
      assert.ok(counts[node] < 6000, `${node} too high: ${counts[node]}`);
    }
  });

  it('empty ring returns null', () => {
    const ring = new ConsistentHashRing();
    assert.equal(ring.getNode('key'), null);
  });
});
