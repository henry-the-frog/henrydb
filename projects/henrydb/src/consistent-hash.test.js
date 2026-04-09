// consistent-hash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConsistentHash } from './consistent-hash.js';

describe('ConsistentHash', () => {
  it('basic: keys map to nodes', () => {
    const ch = new ConsistentHash(100);
    ch.addNode('node-A');
    ch.addNode('node-B');
    ch.addNode('node-C');
    
    const node = ch.getNode('user:123');
    assert.ok(['node-A', 'node-B', 'node-C'].includes(node));
  });

  it('deterministic: same key always maps to same node', () => {
    const ch = new ConsistentHash(100);
    ch.addNode('n1'); ch.addNode('n2');
    
    const first = ch.getNode('key1');
    const second = ch.getNode('key1');
    assert.equal(first, second);
  });

  it('minimal disruption: adding node moves ~1/N keys', () => {
    const ch = new ConsistentHash(150);
    ch.addNode('A'); ch.addNode('B'); ch.addNode('C');
    
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    const before = keys.map(k => ch.getNode(k));
    
    ch.addNode('D'); // Add 4th node
    const after = keys.map(k => ch.getNode(k));
    
    let moved = 0;
    for (let i = 0; i < keys.length; i++) {
      if (before[i] !== after[i]) moved++;
    }
    
    const movedPct = moved / keys.length;
    console.log(`  Adding 4th node: ${(movedPct*100).toFixed(1)}% keys moved (ideal: 25%)`);
    assert.ok(movedPct < 0.40, `Too many keys moved: ${(movedPct*100).toFixed(1)}%`);
    assert.ok(movedPct > 0.10, `Too few keys moved: ${(movedPct*100).toFixed(1)}%`);
  });

  it('balanced distribution', () => {
    const ch = new ConsistentHash(150);
    ch.addNode('A'); ch.addNode('B'); ch.addNode('C');
    
    const keys = Array.from({ length: 10000 }, (_, i) => `k${i}`);
    const dist = ch.getDistribution(keys);
    
    const values = Object.values(dist);
    const max = Math.max(...values);
    const min = Math.min(...values);
    const ratio = max / min;
    
    console.log(`  Distribution: ${JSON.stringify(dist)}, max/min ratio: ${ratio.toFixed(2)}`);
    assert.ok(ratio < 2.0, `Imbalanced: ${ratio.toFixed(2)}`);
  });

  it('replica nodes', () => {
    const ch = new ConsistentHash(100);
    ch.addNode('A'); ch.addNode('B'); ch.addNode('C');
    
    const replicas = ch.getNodes('important-key', 2);
    assert.equal(replicas.length, 2);
    assert.notEqual(replicas[0], replicas[1]); // Different nodes
  });

  it('remove node: keys migrate to remaining', () => {
    const ch = new ConsistentHash(100);
    ch.addNode('A'); ch.addNode('B'); ch.addNode('C');
    
    const before = ch.getNode('test');
    ch.removeNode(before); // Remove the node that owns 'test'
    
    const after = ch.getNode('test');
    assert.notEqual(before, after);
    assert.ok(['A', 'B', 'C'].includes(after));
  });

  it('performance: 100K lookups', () => {
    const ch = new ConsistentHash(150);
    for (let i = 0; i < 10; i++) ch.addNode(`node-${i}`);
    
    const t0 = performance.now();
    for (let i = 0; i < 100000; i++) ch.getNode(`key-${i}`);
    const elapsed = performance.now() - t0;
    
    console.log(`  100K lookups on 10 nodes: ${elapsed.toFixed(1)}ms (${(elapsed/100000*1000).toFixed(3)}µs avg)`);
    assert.ok(elapsed < 500);
  });
});
