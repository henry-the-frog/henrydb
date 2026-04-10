// consistent-hash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ConsistentHashRing } from './consistent-hash.js';

describe('ConsistentHashRing — Basic', () => {
  it('routes keys to nodes', () => {
    const ring = new ConsistentHashRing();
    ring.addNode('node-A');
    ring.addNode('node-B');
    ring.addNode('node-C');
    
    const node = ring.getNode('mykey');
    assert.ok(['node-A', 'node-B', 'node-C'].includes(node));
  });

  it('same key always maps to same node', () => {
    const ring = new ConsistentHashRing();
    ring.addNode('n1');
    ring.addNode('n2');
    
    const a = ring.getNode('consistent-key');
    const b = ring.getNode('consistent-key');
    assert.equal(a, b);
  });

  it('getNodes returns N unique nodes for replication', () => {
    const ring = new ConsistentHashRing();
    for (let i = 0; i < 5; i++) ring.addNode(`node-${i}`);
    
    const nodes = ring.getNodes('replication-key', 3);
    assert.equal(nodes.length, 3);
    assert.equal(new Set(nodes).size, 3); // All unique
  });
});

describe('ConsistentHashRing — Distribution', () => {
  it('distributes keys roughly evenly', () => {
    const ring = new ConsistentHashRing(150);
    for (let i = 0; i < 5; i++) ring.addNode(`node-${i}`);
    
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    const dist = ring.getDistribution(keys);
    
    console.log('    Distribution:');
    for (const [node, count] of dist) {
      console.log(`      ${node}: ${count} keys (${(count / 100).toFixed(1)}%)`);
    }
    
    const counts = [...dist.values()];
    const min = Math.min(...counts);
    const max = Math.max(...counts);
    const ratio = max / min;
    
    console.log(`    Max/min ratio: ${ratio.toFixed(2)}`);
    assert.ok(ratio < 2.0, `Distribution too skewed: ${ratio}`);
  });

  it('more vnodes = better distribution', () => {
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    
    const ring50 = new ConsistentHashRing(50);
    const ring500 = new ConsistentHashRing(500);
    for (let i = 0; i < 5; i++) {
      ring50.addNode(`node-${i}`);
      ring500.addNode(`node-${i}`);
    }
    
    const stddev50 = ring50.distributionStdDev(keys);
    const stddev500 = ring500.distributionStdDev(keys);
    
    console.log(`    50 vnodes stddev: ${stddev50.toFixed(1)}`);
    console.log(`    500 vnodes stddev: ${stddev500.toFixed(1)}`);
    assert.ok(stddev500 < stddev50, 'More vnodes should give better distribution');
  });
});

describe('ConsistentHashRing — Node Changes', () => {
  it('adding a node moves ~1/N of keys', () => {
    const ring = new ConsistentHashRing(150);
    for (let i = 0; i < 4; i++) ring.addNode(`node-${i}`);
    
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    const result = ring.simulateAddNode('node-4', keys);
    
    const expectedFraction = 1 / 5; // Adding 5th node should move ~1/5
    console.log(`    Moved: ${result.moved}/${result.total} (${(result.fraction * 100).toFixed(1)}%, expected ~${(expectedFraction * 100).toFixed(0)}%)`);
    
    // Should be within 2x of expected
    assert.ok(result.fraction < expectedFraction * 2, `Moved too many keys: ${result.fraction}`);
    assert.ok(result.fraction > expectedFraction * 0.5, `Moved too few keys: ${result.fraction}`);
  });

  it('removing a node only affects that node\u0027s keys', () => {
    const ring = new ConsistentHashRing(150);
    for (let i = 0; i < 5; i++) ring.addNode(`node-${i}`);
    
    const keys = Array.from({ length: 10000 }, (_, i) => `key-${i}`);
    const before = {};
    for (const key of keys) before[key] = ring.getNode(key);
    
    ring.removeNode('node-2');
    
    let moved = 0;
    for (const key of keys) {
      if (ring.getNode(key) !== before[key]) moved++;
    }
    
    const dist = ring.getDistribution(keys);
    assert.ok(!dist.has('node-2'), 'Removed node should have no keys');
    
    console.log(`    After remove: ${moved}/${keys.length} keys moved (${(moved / keys.length * 100).toFixed(1)}%)`);
    assert.ok(moved < keys.length * 0.4, 'Should move less than 40% of keys');
  });
});

describe('ConsistentHashRing — Performance', () => {
  it('benchmark: 100K key lookups', () => {
    const ring = new ConsistentHashRing(150);
    for (let i = 0; i < 10; i++) ring.addNode(`node-${i}`);
    
    const N = 100_000;
    const t0 = performance.now();
    for (let i = 0; i < N; i++) ring.getNode(`key-${i}`);
    const elapsed = performance.now() - t0;
    
    console.log(`    ${N} lookups: ${elapsed.toFixed(1)}ms (${(N / elapsed * 1000) | 0}/sec)`);
    console.log(`    Ring size: ${ring.ringSize} vnodes`);
  });
});
