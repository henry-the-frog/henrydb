// robin-hood-hash.test.js — Tests for Robin Hood hash table
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RobinHoodHashMap } from './robin-hood-hash.js';

describe('RobinHoodHashMap', () => {
  it('basic set and get', () => {
    const m = new RobinHoodHashMap();
    m.set('a', 1);
    m.set('b', 2);
    m.set('c', 3);
    
    assert.equal(m.get('a'), 1);
    assert.equal(m.get('b'), 2);
    assert.equal(m.get('c'), 3);
    assert.equal(m.get('d'), undefined);
    assert.equal(m.size, 3);
  });

  it('update existing key', () => {
    const m = new RobinHoodHashMap();
    m.set('key', 'v1');
    m.set('key', 'v2');
    assert.equal(m.get('key'), 'v2');
    assert.equal(m.size, 1);
  });

  it('delete', () => {
    const m = new RobinHoodHashMap();
    m.set('a', 1);
    m.set('b', 2);
    
    assert.equal(m.delete('a'), true);
    assert.equal(m.get('a'), undefined);
    assert.equal(m.get('b'), 2); // Should still exist
    assert.equal(m.size, 1);
    assert.equal(m.delete('a'), false);
  });

  it('has', () => {
    const m = new RobinHoodHashMap();
    m.set('x', 1);
    assert.equal(m.has('x'), true);
    assert.equal(m.has('y'), false);
  });

  it('auto-resize on high load', () => {
    const m = new RobinHoodHashMap(4, 0.75);
    for (let i = 0; i < 100; i++) m.set(`key-${i}`, i);
    
    assert.equal(m.size, 100);
    assert.ok(m.capacity >= 128); // Should have resized
    
    // All elements should still be findable
    for (let i = 0; i < 100; i++) {
      assert.equal(m.get(`key-${i}`), i);
    }
  });

  it('low max probe distance (Robin Hood advantage)', () => {
    const m = new RobinHoodHashMap(256);
    for (let i = 0; i < 200; i++) m.set(i, i);
    
    const stats = m.getStats();
    console.log(`  200 elements in 256 slots: avg probe=${stats.avgProbeDistance}, max probe=${stats.maxProbeDistance}`);
    
    // Robin Hood should keep max probe distance low
    assert.ok(stats.maxProbeDistance < 20, `Max probe ${stats.maxProbeDistance} too high`);
    assert.ok(stats.avgProbeDistance < 5, `Avg probe ${stats.avgProbeDistance} too high`);
  });

  it('probe distance distribution', () => {
    const m = new RobinHoodHashMap(1024);
    for (let i = 0; i < 800; i++) m.set(`item-${i}`, i);
    
    const stats = m.getStats();
    const dist = stats.probeDistribution;
    
    console.log(`  Probe distribution (800/1024):`, 
      Object.entries(dist).sort(([a], [b]) => a - b).map(([d, c]) => `d=${d}: ${c}`).join(', '));
    
    // Most elements should be at distance 0 or 1
    const nearCount = (dist['0'] || 0) + (dist['1'] || 0);
    assert.ok(nearCount > 400, 'Most elements should be within distance 1');
  });

  it('numeric and string keys', () => {
    const m = new RobinHoodHashMap();
    m.set(42, 'answer');
    m.set('hello', 'world');
    m.set(0, 'zero');
    
    assert.equal(m.get(42), 'answer');
    assert.equal(m.get('hello'), 'world');
    assert.equal(m.get(0), 'zero');
  });

  it('entries iterator', () => {
    const m = new RobinHoodHashMap();
    m.set('a', 1);
    m.set('b', 2);
    m.set('c', 3);
    
    const entries = [...m.entries()];
    assert.equal(entries.length, 3);
    const keys = entries.map(e => e.key).sort();
    assert.deepEqual(keys, ['a', 'b', 'c']);
  });

  it('stress: 10K insert + 10K lookup', () => {
    const m = new RobinHoodHashMap();
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) m.set(i, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) m.get(i);
    const lookupMs = performance.now() - t1;
    
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms (${(insertMs/10000*1000).toFixed(3)}µs avg)`);
    console.log(`  10K lookup: ${lookupMs.toFixed(1)}ms (${(lookupMs/10000*1000).toFixed(3)}µs avg)`);
    
    const stats = m.getStats();
    console.log(`  Load: ${(stats.loadFactor*100).toFixed(1)}%, avg probe: ${stats.avgProbeDistance}, max probe: ${stats.maxProbeDistance}`);
    
    assert.ok(insertMs < 500);
    assert.ok(lookupMs < 500);
  });

  it('delete + re-insert maintains correctness', () => {
    const m = new RobinHoodHashMap();
    for (let i = 0; i < 100; i++) m.set(i, i);
    
    // Delete even numbers
    for (let i = 0; i < 100; i += 2) m.delete(i);
    assert.equal(m.size, 50);
    
    // Odd numbers should still be present
    for (let i = 1; i < 100; i += 2) {
      assert.equal(m.get(i), i);
    }
    
    // Re-insert evens
    for (let i = 0; i < 100; i += 2) m.set(i, i + 1000);
    assert.equal(m.size, 100);
    assert.equal(m.get(0), 1000);
    assert.equal(m.get(1), 1);
  });
});
