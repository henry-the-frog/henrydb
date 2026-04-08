// robin-hood-hash.test.js — Tests for Robin Hood hash table
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RobinHoodHashTable } from './robin-hood-hash.js';

describe('RobinHoodHashTable', () => {

  it('basic set and get', () => {
    const ht = new RobinHoodHashTable();
    ht.set(1, 'a');
    ht.set(2, 'b');
    assert.equal(ht.get(1), 'a');
    assert.equal(ht.get(2), 'b');
    assert.equal(ht.get(3), undefined);
  });

  it('update existing', () => {
    const ht = new RobinHoodHashTable();
    ht.set(5, 'old');
    ht.set(5, 'new');
    assert.equal(ht.get(5), 'new');
    assert.equal(ht.size, 1);
  });

  it('delete with backward shift', () => {
    const ht = new RobinHoodHashTable();
    ht.set(1, 'a');
    ht.set(2, 'b');
    assert.ok(ht.delete(1));
    assert.equal(ht.get(1), undefined);
    assert.equal(ht.get(2), 'b');
  });

  it('handles high load factor', () => {
    const ht = new RobinHoodHashTable(16);
    for (let i = 0; i < 50; i++) ht.set(i, i * 10);

    assert.equal(ht.size, 50);
    for (let i = 0; i < 50; i++) assert.equal(ht.get(i), i * 10);
  });

  it('string keys', () => {
    const ht = new RobinHoodHashTable();
    ht.set('hello', 1);
    ht.set('world', 2);
    assert.equal(ht.get('hello'), 1);
    assert.equal(ht.get('world'), 2);
  });

  it('1000 inserts', () => {
    const ht = new RobinHoodHashTable(256);
    for (let i = 0; i < 1000; i++) ht.set(i, i);

    assert.equal(ht.size, 1000);
    for (let i = 0; i < 1000; i++) assert.equal(ht.get(i), i);
  });

  it('low max displacement (Robin Hood property)', () => {
    const ht = new RobinHoodHashTable(1024);
    for (let i = 0; i < 500; i++) ht.set(i, i);

    const stats = ht.getStats();
    // Robin Hood should keep max displacement low
    assert.ok(stats.maxDisplacement < 50, `Max displacement too high: ${stats.maxDisplacement}`);
  });

  it('benchmark: Robin Hood vs Map', () => {
    const n = 50000;
    
    const ht = new RobinHoodHashTable(n * 2);
    const map = new Map();
    
    const t0 = Date.now();
    for (let i = 0; i < n; i++) ht.set(i, i);
    const rhBuildMs = Date.now() - t0;

    const t1 = Date.now();
    for (let i = 0; i < n; i++) map.set(i, i);
    const mapBuildMs = Date.now() - t1;

    const t2 = Date.now();
    for (let i = 0; i < n; i++) ht.get(i);
    const rhLookupMs = Date.now() - t2;

    const t3 = Date.now();
    for (let i = 0; i < n; i++) map.get(i);
    const mapLookupMs = Date.now() - t3;

    const stats = ht.getStats();
    console.log(`    Build: RH ${rhBuildMs}ms vs Map ${mapBuildMs}ms | Lookup: RH ${rhLookupMs}ms (${stats.avgProbesPerLookup} probes/lookup) vs Map ${mapLookupMs}ms`);
  });
});
