// cola.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { COLA } from './cola.js';

describe('COLA', () => {
  it('insert and get', () => {
    const c = new COLA();
    c.insert(3, 'c'); c.insert(1, 'a'); c.insert(2, 'b');
    assert.equal(c.get(1), 'a');
    assert.equal(c.get(2), 'b');
    assert.equal(c.get(3), 'c');
    assert.equal(c.get(4), undefined);
  });

  it('level cascade on insert', () => {
    const c = new COLA();
    c.insert(1, 1); // Level 0: [1]
    c.insert(2, 2); // Merge to level 1: [1,2]
    c.insert(3, 3); // Level 0: [3]
    c.insert(4, 4); // Cascade: merge level 0+1 → level 2: [1,2,3,4]
    
    const stats = c.getStats();
    assert.ok(stats.levels.some(l => l.level === 2));
  });

  it('range query', () => {
    const c = new COLA();
    for (let i = 0; i < 10; i++) c.insert(i, i);
    const r = c.range(3, 7);
    assert.deepEqual(r.map(e => e.key), [3, 4, 5, 6, 7]);
  });

  it('upsert: newer value wins', () => {
    const c = new COLA();
    c.insert(1, 'old');
    c.insert(1, 'new');
    assert.equal(c.get(1), 'new');
  });

  it('stress: 5K inserts and lookups', () => {
    const c = new COLA();
    const t0 = performance.now();
    for (let i = 0; i < 5000; i++) c.insert(i, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 5000; i++) assert.equal(c.get(i), i);
    const lookupMs = performance.now() - t1;
    
    console.log(`  5K insert: ${insertMs.toFixed(1)}ms, 5K lookup: ${lookupMs.toFixed(1)}ms`);
    console.log(`  Levels: ${JSON.stringify(c.getStats().levels.map(l => l.level))}`);
  });
});
