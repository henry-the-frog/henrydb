// symmetric-hash-join.test.js — Tests for symmetric hash join
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SymmetricHashJoin } from './symmetric-hash-join.js';

describe('SymmetricHashJoin', () => {

  it('basic join with interleaved processing', () => {
    const shj = new SymmetricHashJoin(r => r.id, r => r.a_id);

    // Process interleaved
    shj.processLeft({ id: 1, name: 'Alice' });
    shj.processRight({ a_id: 1, val: 'x' }); // Match!
    shj.processLeft({ id: 2, name: 'Bob' });
    shj.processRight({ a_id: 2, val: 'y' }); // Match!
    shj.processRight({ a_id: 1, val: 'z' }); // Match with Alice!

    assert.equal(shj.totalMatches, 3);
  });

  it('processBatch: interleaved batch', () => {
    const shj = new SymmetricHashJoin(r => r.id, r => r.a_id);

    const left = [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }];
    const right = [{ a_id: 1, val: 'x' }, { a_id: 2, val: 'y' }, { a_id: 1, val: 'z' }];

    shj.processBatch(left, right);
    assert.equal(shj.totalMatches, 3);
  });

  it('no matches', () => {
    const shj = new SymmetricHashJoin(r => r.id, r => r.a_id);
    shj.processLeft({ id: 1 });
    shj.processRight({ a_id: 2 });
    assert.equal(shj.totalMatches, 0);
  });

  it('materialize produces row objects', () => {
    const shj = new SymmetricHashJoin(r => r.id, r => r.a_id);
    shj.processLeft({ id: 1, name: 'Alice' });
    shj.processRight({ a_id: 1, data: 'hello' });

    const rows = shj.materialize();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'Alice');
    assert.equal(rows[0].data, 'hello');
  });

  it('streaming: results available incrementally', () => {
    const shj = new SymmetricHashJoin(r => r.id, r => r.a_id);
    
    // First batch: no matches yet
    shj.processLeft({ id: 1, name: 'Alice' });
    assert.equal(shj.totalMatches, 0);

    // Right side arrives: immediate match
    const matches = shj.processRight({ a_id: 1, val: 'x' });
    assert.equal(matches.length, 1);
    assert.equal(shj.totalMatches, 1);

    // Another left arrives: immediate match with existing right
    const matches2 = shj.processLeft({ id: 1, name: 'Alice2' });
    assert.equal(matches2.length, 1); // Matches the already-seen right row
  });

  it('many-to-many join', () => {
    const shj = new SymmetricHashJoin(r => r.key, r => r.key);
    
    shj.processBatch(
      [{ key: 1, side: 'L1' }, { key: 1, side: 'L2' }],
      [{ key: 1, side: 'R1' }, { key: 1, side: 'R2' }]
    );

    // 2 left × 2 right = 4 matches (but interleaved, so depends on order)
    // L1 processes, no right yet → 0
    // R1 processes, matches L1 → 1
    // L2 processes, matches R1 → 1
    // R2 processes, matches L1 and L2 → 2
    assert.equal(shj.totalMatches, 4);
  });

  it('benchmark: symmetric vs standard hash join', () => {
    const n = 10000;
    const leftRows = Array.from({ length: n }, (_, i) => ({ id: i }));
    const rightRows = Array.from({ length: n * 2 }, (_, i) => ({ a_id: i % n }));

    // Symmetric
    const shj = new SymmetricHashJoin(r => r.id, r => r.a_id);
    const t0 = Date.now();
    shj.processBatch(leftRows, rightRows);
    const shjMs = Date.now() - t0;

    // Standard hash join
    const t1 = Date.now();
    const ht = new Map();
    for (let i = 0; i < rightRows.length; i++) {
      const key = rightRows[i].a_id;
      if (!ht.has(key)) ht.set(key, []);
      ht.get(key).push(i);
    }
    let stdMatches = 0;
    for (let i = 0; i < leftRows.length; i++) {
      const matches = ht.get(leftRows[i].id);
      if (matches) stdMatches += matches.length;
    }
    const stdMs = Date.now() - t1;

    console.log(`    Symmetric: ${shjMs}ms (${shj.totalMatches} matches) vs Standard: ${stdMs}ms (${stdMatches} matches)`);
    assert.equal(shj.totalMatches, stdMatches);
  });

  it('stats tracked', () => {
    const shj = new SymmetricHashJoin(r => r.id, r => r.a_id);
    shj.processLeft({ id: 1 });
    shj.processRight({ a_id: 1 });

    const stats = shj.getStats();
    assert.equal(stats.leftProcessed, 1);
    assert.equal(stats.rightProcessed, 1);
    assert.equal(stats.matches, 1);
  });
});
