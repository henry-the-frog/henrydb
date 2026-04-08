// timestamp-ordering.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TimestampOrdering } from './timestamp-ordering.js';

describe('TimestampOrdering', () => {
  it('basic read/write/commit', () => {
    const to = new TimestampOrdering();
    to.begin('T1');
    assert.ok(to.write('T1', 'x', 100).ok);
    const r = to.read('T1', 'x');
    assert.ok(r.ok);
    assert.equal(r.value, 100);
    assert.ok(to.commit('T1'));
    assert.equal(to.getValue('x'), 100);
  });

  it('concurrent reads are fine', () => {
    const to = new TimestampOrdering();
    to.begin('T1');
    to.write('T1', 'x', 42);
    to.commit('T1');
    
    to.begin('T2');
    to.begin('T3');
    assert.ok(to.read('T2', 'x').ok);
    assert.ok(to.read('T3', 'x').ok);
  });

  it('abort: read after newer write', () => {
    const to = new TimestampOrdering();
    to.begin('T1'); // ts=1
    to.begin('T2'); // ts=2
    
    // T2 writes x (newer)
    to.write('T2', 'x', 200);
    to.commit('T2');
    
    // T1 tries to read x (older) — should abort
    const r = to.read('T1', 'x');
    assert.ok(!r.ok);
    assert.equal(r.reason, 'read too late');
  });

  it('abort: write after newer read', () => {
    const to = new TimestampOrdering();
    to.begin('T1'); // ts=1
    to.begin('T2'); // ts=2
    
    // Initialize x
    to.begin('T0');
    to.write('T0', 'x', 0);
    to.commit('T0');
    
    to.begin('T3'); // ts=4
    to.begin('T4'); // ts=5
    
    // T4 reads x (newer)
    to.read('T4', 'x');
    
    // T3 tries to write x (older) — should abort (newer txn already read)
    const w = to.write('T3', 'x', 999);
    assert.ok(!w.ok);
    assert.ok(w.reason.includes('read dependency'));
  });

  it('Thomas Write Rule: skip obsolete write', () => {
    const to = new TimestampOrdering();
    to.begin('T1'); // ts=1
    to.begin('T2'); // ts=2
    
    // T2 writes x (newer)
    to.write('T2', 'x', 200);
    to.commit('T2');
    
    // T1 writes x (older, but no newer read) — Thomas Write Rule: skip
    const w = to.write('T1', 'x', 100);
    assert.ok(w.ok);
    assert.ok(w.skipped);
    assert.equal(to.stats.thomasSkips, 1);
  });

  it('serializable schedule', () => {
    const to = new TimestampOrdering();
    
    // Transfer: x=100, y=0 → x=50, y=50
    to.begin('init');
    to.write('init', 'x', 100);
    to.write('init', 'y', 0);
    to.commit('init');
    
    to.begin('T1'); // Transfer 50 from x to y
    const rx = to.read('T1', 'x');
    assert.ok(rx.ok);
    to.write('T1', 'x', rx.value - 50);
    const ry = to.read('T1', 'y');
    to.write('T1', 'y', ry.value + 50);
    to.commit('T1');
    
    assert.equal(to.getValue('x'), 50);
    assert.equal(to.getValue('y'), 50);
  });

  it('stats tracking', () => {
    const to = new TimestampOrdering();
    to.begin('T1');
    to.write('T1', 'a', 1);
    to.read('T1', 'a');
    to.commit('T1');
    
    assert.equal(to.stats.commits, 1);
    assert.equal(to.stats.reads, 1);
    assert.equal(to.stats.writes, 1);
  });
});
