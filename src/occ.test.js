// occ.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { OCC } from './occ.js';

describe('OCC', () => {
  it('basic read/write/commit', () => {
    const occ = new OCC();
    occ.begin('T1');
    occ.write('T1', 'x', 100);
    const result = occ.commit('T1');
    assert.ok(result.ok);
    assert.equal(occ.getValue('x'), 100);
  });

  it('read own writes', () => {
    const occ = new OCC();
    occ.begin('T1');
    occ.write('T1', 'x', 42);
    assert.equal(occ.read('T1', 'x'), 42);
  });

  it('no conflict: disjoint write sets', () => {
    const occ = new OCC();
    occ.begin('T1'); occ.begin('T2');
    
    occ.write('T1', 'x', 1);
    occ.write('T2', 'y', 2);
    
    assert.ok(occ.commit('T1').ok);
    assert.ok(occ.commit('T2').ok);
  });

  it('read-write conflict: abort', () => {
    const occ = new OCC();
    occ.begin('T1');
    occ.read('T1', 'x'); // T1 reads x
    
    occ.begin('T2');
    occ.write('T2', 'x', 999); // T2 writes x
    occ.commit('T2'); // T2 commits first
    
    const result = occ.commit('T1'); // T1 should fail validation
    assert.ok(!result.ok);
    assert.ok(result.reason.includes('Conflict'));
  });

  it('write-write conflict: abort', () => {
    const occ = new OCC();
    occ.begin('T1'); occ.begin('T2');
    
    occ.write('T1', 'x', 1);
    occ.write('T2', 'x', 2);
    
    occ.commit('T1'); // T1 commits first
    const result = occ.commit('T2'); // T2 should fail
    assert.ok(!result.ok);
  });

  it('serializable: no lost updates', () => {
    const occ = new OCC();
    
    // Initialize
    occ.begin('init');
    occ.write('init', 'balance', 100);
    occ.commit('init');
    
    // T1: read balance, increment
    occ.begin('T1');
    const b1 = occ.read('T1', 'balance');
    occ.write('T1', 'balance', b1 + 10);
    
    // T2: read balance, decrement (concurrent)
    occ.begin('T2');
    const b2 = occ.read('T2', 'balance');
    occ.write('T2', 'balance', b2 - 5);
    
    occ.commit('T1'); // T1 succeeds
    const r2 = occ.commit('T2'); // T2 should abort (read stale balance)
    assert.ok(!r2.ok);
  });

  it('abort releases resources', () => {
    const occ = new OCC();
    occ.begin('T1');
    occ.write('T1', 'x', 1);
    occ.abort('T1');
    assert.equal(occ.getValue('x'), null); // Never committed
    assert.equal(occ.getStats().activeTxns, 0);
  });

  it('stats', () => {
    const occ = new OCC();
    occ.begin('T1');
    occ.commit('T1');
    occ.begin('T2');
    occ.abort('T2');
    
    const stats = occ.getStats();
    assert.equal(stats.commits, 1);
    assert.equal(stats.aborts, 1);
    assert.equal(stats.validations, 1);
  });
});
