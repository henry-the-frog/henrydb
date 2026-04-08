// aries-recovery.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ARIESRecovery } from './aries-recovery.js';

describe('ARIES Recovery', () => {
  it('basic write and read', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'x', 100);
    db.commit('T1');
    assert.equal(db.getValue('x'), 100);
  });

  it('abort undoes writes', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'x', 100);
    db.write('T1', 'y', 200);
    db.abort('T1');
    assert.equal(db.getValue('x'), null);
    assert.equal(db.getValue('y'), null);
  });

  it('crash recovery: committed txn preserved', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'x', 42);
    db.commit('T1');
    
    const result = db.crashAndRecover();
    assert.equal(db.getValue('x'), 42); // Committed data restored
  });

  it('crash recovery: uncommitted txn undone', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'x', 100);
    db.commit('T1');
    
    db.begin('T2');
    db.write('T2', 'x', 999); // Uncommitted
    
    const result = db.crashAndRecover();
    assert.equal(db.getValue('x'), 100); // Reverted to T1's committed value
    assert.ok(result.undone > 0);
  });

  it('crash recovery with checkpoint', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'a', 1);
    db.commit('T1');
    
    db.checkpoint();
    
    db.begin('T2');
    db.write('T2', 'b', 2);
    db.commit('T2');
    
    db.begin('T3');
    db.write('T3', 'c', 3); // Uncommitted at crash
    
    const result = db.crashAndRecover();
    assert.equal(db.getValue('a'), 1);
    assert.equal(db.getValue('b'), 2);
    assert.equal(db.getValue('c'), null); // Undone
  });

  it('multiple updates to same key', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'x', 1);
    db.write('T1', 'x', 2);
    db.write('T1', 'x', 3);
    db.commit('T1');
    assert.equal(db.getValue('x'), 3);
  });

  it('log contains before/after images', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'x', 100);
    
    const log = db.getLog();
    const update = log.find(e => e.type === 'UPDATE');
    assert.equal(update.before, null);
    assert.equal(update.after, 100);
  });

  it('stats tracking', () => {
    const db = new ARIESRecovery();
    db.begin('T1');
    db.write('T1', 'x', 1);
    db.commit('T1');
    db.crashAndRecover();
    
    const stats = db.getStats();
    assert.ok(stats.redone > 0);
    assert.ok(stats.logSize > 0);
  });
});
