// savepoints.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SavepointTransaction } from './savepoints.js';

describe('Savepoints', () => {
  it('basic write and read', () => {
    const tx = new SavepointTransaction('T1');
    tx.write('x', 1);
    assert.equal(tx.read('x'), 1);
  });

  it('rollback to savepoint', () => {
    const tx = new SavepointTransaction('T1');
    tx.write('a', 1);
    tx.savepoint('sp1');
    tx.write('b', 2);
    tx.write('a', 10); // Override a
    
    tx.rollbackTo('sp1');
    assert.equal(tx.read('a'), 1); // Restored
    assert.equal(tx.read('b'), null); // Never happened
  });

  it('nested savepoints', () => {
    const tx = new SavepointTransaction('T1');
    tx.write('x', 1);
    tx.savepoint('sp1');
    tx.write('x', 2);
    tx.savepoint('sp2');
    tx.write('x', 3);
    
    tx.rollbackTo('sp2');
    assert.equal(tx.read('x'), 2);
    
    tx.rollbackTo('sp1');
    assert.equal(tx.read('x'), 1);
  });

  it('commit returns final state', () => {
    const tx = new SavepointTransaction('T1');
    tx.write('a', 1);
    tx.savepoint('sp');
    tx.write('b', 2);
    
    const result = tx.commit();
    assert.equal(result.get('a'), 1);
    assert.equal(result.get('b'), 2);
  });

  it('full rollback', () => {
    const tx = new SavepointTransaction('T1');
    tx.write('a', 1);
    tx.write('b', 2);
    tx.rollback();
    assert.equal(tx.read('a'), null);
    assert.equal(tx.read('b'), null);
  });

  it('release savepoint', () => {
    const tx = new SavepointTransaction('T1');
    tx.savepoint('sp');
    tx.releaseSavepoint('sp');
    assert.throws(() => tx.rollbackTo('sp'));
  });

  it('delete with savepoint rollback', () => {
    const tx = new SavepointTransaction('T1');
    tx.write('x', 42);
    tx.savepoint('sp');
    tx.delete('x');
    assert.equal(tx.read('x'), null);
    
    tx.rollbackTo('sp');
    assert.equal(tx.read('x'), 42);
  });
});
