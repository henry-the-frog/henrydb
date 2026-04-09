// transaction.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionManager } from './transaction.js';

describe('TransactionManager', () => {
  it('begin, commit, abort', () => {
    const tm = new TransactionManager();
    const tx1 = tm.begin();
    const tx2 = tm.begin();
    tm.commit(tx1.id);
    tm.abort(tx2.id);
    assert.equal(tx1.status, 'committed');
    assert.equal(tx2.status, 'aborted');
    assert.equal(tm.activeCount, 0);
  });

  it('tracks reads and writes', () => {
    const tm = new TransactionManager();
    const tx = tm.begin();
    tx.addRead('key1');
    tx.addWrite('key2', 'value');
    assert.equal(tx.readSet.size, 1);
    assert.equal(tx.writeSet.size, 1);
  });
});
