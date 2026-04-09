// mvcc.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCStore } from './mvcc.js';

describe('MVCCStore', () => {
  it('basic read/write within transaction', () => {
    const store = new MVCCStore();
    const tx = store.begin();
    store.write(tx, 'key1', 'value1');
    assert.equal(store.read(tx, 'key1'), 'value1');
    store.commit(tx);
  });

  it('snapshot isolation: tx2 does not see tx1 uncommitted writes', () => {
    const store = new MVCCStore();
    const tx1 = store.begin();
    const tx2 = store.begin();
    
    store.write(tx1, 'key', 'from-tx1');
    
    // tx2 started before tx1's write, shouldn't see it
    assert.equal(store.read(tx2, 'key'), undefined);
    
    store.commit(tx1);
    store.commit(tx2);
  });

  it('committed writes are visible to new transactions', () => {
    const store = new MVCCStore();
    const tx1 = store.begin();
    store.write(tx1, 'key', 'hello');
    store.commit(tx1);
    
    const tx2 = store.begin();
    assert.equal(store.read(tx2, 'key'), 'hello');
    store.commit(tx2);
  });

  it('rollback removes writes', () => {
    const store = new MVCCStore();
    const tx = store.begin();
    store.write(tx, 'key', 'value');
    store.rollback(tx);
    
    const tx2 = store.begin();
    assert.equal(store.read(tx2, 'key'), undefined);
  });

  it('delete within MVCC', () => {
    const store = new MVCCStore();
    const tx1 = store.begin();
    store.write(tx1, 'key', 'value');
    store.commit(tx1);
    
    const tx2 = store.begin();
    store.delete(tx2, 'key');
    store.commit(tx2);
    
    const tx3 = store.begin();
    assert.equal(store.read(tx3, 'key'), undefined);
  });

  it('multiple versions of same key', () => {
    const store = new MVCCStore();
    
    const tx1 = store.begin();
    store.write(tx1, 'counter', 1);
    store.commit(tx1);
    
    const tx2 = store.begin();
    store.write(tx2, 'counter', 2);
    store.commit(tx2);
    
    const tx3 = store.begin();
    store.write(tx3, 'counter', 3);
    store.commit(tx3);
    
    const stats = store.getStats();
    assert.equal(stats.totalVersions, 3);
  });
});
