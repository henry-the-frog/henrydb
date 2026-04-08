// kv-store.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RangeTombstoneStore, KVStore, ColumnFamily } from './kv-store.js';

describe('RangeTombstoneStore', () => {
  it('marks range as deleted', () => {
    const ts = new RangeTombstoneStore();
    ts.addTombstone(10, 20);
    assert.equal(ts.isDeleted(15), true);
    assert.equal(ts.isDeleted(5), false);
    assert.equal(ts.isDeleted(20), false); // [10, 20) — exclusive end
  });

  it('filters entries', () => {
    const ts = new RangeTombstoneStore();
    ts.addTombstone('c', 'f');
    const entries = [{ key: 'a' }, { key: 'd' }, { key: 'g' }];
    assert.equal(ts.filter(entries).length, 2);
  });

  it('compact overlapping', () => {
    const ts = new RangeTombstoneStore();
    ts.addTombstone(1, 5);
    ts.addTombstone(3, 8);
    ts.addTombstone(10, 15);
    ts.compact();
    assert.equal(ts.count, 2); // [1,8) and [10,15)
  });
});

describe('KVStore', () => {
  it('put and get', () => {
    const kv = new KVStore();
    kv.put('a', 1);
    kv.put('b', 2);
    assert.equal(kv.get('a'), 1);
    assert.equal(kv.get('b'), 2);
    assert.equal(kv.get('c'), undefined);
  });

  it('delete key', () => {
    const kv = new KVStore();
    kv.put('a', 1);
    kv.delete('a');
    assert.equal(kv.get('a'), undefined);
  });

  it('deleteRange', () => {
    const kv = new KVStore();
    kv.put('a', 1); kv.put('b', 2); kv.put('c', 3); kv.put('d', 4);
    kv.deleteRange('b', 'd');
    assert.equal(kv.get('a'), 1);
    assert.equal(kv.get('b'), undefined);
    assert.equal(kv.get('c'), undefined);
    assert.equal(kv.get('d'), 4);
  });

  it('flush and read from SSTable', () => {
    const kv = new KVStore(5);
    for (let i = 0; i < 10; i++) kv.put(`k${i}`, i);
    assert.ok(kv.sstableCount >= 1);
    for (let i = 0; i < 10; i++) assert.equal(kv.get(`k${i}`), i);
  });

  it('compact SSTables', () => {
    const kv = new KVStore(5);
    for (let i = 0; i < 20; i++) kv.put(`k${i}`, i);
    kv.compact();
    assert.equal(kv.sstableCount, 1);
    assert.equal(kv.get('k10'), 10);
  });

  it('benchmark: 10K ops', () => {
    const kv = new KVStore(1000);
    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) kv.put(`k${i}`, i);
    const t1 = Date.now();
    for (let i = 0; i < 10000; i++) kv.get(`k${i}`);
    console.log(`    KV Store 10K: put=${t1 - t0}ms, get=${Date.now() - t1}ms, flushes=${kv.stats.flushes}`);
  });
});

describe('ColumnFamily', () => {
  it('put and get row', () => {
    const cf = new ColumnFamily('users', ['name', 'age', 'email']);
    cf.putRow('user1', { name: 'Alice', age: 30 });
    const row = cf.getRow('user1');
    assert.equal(row.name, 'Alice');
    assert.equal(row.age, 30);
  });

  it('get single column', () => {
    const cf = new ColumnFamily('users', ['name', 'age']);
    cf.putRow('user1', { name: 'Alice', age: 30 });
    assert.equal(cf.getColumn('user1', 'name'), 'Alice');
  });

  it('delete row', () => {
    const cf = new ColumnFamily('users', ['name', 'age']);
    cf.putRow('user1', { name: 'Alice', age: 30 });
    cf.deleteRow('user1');
    assert.equal(cf.getRow('user1'), null);
  });

  it('missing row returns null', () => {
    const cf = new ColumnFamily('users', ['name']);
    assert.equal(cf.getRow('missing'), null);
  });
});
