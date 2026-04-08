// write-batch.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WriteBatch, TournamentTree, NSMPage, DSMPage } from './write-batch.js';

describe('WriteBatch', () => {
  it('atomic writes', () => {
    const store = new Map();
    const batch = new WriteBatch(store);
    batch.put('a', 1).put('b', 2).put('c', 3);
    const result = batch.commit();
    assert.equal(result.ok, true);
    assert.equal(store.get('a'), 1);
    assert.equal(store.get('b'), 2);
    assert.equal(store.get('c'), 3);
  });

  it('delete in batch', () => {
    const store = new Map([['a', 1], ['b', 2]]);
    const batch = new WriteBatch(store);
    batch.delete('a').put('c', 3);
    batch.commit();
    assert.equal(store.has('a'), false);
    assert.equal(store.get('c'), 3);
  });

  it('double commit throws', () => {
    const store = new Map();
    const batch = new WriteBatch(store);
    batch.put('a', 1);
    batch.commit();
    assert.throws(() => batch.commit());
  });

  it('batch size', () => {
    const batch = new WriteBatch(new Map());
    batch.put('a', 1).put('b', 2).delete('c');
    assert.equal(batch.size, 3);
  });
});

describe('TournamentTree (k-way merge)', () => {
  it('merge 3 sorted arrays', () => {
    const a = [1, 4, 7];
    const b = [2, 5, 8];
    const c = [3, 6, 9];
    const tt = new TournamentTree([a, b, c]);
    const result = [...tt.merge()];
    assert.deepEqual(result, [1, 2, 3, 4, 5, 6, 7, 8, 9]);
  });

  it('merge with empty source', () => {
    const a = [1, 2, 3];
    const b = [];
    const c = [4, 5];
    const tt = new TournamentTree([a, b, c]);
    const result = [...tt.merge()];
    assert.deepEqual(result, [1, 2, 3, 4, 5]);
  });

  it('single source', () => {
    const tt = new TournamentTree([[1, 2, 3]]);
    assert.deepEqual([...tt.merge()], [1, 2, 3]);
  });

  it('many sources', () => {
    const sources = Array.from({ length: 10 }, (_, i) => [i * 10, i * 10 + 5]);
    const tt = new TournamentTree(sources);
    const result = [...tt.merge()];
    assert.equal(result.length, 20);
    // Check sorted
    for (let i = 1; i < result.length; i++) assert.ok(result[i] >= result[i - 1]);
  });
});

describe('NSMPage', () => {
  it('insert and scan', () => {
    const page = new NSMPage(4096);
    page.insert({ id: 1, name: 'Alice' });
    page.insert({ id: 2, name: 'Bob' });
    assert.equal(page.count, 2);
    assert.equal(page.get(0).name, 'Alice');
  });

  it('rejects when full', () => {
    const page = new NSMPage(50); // Tiny page
    assert.equal(page.insert({ id: 1, name: 'Alice' }), true);
    assert.equal(page.insert({ id: 2, name: 'Bob who has a really long name' }), false);
  });

  it('utilization tracking', () => {
    const page = new NSMPage(4096);
    page.insert({ id: 1 });
    assert.ok(page.utilization > 0 && page.utilization < 1);
  });
});

describe('DSMPage', () => {
  it('insert and reconstruct', () => {
    const page = new DSMPage(['id', 'name', 'age']);
    page.insert({ id: 1, name: 'Alice', age: 30 });
    page.insert({ id: 2, name: 'Bob', age: 25 });
    assert.equal(page.count, 2);
    assert.deepEqual(page.reconstruct(0), { id: 1, name: 'Alice', age: 30 });
  });

  it('column access', () => {
    const page = new DSMPage(['id', 'val']);
    page.insert({ id: 1, val: 100 });
    page.insert({ id: 2, val: 200 });
    assert.deepEqual(page.getColumn('val'), [100, 200]);
  });
});
