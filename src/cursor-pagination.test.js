// cursor-pagination.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CursorPaginator } from './cursor-pagination.js';

const data = Array.from({ length: 50 }, (_, i) => ({ id: i + 1, name: `item_${i + 1}` }));

describe('CursorPaginator', () => {
  it('first page', () => {
    const p = new CursorPaginator(data, { pageSize: 10 });
    const page = p.first();
    assert.equal(page.items.length, 10);
    assert.equal(page.items[0].id, 1);
    assert.equal(page.items[9].id, 10);
    assert.ok(page.pageInfo.hasNextPage);
    assert.ok(!page.pageInfo.hasPreviousPage);
  });

  it('next page after cursor', () => {
    const p = new CursorPaginator(data, { pageSize: 10 });
    const page = p.after(10);
    assert.equal(page.items.length, 10);
    assert.equal(page.items[0].id, 11);
  });

  it('last page', () => {
    const p = new CursorPaginator(data, { pageSize: 10 });
    const page = p.after(40);
    assert.equal(page.items.length, 10);
    assert.equal(page.items[0].id, 41);
    assert.ok(!page.pageInfo.hasNextPage);
  });

  it('previous page', () => {
    const p = new CursorPaginator(data, { pageSize: 10 });
    const page = p.before(21);
    assert.equal(page.items.length, 10);
    assert.equal(page.items[0].id, 11);
    assert.equal(page.items[9].id, 20);
  });

  it('descending order', () => {
    const p = new CursorPaginator(data, { pageSize: 5, direction: 'DESC' });
    const page = p.first();
    assert.equal(page.items[0].id, 50);
    assert.equal(page.items[4].id, 46);
  });

  it('page info: totalCount', () => {
    const p = new CursorPaginator(data, { pageSize: 10 });
    assert.equal(p.first().totalCount, 50);
  });

  it('empty data', () => {
    const p = new CursorPaginator([], { pageSize: 10 });
    const page = p.first();
    assert.equal(page.items.length, 0);
    assert.ok(!page.pageInfo.hasNextPage);
  });

  it('page size larger than data', () => {
    const small = [{ id: 1 }, { id: 2 }];
    const p = new CursorPaginator(small, { pageSize: 10 });
    const page = p.first();
    assert.equal(page.items.length, 2);
    assert.ok(!page.pageInfo.hasNextPage);
  });
});
