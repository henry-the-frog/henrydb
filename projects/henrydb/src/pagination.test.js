// pagination.test.js
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { OffsetPaginator, KeysetPaginator, CursorPaginator } from './pagination.js';

const rows = Array.from({ length: 50 }, (_, i) => ({ id: i + 1, name: `Item ${i + 1}` }));

describe('OffsetPaginator', () => {
  const pag = new OffsetPaginator();

  test('first page', () => {
    const r = pag.paginate(rows, 1, 10);
    assert.equal(r.data.length, 10);
    assert.equal(r.data[0].id, 1);
    assert.equal(r.page, 1);
    assert.equal(r.totalPages, 5);
    assert.ok(r.hasNextPage);
    assert.ok(!r.hasPrevPage);
  });

  test('middle page', () => {
    const r = pag.paginate(rows, 3, 10);
    assert.equal(r.data[0].id, 21);
    assert.ok(r.hasNextPage);
    assert.ok(r.hasPrevPage);
  });

  test('last page', () => {
    const r = pag.paginate(rows, 5, 10);
    assert.equal(r.data[0].id, 41);
    assert.ok(!r.hasNextPage);
    assert.ok(r.hasPrevPage);
  });

  test('page beyond end', () => {
    const r = pag.paginate(rows, 6, 10);
    assert.equal(r.data.length, 0);
  });
});

describe('KeysetPaginator', () => {
  const pag = new KeysetPaginator('id', 'asc');

  test('first page (no cursor)', () => {
    const r = pag.paginate(rows, null, 10);
    assert.equal(r.data.length, 10);
    assert.equal(r.data[0].id, 1);
    assert.equal(r.cursor, 10);
    assert.ok(r.hasMore);
  });

  test('second page (after cursor)', () => {
    const r = pag.paginate(rows, 10, 10);
    assert.equal(r.data.length, 10);
    assert.equal(r.data[0].id, 11);
    assert.equal(r.cursor, 20);
  });

  test('last page', () => {
    const r = pag.paginate(rows, 40, 10);
    assert.equal(r.data.length, 10);
    assert.equal(r.data[0].id, 41);
    assert.ok(!r.hasMore);
  });

  test('descending order', () => {
    const descPag = new KeysetPaginator('id', 'desc');
    const r = descPag.paginate(rows, null, 5);
    assert.equal(r.data[0].id, 50);
    assert.equal(r.data[4].id, 46);
  });
});

describe('CursorPaginator', () => {
  const pag = new CursorPaginator('id');

  test('first N items', () => {
    const r = pag.paginate(rows, { first: 5 });
    assert.equal(r.edges.length, 5);
    assert.equal(r.edges[0].node.id, 1);
    assert.ok(r.edges[0].cursor);
    assert.ok(r.pageInfo.hasNextPage);
    assert.ok(!r.pageInfo.hasPreviousPage);
    assert.equal(r.totalCount, 50);
  });

  test('first N after cursor', () => {
    const page1 = pag.paginate(rows, { first: 5 });
    const r = pag.paginate(rows, { first: 5, after: page1.pageInfo.endCursor });
    assert.equal(r.edges[0].node.id, 6);
    assert.equal(r.edges.length, 5);
    assert.ok(r.pageInfo.hasPreviousPage);
  });

  test('last N items', () => {
    const r = pag.paginate(rows, { last: 5 });
    assert.equal(r.edges.length, 5);
    assert.equal(r.edges[0].node.id, 46);
    assert.ok(!r.pageInfo.hasNextPage);
    assert.ok(r.pageInfo.hasPreviousPage);
  });

  test('last N before cursor', () => {
    const page = pag.paginate(rows, { first: 5 });
    // Get items before the start of page 1 should be empty
    const r = pag.paginate(rows, { last: 3, before: page.pageInfo.startCursor });
    assert.equal(r.edges.length, 0);
  });

  test('cursor encodes/decodes correctly', () => {
    const page = pag.paginate(rows, { first: 3 });
    assert.ok(page.pageInfo.startCursor);
    assert.ok(page.pageInfo.endCursor);
    // Cursors should be base64
    assert.ok(/^[A-Za-z0-9+/=]+$/.test(page.pageInfo.startCursor));
  });

  test('full traversal via cursors', () => {
    let allItems = [];
    let cursor = null;
    let hasMore = true;

    while (hasMore) {
      const page = pag.paginate(rows, { first: 10, ...(cursor ? { after: cursor } : {}) });
      allItems.push(...page.edges.map(e => e.node));
      cursor = page.pageInfo.endCursor;
      hasMore = page.pageInfo.hasNextPage;
    }

    assert.equal(allItems.length, 50);
    assert.equal(allItems[0].id, 1);
    assert.equal(allItems[49].id, 50);
  });
});
