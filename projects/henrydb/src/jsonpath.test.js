// jsonpath.test.js — Tests for JSON Path query engine
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { JSONPath } from './jsonpath.js';

const store = {
  store: {
    book: [
      { category: 'reference', author: 'Nigel Rees', title: 'Sayings of the Century', price: 8.95 },
      { category: 'fiction', author: 'Evelyn Waugh', title: 'Sword of Honour', price: 12.99 },
      { category: 'fiction', author: 'Herman Melville', title: 'Moby Dick', isbn: '0-553-21311-3', price: 8.99 },
      { category: 'fiction', author: 'J. R. R. Tolkien', title: 'The Lord of the Rings', isbn: '0-395-19395-8', price: 22.99 },
    ],
    bicycle: { color: 'red', price: 19.95 },
  },
};

describe('JSONPath', () => {
  describe('basic access', () => {
    test('$ returns root', () => {
      const r = JSONPath.query(store, '$');
      assert.equal(r.length, 1);
      assert.deepEqual(r[0], store);
    });

    test('$.store.bicycle.color', () => {
      const r = JSONPath.query(store, '$.store.bicycle.color');
      assert.deepEqual(r, ['red']);
    });

    test('$.store.book[0].title', () => {
      const r = JSONPath.query(store, '$.store.book[0].title');
      assert.deepEqual(r, ['Sayings of the Century']);
    });

    test('$.store.book[-1].title (negative index)', () => {
      const r = JSONPath.query(store, '$.store.book[-1].title');
      assert.deepEqual(r, ['The Lord of the Rings']);
    });
  });

  describe('wildcard', () => {
    test('$.store.book[*].author', () => {
      const r = JSONPath.query(store, '$.store.book[*].author');
      assert.equal(r.length, 4);
      assert.ok(r.includes('Nigel Rees'));
      assert.ok(r.includes('J. R. R. Tolkien'));
    });

    test('$.store.* (all direct children)', () => {
      const r = JSONPath.query(store, '$.store.*');
      assert.equal(r.length, 2); // book array + bicycle object
    });
  });

  describe('recursive descent', () => {
    test('$..author (all authors)', () => {
      const r = JSONPath.query(store, '$..author');
      assert.equal(r.length, 4);
      assert.ok(r.includes('Herman Melville'));
    });

    test('$..price (all prices)', () => {
      const r = JSONPath.query(store, '$..price');
      assert.equal(r.length, 5); // 4 book prices + bicycle price
      assert.ok(r.includes(19.95));
    });

    test('$..isbn (books with ISBN)', () => {
      const r = JSONPath.query(store, '$..isbn');
      assert.equal(r.length, 2);
    });
  });

  describe('filter expressions', () => {
    test('$.store.book[?(@.price < 10)]', () => {
      const r = JSONPath.query(store, '$.store.book[?(@.price < 10)]');
      assert.equal(r.length, 2);
      assert.ok(r.every(b => b.price < 10));
    });

    test('$.store.book[?(@.price > 20)]', () => {
      const r = JSONPath.query(store, '$.store.book[?(@.price > 20)]');
      assert.equal(r.length, 1);
      assert.equal(r[0].title, 'The Lord of the Rings');
    });

    test('$.store.book[?(@.category == "fiction")]', () => {
      const r = JSONPath.query(store, '$.store.book[?(@.category == "fiction")]');
      assert.equal(r.length, 3);
      assert.ok(r.every(b => b.category === 'fiction'));
    });

    test('filter with string comparison', () => {
      const r = JSONPath.query(store, '$.store.book[?(@.author == "Herman Melville")]');
      assert.equal(r.length, 1);
      assert.equal(r[0].title, 'Moby Dick');
    });
  });

  describe('slice', () => {
    test('$.store.book[0:2]', () => {
      const r = JSONPath.query(store, '$.store.book[0:2]');
      assert.equal(r.length, 2);
      assert.equal(r[0].title, 'Sayings of the Century');
    });

    test('$.store.book[1:3]', () => {
      const r = JSONPath.query(store, '$.store.book[1:3]');
      assert.equal(r.length, 2);
      assert.equal(r[0].title, 'Sword of Honour');
    });
  });

  describe('JSONPath.first', () => {
    test('returns first match', () => {
      const r = JSONPath.first(store, '$.store.book[0].title');
      assert.equal(r, 'Sayings of the Century');
    });

    test('returns undefined for no match', () => {
      const r = JSONPath.first(store, '$.nonexistent');
      assert.equal(r, undefined);
    });
  });

  describe('JSONPath.exists', () => {
    test('returns true for existing path', () => {
      assert.ok(JSONPath.exists(store, '$.store.bicycle'));
    });

    test('returns false for missing path', () => {
      assert.ok(!JSONPath.exists(store, '$.store.car'));
    });
  });

  describe('JSONPath.contains (@>)', () => {
    test('object contains subset', () => {
      assert.ok(JSONPath.contains({ a: 1, b: 2, c: 3 }, { a: 1, b: 2 }));
    });

    test('object does not contain non-matching subset', () => {
      assert.ok(!JSONPath.contains({ a: 1, b: 2 }, { a: 1, b: 3 }));
    });

    test('array contains subset', () => {
      assert.ok(JSONPath.contains([1, 2, 3, 4], [2, 4]));
    });

    test('nested object containment', () => {
      assert.ok(JSONPath.contains(
        { a: { b: { c: 1, d: 2 } } },
        { a: { b: { c: 1 } } }
      ));
    });

    test('array of objects containment', () => {
      assert.ok(JSONPath.contains(
        [{ id: 1, name: 'a' }, { id: 2, name: 'b' }],
        [{ id: 1 }]
      ));
    });

    test('scalar equality', () => {
      assert.ok(JSONPath.contains(42, 42));
      assert.ok(!JSONPath.contains(42, 43));
    });
  });

  describe('complex queries', () => {
    test('nested object navigation', () => {
      const data = {
        users: [
          { name: 'Alice', addresses: [{ city: 'NYC' }, { city: 'LA' }] },
          { name: 'Bob', addresses: [{ city: 'SF' }] },
        ],
      };

      const cities = JSONPath.query(data, '$.users[*].addresses[*].city');
      assert.deepEqual(cities, ['NYC', 'LA', 'SF']);
    });

    test('filter on nested value', () => {
      const data = {
        products: [
          { name: 'A', details: { weight: 100 } },
          { name: 'B', details: { weight: 200 } },
          { name: 'C', details: { weight: 50 } },
        ],
      };

      const heavy = JSONPath.query(data, '$.products[?(@.details.weight > 150)]');
      assert.equal(heavy.length, 1);
      assert.equal(heavy[0].name, 'B');
    });

    test('empty results for non-matching queries', () => {
      const r = JSONPath.query(store, '$.store.book[?(@.price > 100)]');
      assert.deepEqual(r, []);
    });
  });
});
