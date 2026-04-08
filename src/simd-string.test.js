// simd-string.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { batchLike, batchContains, batchUpper, batchLower, batchLength, batchSubstring, batchConcat, batchTrim, likeToRegex } from './simd-string.js';

const names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', null, 'Frank', 'Grace'];

describe('SIMD String Operations', () => {
  it('batchLike with % wildcard', () => {
    const sel = batchLike(names, 'A%');
    assert.deepEqual(sel, [0]); // Alice
  });

  it('batchLike with _ wildcard', () => {
    const sel = batchLike(names, 'B_b');
    assert.deepEqual(sel, [1]); // Bob
  });

  it('batchContains', () => {
    const sel = batchContains(names, 'ar');
    assert.ok(sel.includes(2)); // Charlie
  });

  it('batchUpper', () => {
    const result = batchUpper(names);
    assert.equal(result[0], 'ALICE');
    assert.equal(result[5], null);
  });

  it('batchLower', () => {
    const result = batchLower(names);
    assert.equal(result[0], 'alice');
  });

  it('batchLength', () => {
    const result = batchLength(names);
    assert.equal(result[0], 5); // Alice
    assert.equal(result[5], null);
  });

  it('batchSubstring', () => {
    const result = batchSubstring(names, 0, 3);
    assert.equal(result[0], 'Ali');
    assert.equal(result[1], 'Bob');
  });

  it('batchConcat', () => {
    const first = ['John', 'Jane'];
    const last = ['Doe', 'Smith'];
    assert.deepEqual(batchConcat(first, last, ' '), ['John Doe', 'Jane Smith']);
  });

  it('batchTrim', () => {
    assert.deepEqual(batchTrim(['  hello ', ' world', null]), ['hello', 'world', null]);
  });

  it('benchmark: 100K LIKE operations', () => {
    const col = Array.from({ length: 100000 }, (_, i) => `user_${i}_name`);
    const t0 = Date.now();
    const sel = batchLike(col, '%500%');
    const ms = Date.now() - t0;
    console.log(`    100K LIKE: ${ms}ms, ${sel.length} matches`);
  });
});
