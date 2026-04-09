// siphash.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { siphash } from './siphash.js';

describe('SipHash', () => {
  it('deterministic', () => {
    const h1 = siphash('hello');
    const h2 = siphash('hello');
    assert.equal(h1, h2);
  });

  it('different keys → different hashes', () => {
    assert.notEqual(siphash('hello'), siphash('world'));
  });

  it('avalanche: similar inputs → different hashes', () => {
    const h1 = siphash('test1');
    const h2 = siphash('test2');
    // At least some bits should differ
    const diff = h1 ^ h2;
    assert.notEqual(diff, 0n);
  });
});
