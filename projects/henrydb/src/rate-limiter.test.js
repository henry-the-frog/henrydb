// rate-limiter.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TokenBucket, SlidingWindowCounter } from './rate-limiter.js';

describe('TokenBucket', () => {
  it('allows within capacity', () => {
    const tb = new TokenBucket(5, 1);
    assert.equal(tb.allow(), true);
    assert.equal(tb.allow(), true);
  });

  it('rejects over capacity', () => {
    const tb = new TokenBucket(2, 0);
    assert.equal(tb.allow(), true);
    assert.equal(tb.allow(), true);
    assert.equal(tb.allow(), false);
  });
});

describe('SlidingWindowCounter', () => {
  it('allows within limit', () => {
    const sw = new SlidingWindowCounter(1000, 3);
    assert.equal(sw.allow(), true);
    assert.equal(sw.allow(), true);
    assert.equal(sw.allow(), true);
    assert.equal(sw.allow(), false);
  });
});
