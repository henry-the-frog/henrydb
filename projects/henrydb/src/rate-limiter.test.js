// rate-limiter.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  TokenBucket, LeakyBucket, SlidingWindowCounter,
  FixedWindowCounter, PerKeyRateLimiter,
} from './rate-limiter.js';

describe('TokenBucket', () => {
  it('allows within capacity', () => {
    let now = 1000;
    const tb = new TokenBucket(5, 1, () => now);
    assert.ok(tb.allow());
    assert.ok(tb.allow());
    assert.ok(tb.allow());
    assert.ok(tb.allow());
    assert.ok(tb.allow());
    assert.ok(!tb.allow()); // 6th should fail
  });

  it('refills over time', () => {
    let now = 1000;
    const tb = new TokenBucket(5, 10, () => now); // 10/sec
    // Drain all
    for (let i = 0; i < 5; i++) tb.allow();
    assert.ok(!tb.allow());
    // Advance 500ms → 5 tokens refilled
    now += 500;
    assert.ok(tb.allow());
  });

  it('multi-token requests', () => {
    let now = 1000;
    const tb = new TokenBucket(10, 1, () => now);
    assert.ok(tb.allow(5));  // 5 tokens
    assert.ok(tb.allow(5));  // another 5
    assert.ok(!tb.allow(1)); // empty
  });

  it('waitTime returns correct delay', () => {
    let now = 1000;
    const tb = new TokenBucket(10, 2, () => now); // 2/sec
    for (let i = 0; i < 10; i++) tb.allow();
    assert.equal(tb.waitTime(1), 500); // 1 token at 2/sec = 500ms
    assert.equal(tb.waitTime(4), 2000); // 4 tokens at 2/sec = 2000ms
  });

  it('never exceeds capacity', () => {
    let now = 1000;
    const tb = new TokenBucket(5, 100, () => now);
    now += 100000; // way into the future
    assert.equal(tb.available, 5); // capped at capacity
  });
});

describe('LeakyBucket', () => {
  it('allows within capacity', () => {
    let now = 1000;
    const lb = new LeakyBucket(3, 1, () => now);
    assert.ok(lb.allow());
    assert.ok(lb.allow());
    assert.ok(lb.allow());
    assert.ok(!lb.allow()); // full
  });

  it('drains over time', () => {
    let now = 1000;
    const lb = new LeakyBucket(3, 2, () => now); // drain 2/sec
    lb.allow(); lb.allow(); lb.allow(); // full
    assert.ok(!lb.allow());
    now += 1000; // 1 second → 2 drained
    assert.ok(lb.allow());
    assert.ok(lb.allow());
    assert.ok(!lb.allow());
  });

  it('level reports current fill', () => {
    let now = 1000;
    const lb = new LeakyBucket(10, 5, () => now);
    lb.allow(3);
    now += 200; // drain 1 unit (5/sec * 0.2s)
    assert.ok(Math.abs(lb.level - 2) < 0.1);
  });
});

describe('SlidingWindowCounter', () => {
  it('allows within limit', () => {
    let now = 0;
    const sw = new SlidingWindowCounter(1000, 5, () => now);
    for (let i = 0; i < 5; i++) assert.ok(sw.allow());
    assert.ok(!sw.allow()); // 6th denied
  });

  it('window slides — previous count decays', () => {
    let now = 0;
    const sw = new SlidingWindowCounter(1000, 10, () => now);
    // Fill 8 in first window
    for (let i = 0; i < 8; i++) sw.allow();
    // Move to next window midway (500ms in)
    now = 1500; // weight = 0.5, prev contributes 8*0.5=4
    // Should be able to add up to ~6 more
    let allowed = 0;
    for (let i = 0; i < 10; i++) {
      if (sw.allow()) allowed++;
    }
    assert.ok(allowed >= 5 && allowed <= 7, `Expected ~6, got ${allowed}`);
  });

  it('old windows fully decay', () => {
    let now = 0;
    const sw = new SlidingWindowCounter(1000, 5, () => now);
    for (let i = 0; i < 5; i++) sw.allow();
    now = 2500; // 2.5 windows later — fully decayed
    for (let i = 0; i < 5; i++) assert.ok(sw.allow());
  });
});

describe('FixedWindowCounter', () => {
  it('allows within limit per window', () => {
    let now = 0;
    const fw = new FixedWindowCounter(1000, 3, () => now);
    assert.ok(fw.allow());
    assert.ok(fw.allow());
    assert.ok(fw.allow());
    assert.ok(!fw.allow());
  });

  it('resets at window boundary', () => {
    let now = 0;
    const fw = new FixedWindowCounter(1000, 2, () => now);
    fw.allow(); fw.allow();
    assert.ok(!fw.allow());
    now = 1000; // new window
    assert.ok(fw.allow());
    assert.ok(fw.allow());
    assert.ok(!fw.allow());
  });

  it('remaining tracks correctly', () => {
    let now = 0;
    const fw = new FixedWindowCounter(1000, 5, () => now);
    assert.equal(fw.remaining, 5);
    fw.allow(); fw.allow();
    assert.equal(fw.remaining, 3);
  });
});

describe('PerKeyRateLimiter', () => {
  it('isolates keys', () => {
    const pkl = new PerKeyRateLimiter(
      () => new FixedWindowCounter(1000, 2, Date.now)
    );
    assert.ok(pkl.allow('a')); assert.ok(pkl.allow('a'));
    assert.ok(!pkl.allow('a'));
    // Key 'b' is independent
    assert.ok(pkl.allow('b')); assert.ok(pkl.allow('b'));
    assert.ok(!pkl.allow('b'));
    assert.equal(pkl.size, 2);
    pkl.close();
  });

  it('creates limiters lazily', () => {
    let created = 0;
    const pkl = new PerKeyRateLimiter(() => {
      created++;
      return new FixedWindowCounter(1000, 5, Date.now);
    });
    assert.equal(created, 0);
    pkl.allow('x');
    assert.equal(created, 1);
    pkl.allow('x'); // reuses
    assert.equal(created, 1);
    pkl.allow('y');
    assert.equal(created, 2);
    pkl.close();
  });
});
