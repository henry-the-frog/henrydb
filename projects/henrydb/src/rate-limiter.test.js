// rate-limiter.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { TokenBucket, SlidingWindowCounter, ConnectionRateLimiter } from './rate-limiter.js';

describe('TokenBucket', () => {
  test('allows consumption within capacity', () => {
    const bucket = new TokenBucket(10, 10);
    assert.ok(bucket.tryConsume(5));
    assert.ok(bucket.tryConsume(5));
  });

  test('rejects when empty', () => {
    const bucket = new TokenBucket(2, 1);
    assert.ok(bucket.tryConsume(2));
    assert.ok(!bucket.tryConsume(1));
  });

  test('refills over time', async () => {
    const bucket = new TokenBucket(10, 100); // 100/sec refill
    bucket.tryConsume(10);
    assert.ok(!bucket.tryConsume(1));
    
    await new Promise(r => setTimeout(r, 50));
    assert.ok(bucket.tryConsume(1));
  });

  test('waitTime returns correct estimate', () => {
    const bucket = new TokenBucket(10, 10);
    assert.equal(bucket.waitTime(1), 0); // Has tokens
    
    bucket.tryConsume(10);
    const wait = bucket.waitTime(1);
    assert.ok(wait > 0);
    assert.ok(wait < 200); // Should need ~100ms for 1 token at 10/sec
  });
});

describe('SlidingWindowCounter', () => {
  test('counts events in window', () => {
    const counter = new SlidingWindowCounter(1000);
    counter.add();
    counter.add();
    counter.add();
    assert.equal(counter.count(), 3);
  });

  test('events expire outside window', async () => {
    const counter = new SlidingWindowCounter(50);
    counter.add();
    assert.equal(counter.count(), 1);
    
    await new Promise(r => setTimeout(r, 60));
    assert.equal(counter.count(), 0);
  });
});

describe('ConnectionRateLimiter', () => {
  let limiter;

  beforeEach(() => {
    limiter = new ConnectionRateLimiter({
      maxConnectionsPerIP: 3,
      maxQueriesPerSecond: 10,
      maxConnectionRate: 5,
      slowlorisTimeoutMs: 100,
      globalMaxConnections: 10,
    });
  });

  test('allows connection within limits', () => {
    const result = limiter.allowConnection('10.0.0.1');
    assert.ok(result.allowed);
  });

  test('rejects when per-IP limit reached', () => {
    limiter.registerConnection('10.0.0.1', 'c1');
    limiter.registerConnection('10.0.0.1', 'c2');
    limiter.registerConnection('10.0.0.1', 'c3');
    
    const result = limiter.allowConnection('10.0.0.1');
    assert.ok(!result.allowed);
    assert.equal(result.reason, 'per_ip_limit');
  });

  test('rejects when global limit reached', () => {
    for (let i = 0; i < 10; i++) {
      limiter.registerConnection(`10.0.0.${i}`, `c${i}`);
    }
    const result = limiter.allowConnection('10.0.0.99');
    assert.ok(!result.allowed);
    assert.equal(result.reason, 'global_limit');
  });

  test('removal frees up slots', () => {
    limiter.registerConnection('10.0.0.1', 'c1');
    limiter.registerConnection('10.0.0.1', 'c2');
    limiter.registerConnection('10.0.0.1', 'c3');
    
    limiter.removeConnection('10.0.0.1', 'c1');
    const result = limiter.allowConnection('10.0.0.1');
    assert.ok(result.allowed);
  });

  test('QPS limiting', () => {
    for (let i = 0; i < 10; i++) {
      assert.ok(limiter.allowQuery('10.0.0.1'));
    }
    assert.ok(!limiter.allowQuery('10.0.0.1')); // 11th rejected
  });

  test('slowloris detection', async () => {
    limiter.registerConnection('10.0.0.1', 'slow1');
    limiter.registerConnection('10.0.0.1', 'slow2');
    
    await new Promise(r => setTimeout(r, 120));
    
    const killed = limiter.detectSlowloris();
    assert.equal(killed.length, 2);
    assert.equal(limiter.getStats().activeConnections, 0);
  });

  test('activity prevents slowloris kill', async () => {
    limiter.registerConnection('10.0.0.1', 'active');
    
    await new Promise(r => setTimeout(r, 50));
    limiter.recordActivity('active');
    
    await new Promise(r => setTimeout(r, 60));
    const killed = limiter.detectSlowloris();
    assert.equal(killed.length, 0);
  });

  test('different IPs independent', () => {
    limiter.registerConnection('10.0.0.1', 'c1');
    limiter.registerConnection('10.0.0.1', 'c2');
    limiter.registerConnection('10.0.0.1', 'c3');
    
    // Different IP should still be allowed
    const result = limiter.allowConnection('10.0.0.2');
    assert.ok(result.allowed);
  });

  test('stats tracking', () => {
    limiter.allowConnection('10.0.0.1');
    limiter.registerConnection('10.0.0.1', 'c1');
    
    const stats = limiter.getStats();
    assert.equal(stats.totalAllowed, 1);
    assert.equal(stats.activeConnections, 1);
    assert.equal(stats.uniqueIPs, 1);
  });

  test('getIPInfo', () => {
    limiter.registerConnection('10.0.0.1', 'c1');
    limiter.registerConnection('10.0.0.1', 'c2');
    limiter.allowQuery('10.0.0.1');
    
    const info = limiter.getIPInfo('10.0.0.1');
    assert.equal(info.connections, 2);
    assert.equal(info.queriesInLastSecond, 1);
  });
});
