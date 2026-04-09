// query-timeout.test.js
import { describe, test, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { CancelToken, QueryCancelledError, QueryTimeoutError, TimeoutManager, withTimeout } from './query-timeout.js';

describe('CancelToken', () => {
  test('starts not cancelled', () => {
    const token = new CancelToken();
    assert.equal(token.isCancelled, false);
    assert.equal(token.reason, null);
  });

  test('cancel sets state', () => {
    const token = new CancelToken();
    token.cancel('test reason');
    assert.equal(token.isCancelled, true);
    assert.equal(token.reason, 'test reason');
  });

  test('throwIfCancelled throws when cancelled', () => {
    const token = new CancelToken();
    token.throwIfCancelled(); // should not throw
    
    token.cancel();
    assert.throws(() => token.throwIfCancelled(), QueryCancelledError);
  });

  test('onCancel callback fires on cancel', () => {
    const token = new CancelToken();
    let called = false;
    token.onCancel(() => { called = true; });
    
    assert.ok(!called);
    token.cancel();
    assert.ok(called);
  });

  test('onCancel fires immediately if already cancelled', () => {
    const token = new CancelToken();
    token.cancel();
    
    let called = false;
    token.onCancel(() => { called = true; });
    assert.ok(called);
  });

  test('double cancel is idempotent', () => {
    const token = new CancelToken();
    let count = 0;
    token.onCancel(() => count++);
    
    token.cancel('first');
    token.cancel('second');
    assert.equal(count, 1);
    assert.equal(token.reason, 'first');
  });
});

describe('TimeoutManager', () => {
  let tm;

  beforeEach(() => { tm = new TimeoutManager(); });
  afterEach(() => { tm.destroy(); });

  test('start and end query', () => {
    const { queryId, token } = tm.startQuery('SELECT 1');
    assert.ok(queryId > 0);
    assert.ok(!token.isCancelled);
    
    const result = tm.endQuery(queryId);
    assert.ok(result.elapsed >= 0);
    assert.ok(!result.cancelled);
  });

  test('query timeout cancels token', async () => {
    const { queryId, token } = tm.startQuery('SELECT slow()', 50);
    
    await new Promise(r => setTimeout(r, 80));
    
    assert.ok(token.isCancelled);
    assert.ok(token.reason.includes('timeout'));
    tm.endQuery(queryId);
  });

  test('cancel specific query', () => {
    const { queryId, token } = tm.startQuery('SELECT 1');
    tm.cancelQuery(queryId, 'Manual cancel');
    
    assert.ok(token.isCancelled);
    assert.equal(token.reason, 'Manual cancel');
    tm.endQuery(queryId);
  });

  test('cancelAll cancels everything', () => {
    const q1 = tm.startQuery('SELECT 1');
    const q2 = tm.startQuery('SELECT 2');
    const q3 = tm.startQuery('SELECT 3');
    
    const count = tm.cancelAll();
    assert.equal(count, 3);
    assert.ok(q1.token.isCancelled);
    assert.ok(q2.token.isCancelled);
    assert.ok(q3.token.isCancelled);
  });

  test('getActiveQueries returns running queries', () => {
    tm.startQuery('SELECT 1', 5000);
    tm.startQuery('SELECT 2', 5000);
    
    const active = tm.getActiveQueries();
    assert.equal(active.length, 2);
    assert.ok(active[0].sql.includes('SELECT'));
    assert.ok(active[0].elapsedMs >= 0);
    
    tm.cancelAll();
  });

  test('ended queries removed from active list', () => {
    const { queryId } = tm.startQuery('SELECT 1');
    assert.equal(tm.getActiveQueries().length, 1);
    
    tm.endQuery(queryId);
    assert.equal(tm.getActiveQueries().length, 0);
  });

  test('stats tracking', () => {
    const q1 = tm.startQuery('SELECT 1');
    tm.endQuery(q1.queryId);
    
    const q2 = tm.startQuery('SELECT 2');
    tm.cancelQuery(q2.queryId);
    tm.endQuery(q2.queryId);
    
    const stats = tm.getStats();
    assert.equal(stats.totalQueries, 2);
    assert.equal(stats.totalCancellations, 1);
  });
});

describe('withTimeout', () => {
  test('completes before timeout', async () => {
    const result = await withTimeout(async (token) => {
      return 42;
    }, 1000);
    assert.equal(result, 42);
  });

  test('throws on timeout', async () => {
    await assert.rejects(async () => {
      await withTimeout(async (token) => {
        await new Promise(r => setTimeout(r, 200));
        return 'too slow';
      }, 50);
    }, QueryTimeoutError);
  });

  test('token is cancelled on timeout', async () => {
    let tokenRef = null;
    try {
      await withTimeout(async (token) => {
        tokenRef = token;
        await new Promise(r => setTimeout(r, 200));
      }, 50);
    } catch {}
    assert.ok(tokenRef.isCancelled);
  });
});
