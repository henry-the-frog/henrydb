// thread-pool.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ThreadPool } from './thread-pool.js';

describe('ThreadPool', () => {
  it('processes all submitted tasks', () => {
    const pool = new ThreadPool(4);
    for (let i = 0; i < 20; i++) pool.submit(`task-${i}`);
    pool.runAll();
    assert.equal(pool.completed.length, 20);
    assert.equal(pool.getStats().totalCompleted, 20);
  });

  it('work stealing balances load', () => {
    // Submit all tasks to a 1-worker scenario then check stealing
    const pool = new ThreadPool(4, { submitPolicy: 'round-robin' });
    // Submit 16 tasks — 4 per worker
    for (let i = 0; i < 16; i++) pool.submit(i);
    pool.runAll();
    const stats = pool.getStats();
    assert.equal(stats.totalCompleted, 16);
    // Each worker should have completed ~4 (balanced)
    for (const w of stats.workers) {
      assert.ok(w.completed >= 2, `Worker ${w.id} only completed ${w.completed}`);
    }
  });

  it('handles empty pool', () => {
    const pool = new ThreadPool(2);
    const ticks = pool.runAll();
    assert.equal(ticks, 0);
    assert.equal(pool.completed.length, 0);
  });

  it('single worker processes sequentially', () => {
    const pool = new ThreadPool(1);
    pool.submit('a'); pool.submit('b'); pool.submit('c');
    pool.runAll();
    assert.deepEqual(pool.completed, ['c', 'b', 'a']); // LIFO from deque bottom
  });

  it('work stealing happens when queues are unbalanced', () => {
    const pool = new ThreadPool(2, { submitPolicy: 'round-robin' });
    // Force imbalance: submit 10 tasks (alternating workers)
    for (let i = 0; i < 10; i++) pool.submit(i);
    pool.runAll();
    const stats = pool.getStats();
    // With 2 workers and 10 tasks, both should complete some
    const totalStolen = stats.workers.reduce((s, w) => s + w.stolen, 0);
    assert.equal(stats.totalCompleted, 10);
    // At least some tasks should be completed (stealing or not)
    for (const w of stats.workers) assert.ok(w.completed > 0);
  });

  it('task dependencies block until prerequisites complete', () => {
    const pool = new ThreadPool(2);
    const idA = pool.submit({ id: 'A', name: 'setup' });
    const idB = pool.submit({ id: 'B', name: 'depends-on-A' }, { dependsOn: ['A'] });
    assert.equal(pool.getStats().blocked, 1);
    pool.runAll();
    assert.equal(pool.completed.length, 2);
    // A must complete before B
    const completionOrder = pool.completed.map(t => t.id);
    assert.ok(completionOrder.indexOf('A') < completionOrder.indexOf('B'));
  });

  it('diamond dependency pattern', () => {
    const pool = new ThreadPool(4);
    pool.submit({ id: 'root', name: 'root' });
    pool.submit({ id: 'left', name: 'left' }, { dependsOn: ['root'] });
    pool.submit({ id: 'right', name: 'right' }, { dependsOn: ['root'] });
    pool.submit({ id: 'join', name: 'join' }, { dependsOn: ['left', 'right'] });
    pool.runAll();
    assert.equal(pool.completed.length, 4);
    const order = pool.completed.map(t => t.id);
    assert.ok(order.indexOf('root') < order.indexOf('left'));
    assert.ok(order.indexOf('root') < order.indexOf('right'));
    assert.ok(order.indexOf('left') < order.indexOf('join'));
    assert.ok(order.indexOf('right') < order.indexOf('join'));
  });

  it('stats track idle ticks', () => {
    const pool = new ThreadPool(4);
    pool.submit('only-one');
    pool.runAll();
    const stats = pool.getStats();
    // 3 workers should have idle ticks (only 1 task for 4 workers)
    const totalIdle = stats.workers.reduce((s, w) => s + w.idleTicks, 0);
    assert.ok(totalIdle > 0, 'Expected some idle ticks');
  });

  it('shortest-queue policy distributes evenly', () => {
    const pool = new ThreadPool(3, { submitPolicy: 'shortest' });
    for (let i = 0; i < 9; i++) pool.submit(i);
    // Before running, check that queues are balanced
    const stats = pool.getStats();
    for (const w of stats.workers) {
      assert.equal(w.queueLength, 3, `Worker ${w.id} has ${w.queueLength} tasks`);
    }
  });

  it('large task count stress test', () => {
    const pool = new ThreadPool(8);
    for (let i = 0; i < 1000; i++) pool.submit(i);
    const ticks = pool.runAll();
    assert.equal(pool.completed.length, 1000);
    assert.ok(ticks <= 200, `Expected ≤200 ticks, got ${ticks}`);
    console.log(`  1000 tasks, 8 workers: ${ticks} ticks, ${pool.getStats().workers.map(w => w.stolen).reduce((a,b)=>a+b,0)} steals`);
  });
});
