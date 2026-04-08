// prepared-cache.test.js — Tests for prepared statement cache with compiled execution
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PreparedQueryCache } from './prepared-cache.js';
import { Database } from './db.js';

function setupDB(n = 500) {
  const db = new Database();
  db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, region TEXT, score INT)');
  db.execute('CREATE TABLE events (id INT PRIMARY KEY, user_id INT, type TEXT, ts INT)');

  for (let i = 0; i < n; i++) {
    const region = ['US', 'EU', 'APAC', 'LATAM'][i % 4];
    db.execute(`INSERT INTO users VALUES (${i}, 'User ${i}', '${region}', ${(i * 37) % 100})`);
  }
  for (let i = 0; i < n * 2; i++) {
    const type = ['click', 'view', 'purchase'][i % 3];
    db.execute(`INSERT INTO events VALUES (${i}, ${i % n}, '${type}', ${1000000 + i})`);
  }

  return db;
}

describe('PreparedQueryCache', () => {
  
  it('prepare and execute single-table query', () => {
    const db = setupDB(200);
    const cache = new PreparedQueryCache(db);

    cache.prepare('q1', {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'users' },
    });

    const result = cache.execute('q1');
    assert.ok(result);
    assert.equal(result.rows.length, 200);
    assert.equal(result.execCount, 1);
  });

  it('repeat execution uses cached path', () => {
    const db = setupDB(300);
    const cache = new PreparedQueryCache(db);

    cache.prepare('q1', {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'users' },
    });

    // Execute 5 times
    for (let i = 0; i < 5; i++) {
      const result = cache.execute('q1');
      assert.equal(result.rows.length, 300);
      assert.equal(result.execCount, i + 1);
    }

    assert.equal(cache.stats.executeCount, 5);
    assert.equal(cache.stats.cacheHits, 5);
  });

  it('join query prepared and cached', () => {
    const db = setupDB(200);
    const cache = new PreparedQueryCache(db);

    const prepared = cache.prepare('join_q', {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'users', alias: 'u' },
      joins: [{
        table: 'events',
        alias: 'e',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'u', name: 'id' },
          right: { type: 'column_ref', table: 'e', name: 'user_id' }
        }
      }],
      limit: { value: 50 }
    });

    assert.ok(prepared.engine);

    const result = cache.execute('join_q');
    assert.equal(result.rows.length, 50);
  });

  it('deallocate removes from cache', () => {
    const db = setupDB(100);
    const cache = new PreparedQueryCache(db);

    cache.prepare('q1', {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'users' },
    });

    cache.deallocate('q1');

    assert.throws(() => cache.execute('q1'), /not found/);
  });

  it('list shows all prepared statements', () => {
    const db = setupDB(100);
    const cache = new PreparedQueryCache(db);

    cache.prepare('q1', { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'users' } });
    cache.prepare('q2', { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'events' } });

    cache.execute('q1');
    cache.execute('q1');
    cache.execute('q2');

    const list = cache.list();
    assert.equal(list.length, 2);
    assert.equal(list.find(e => e.name === 'q1').execCount, 2);
    assert.equal(list.find(e => e.name === 'q2').execCount, 1);
  });

  it('benchmark: prepared vs unprepared execution', () => {
    const db = setupDB(500);
    const cache = new PreparedQueryCache(db);

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'users', alias: 'u' },
      joins: [{
        table: 'events',
        alias: 'e',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'u', name: 'id' },
          right: { type: 'column_ref', table: 'e', name: 'user_id' }
        }
      }],
      limit: { value: 200 }
    };

    // Prepare (includes warmup)
    cache.prepare('bench', ast);

    // Execute 5 times (cached path)
    const times = [];
    for (let i = 0; i < 5; i++) {
      const t0 = Date.now();
      const result = cache.execute('bench');
      times.push(Date.now() - t0);
      assert.equal(result.rows.length, 200);
    }

    // Compare with standard execution
    const t1 = Date.now();
    db.execute('SELECT * FROM users u JOIN events e ON u.id = e.user_id LIMIT 200');
    const volcanoMs = Date.now() - t1;

    const avgCachedMs = times.reduce((a, b) => a + b, 0) / times.length;
    console.log(`    Prepared (avg of 5): ${avgCachedMs.toFixed(1)}ms vs Volcano: ${volcanoMs}ms (${(volcanoMs / Math.max(avgCachedMs, 0.1)).toFixed(1)}x)`);
  });

  it('stats track everything', () => {
    const db = setupDB(100);
    const cache = new PreparedQueryCache(db);

    cache.prepare('q1', { type: 'SELECT', columns: [{ name: '*' }], from: { table: 'users' } });
    cache.execute('q1');
    cache.execute('q1');

    const stats = cache.getStats();
    assert.equal(stats.prepareCount, 1);
    assert.equal(stats.executeCount, 2);
    assert.equal(stats.cacheHits, 2);
    assert.equal(stats.cacheSize, 1);
    assert.ok(stats.engineStats);
  });
});
