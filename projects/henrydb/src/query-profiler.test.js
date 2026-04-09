// query-profiler.test.js — Tests for query profiler
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Query Profiler (db.profile)', () => {
  function setupDB() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    for (let i = 1; i <= 1000; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'User ${i}', ${20 + i % 50})`);
    }
    return db;
  }

  it('profile returns result and timing', () => {
    const db = setupDB();
    const { result, profile } = db.profile('SELECT * FROM users WHERE age = 30');
    
    assert.ok(result.rows.length > 0);
    assert.ok(profile.totalMs >= 0);
    assert.ok(profile.phases.length >= 2); // PARSE + EXECUTE
    assert.ok(profile.formatted.includes('PARSE'));
    assert.ok(profile.formatted.includes('EXECUTE'));
    assert.ok(profile.formatted.includes('TOTAL'));
  });

  it('profile shows PARSE time', () => {
    const db = setupDB();
    const { profile } = db.profile('SELECT * FROM users WHERE age = 25');
    
    const parsePhase = profile.phases.find(p => p.name === 'PARSE');
    assert.ok(parsePhase);
    assert.ok(parsePhase.durationMs >= 0);
  });

  it('profile shows cached parse for repeated queries', () => {
    const db = setupDB();
    
    // First execution: not cached
    const { profile: p1 } = db.profile('SELECT * FROM users WHERE age = 25');
    assert.equal(p1.phases[0].cached, false);
    
    // Second execution: cached
    const { profile: p2 } = db.profile('SELECT * FROM users WHERE age = 25');
    assert.equal(p2.phases[0].cached, true);
    assert.ok(p2.phases[0].durationMs <= p1.phases[0].durationMs + 1); // Cached should be faster
  });

  it('profile works with complex queries', () => {
    const db = setupDB();
    db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)');
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${i % 100 + 1}, ${i * 10})`);
    }
    
    const { result, profile } = db.profile(
      'SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total DESC LIMIT 10'
    );
    
    assert.equal(result.rows.length, 10);
    assert.ok(profile.totalMs > 0);
    
    console.log('\n  Profile output:\n' + profile.formatted.split('\n').map(l => '    ' + l).join('\n'));
  });

  it('profile formatted output is readable', () => {
    const db = setupDB();
    const { profile } = db.profile('SELECT * FROM users LIMIT 5');
    
    assert.ok(profile.formatted.includes('Query:'));
    assert.ok(profile.formatted.includes('Duration'));
    assert.ok(profile.formatted.includes('TOTAL'));
    // Should contain percentages
    assert.ok(profile.formatted.includes('%'));
  });

  it('profile with INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    
    const { result, profile } = db.profile("INSERT INTO t VALUES (1, 'hello')");
    assert.ok(profile.totalMs >= 0);
  });

  it('profile with aggregation', () => {
    const db = setupDB();
    const { result, profile } = db.profile('SELECT age, COUNT(*) as cnt FROM users GROUP BY age ORDER BY cnt DESC LIMIT 5');
    
    assert.equal(result.rows.length, 5);
    assert.ok(profile.phases.find(p => p.name === 'EXECUTE').rows === 5);
  });

  it('profile benchmark: 100 profiled queries', () => {
    const db = setupDB();
    
    const t0 = performance.now();
    for (let i = 0; i < 100; i++) {
      db.profile(`SELECT * FROM users WHERE id = ${i + 1}`);
    }
    const elapsed = performance.now() - t0;
    
    console.log(`  100 profiled queries: ${elapsed.toFixed(1)}ms (${(elapsed/100).toFixed(3)}ms avg)`);
    // Profiling overhead should be minimal
    
    const t1 = performance.now();
    for (let i = 0; i < 100; i++) {
      db.execute(`SELECT * FROM users WHERE id = ${i + 1}`);
    }
    const normalElapsed = performance.now() - t1;
    
    const overhead = ((elapsed - normalElapsed) / normalElapsed * 100).toFixed(1);
    console.log(`  Profiling overhead: ${overhead}% (${normalElapsed.toFixed(1)}ms without)`);
  });
});
