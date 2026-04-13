// session-b-regression-fuzzer.test.js — Targeted fuzzer for today's new features
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Session B regression fuzzer', () => {
  
  it('1000 random window function queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, cat TEXT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, 'cat${i % 5}', ${Math.floor(Math.random() * 1000)})`);
    
    const funcs = ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'SUM', 'AVG', 'MIN', 'MAX', 'COUNT'];
    let crashes = 0;
    
    for (let trial = 0; trial < 1000; trial++) {
      const func = funcs[trial % funcs.length];
      const hasPartition = trial % 3 === 0;
      const hasOrder = trial % 2 === 0;
      
      let overClause = '(';
      if (hasPartition) overClause += 'PARTITION BY cat ';
      if (hasOrder) overClause += 'ORDER BY val';
      if (!hasPartition && !hasOrder) overClause += 'ORDER BY id';
      overClause += ')';
      
      const arg = ['ROW_NUMBER', 'RANK', 'DENSE_RANK'].includes(func) ? '' : (func === 'COUNT' ? '*' : 'val');
      const sql = `SELECT id, ${func}(${arg}) OVER ${overClause} as w FROM t LIMIT 5`;
      
      try {
        const r = db.execute(sql);
        assert.ok(r.rows.length <= 5);
      } catch (e) {
        if (!e.message.includes('parse') && !e.message.includes('syntax')) crashes++;
      }
    }
    assert.strictEqual(crashes, 0, `${crashes} crashes in 1000 window function queries`);
  });

  it('500 random CTE queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    
    let crashes = 0;
    for (let trial = 0; trial < 500; trial++) {
      const n = (trial % 50) + 1;
      const sql = `WITH cte AS (SELECT * FROM t WHERE id <= ${n}) SELECT COUNT(*) as cnt FROM cte`;
      try {
        const r = db.execute(sql);
        assert.strictEqual(r.rows[0].cnt, n);
      } catch (e) {
        crashes++;
      }
    }
    assert.strictEqual(crashes, 0);
  });

  it('500 random transaction operations', () => {
    const db = new Database();
    db.execute('CREATE TABLE counter (id INT, val INT)');
    db.execute('INSERT INTO counter VALUES (1, 0)');
    
    let expected = 0;
    let crashes = 0;
    
    for (let trial = 0; trial < 500; trial++) {
      const shouldCommit = trial % 3 !== 0;
      try {
        db.execute('BEGIN');
        db.execute('UPDATE counter SET val = val + 1 WHERE id = 1');
        if (shouldCommit) {
          db.execute('COMMIT');
          expected++;
        } else {
          db.execute('ROLLBACK');
        }
      } catch (e) {
        crashes++;
      }
    }
    
    const actual = db.execute('SELECT val FROM counter WHERE id = 1').rows[0].val;
    assert.strictEqual(actual, expected, `expected ${expected}, got ${actual}`);
    assert.strictEqual(crashes, 0);
  });

  it('200 random HAVING queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (cat TEXT, val INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES ('cat${i % 10}', ${i})`);
    
    let crashes = 0;
    for (let trial = 0; trial < 200; trial++) {
      const threshold = trial * 5;
      try {
        const r = db.execute(`SELECT cat, SUM(val) as total FROM t GROUP BY cat HAVING SUM(val) > ${threshold}`);
        for (const row of r.rows) {
          assert.ok(row.total > threshold, `${row.cat}: ${row.total} should be > ${threshold}`);
        }
      } catch (e) {
        crashes++;
      }
    }
    assert.strictEqual(crashes, 0);
  });

  it('300 random join queries with ANALYZE', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, val INT)');
    db.execute('CREATE TABLE b (id INT, a_id INT, data TEXT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO a VALUES (${i}, ${i * 10})`);
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO b VALUES (${i}, ${(i % 50) + 1}, 'data${i}')`);
    db.execute('ANALYZE TABLE a');
    db.execute('ANALYZE TABLE b');
    
    let crashes = 0;
    for (let trial = 0; trial < 300; trial++) {
      const minVal = trial * 3;
      try {
        const r = db.execute(`SELECT a.val, b.data FROM a JOIN b ON a.id = b.a_id WHERE a.val > ${minVal} LIMIT 10`);
        assert.ok(r.rows.length <= 10);
        for (const row of r.rows) {
          assert.ok(row.val > minVal);
        }
      } catch (e) {
        crashes++;
      }
    }
    assert.strictEqual(crashes, 0);
  });
});
