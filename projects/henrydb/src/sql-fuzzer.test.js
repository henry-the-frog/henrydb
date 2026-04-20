// sql-fuzzer.test.js — Random SQL generation to find crashes
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomChoice(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function setupFuzzDB() {
  const db = new Database();
  db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, a INT, b TEXT, c FLOAT)');
  db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, x INT, y TEXT)');
  db.execute('CREATE INDEX idx_t1_a ON t1(a)');
  db.execute('CREATE INDEX idx_t2_x ON t2(x)');
  
  for (let i = 1; i <= 50; i++) {
    db.execute(`INSERT INTO t1 VALUES (${i}, ${i % 10}, 'str${i}', ${(i * 1.5).toFixed(2)})`);
    db.execute(`INSERT INTO t2 VALUES (${i}, ${((i - 1) % 50) + 1}, ${i * 100}, 'val${i}')`);
  }
  return db;
}

function generateRandomWhere() {
  const cols = ['id', 'a', 'b', 'c'];
  const ops = ['=', '>', '<', '>=', '<=', '!='];
  const col = randomChoice(cols);
  const op = randomChoice(ops);
  
  let val;
  if (col === 'b') {
    val = `'str${randomInt(1, 50)}'`;
  } else if (col === 'c') {
    val = (randomInt(1, 75) * 1.5).toFixed(2);
  } else {
    val = randomInt(0, 60);
  }
  
  return `${col} ${op} ${val}`;
}

function generateRandomOrderBy() {
  const cols = ['id', 'a', 'b', 'c'];
  const col = randomChoice(cols);
  const dir = randomChoice(['ASC', 'DESC']);
  return `${col} ${dir}`;
}

describe('SQL Fuzzer — Crash Detection', () => {
  it('random SELECT WHERE queries (100 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    for (let i = 0; i < 100; i++) {
      const where = generateRandomWhere();
      const sql = `SELECT * FROM t1 WHERE ${where}`;
      try {
        const r = db.execute(sql);
        assert.ok(Array.isArray(r.rows), `Query should return rows: ${sql}`);
      } catch (e) {
        // Expected errors (type mismatch, etc.) are OK
        if (e.message.includes('INTERNAL') || e.message.includes('Cannot read') || 
            e.message.includes('is not a function') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on: ${sql}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} queries caused internal errors`);
  });

  it('random SELECT with ORDER BY (50 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    for (let i = 0; i < 50; i++) {
      const orderBy = generateRandomOrderBy();
      const limit = randomInt(1, 20);
      const sql = `SELECT * FROM t1 ORDER BY ${orderBy} LIMIT ${limit}`;
      try {
        const r = db.execute(sql);
        assert.ok(r.rows.length <= limit, `Should respect LIMIT: ${sql}`);
      } catch (e) {
        if (e.message.includes('is not a function') || e.message.includes('Cannot read') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on: ${sql}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} queries caused internal errors`);
  });

  it('random JOINs (50 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    const joinTypes = ['JOIN', 'LEFT JOIN', 'RIGHT JOIN'];
    for (let i = 0; i < 50; i++) {
      const joinType = randomChoice(joinTypes);
      const where = randomChoice([
        `t1.a > ${randomInt(0, 10)}`,
        `t2.x > ${randomInt(0, 5000)}`,
        `t1.id = ${randomInt(1, 50)}`,
        ''
      ]);
      const sql = `SELECT t1.id, t2.x FROM t1 ${joinType} t2 ON t1.id = t2.t1_id${where ? ' WHERE ' + where : ''} LIMIT 20`;
      try {
        const r = db.execute(sql);
        assert.ok(Array.isArray(r.rows));
      } catch (e) {
        if (e.message.includes('is not a function') || e.message.includes('Cannot read') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on: ${sql}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} queries caused internal errors`);
  });

  it('random aggregations (50 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    const aggFuncs = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'];
    const aggCols = ['id', 'a', 'c'];
    for (let i = 0; i < 50; i++) {
      const func = randomChoice(aggFuncs);
      const col = randomChoice(aggCols);
      const groupBy = randomChoice(['', 'GROUP BY a', 'GROUP BY b']);
      const sql = `SELECT ${groupBy ? (groupBy.includes('a') ? 'a, ' : 'b, ') : ''}${func}(${col}) as agg FROM t1 ${groupBy}`;
      try {
        const r = db.execute(sql);
        assert.ok(r.rows.length > 0, `Aggregation should return rows: ${sql}`);
      } catch (e) {
        if (e.message.includes('is not a function') || e.message.includes('Cannot read') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on: ${sql}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} queries caused internal errors`);
  });

  it('random window functions (50 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    const winFuncs = ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE'];
    const winCols = ['a', 'id', 'c'];
    for (let i = 0; i < 50; i++) {
      const func = randomChoice(winFuncs);
      const col = randomChoice(winCols);
      const partBy = randomChoice(['', 'PARTITION BY a']);
      const args = func === 'NTILE' ? `(${randomInt(2, 5)})` : '()';
      const sql = `SELECT id, ${func}${args} OVER (${partBy} ORDER BY ${col}) as win FROM t1 LIMIT 20`;
      try {
        const r = db.execute(sql);
        assert.ok(r.rows.length > 0);
      } catch (e) {
        if (e.message.includes('is not a function') || e.message.includes('Cannot read') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on: ${sql}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} queries caused internal errors`);
  });

  it('random CTEs (30 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    for (let i = 0; i < 30; i++) {
      const where = generateRandomWhere();
      const sql = `WITH cte AS (SELECT * FROM t1 WHERE ${where}) SELECT COUNT(*) as cnt FROM cte`;
      try {
        const r = db.execute(sql);
        assert.ok(r.rows[0].cnt >= 0);
      } catch (e) {
        if (e.message.includes('is not a function') || e.message.includes('Cannot read') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on: ${sql}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} queries caused internal errors`);
  });

  it('random savepoint + rollback (20 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    for (let i = 0; i < 20; i++) {
      try {
        db.execute(`SAVEPOINT sp_${i}`);
        const op = randomChoice(['INSERT', 'UPDATE', 'DELETE']);
        if (op === 'INSERT') {
          db.execute(`INSERT INTO t1 VALUES (${1000 + i}, ${i}, 'fuzz${i}', ${i * 0.5})`);
        } else if (op === 'UPDATE') {
          db.execute(`UPDATE t1 SET a = ${i} WHERE id = ${randomInt(1, 50)}`);
        } else {
          db.execute(`DELETE FROM t1 WHERE id = ${randomInt(1, 50)}`);
        }
        db.execute(`ROLLBACK TO sp_${i}`);
      } catch (e) {
        if (e.message.includes('is not a function') || e.message.includes('Cannot read') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on savepoint iteration ${i}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} savepoint operations caused internal errors`);
  });

  it('random combined queries (30 iterations)', () => {
    const db = setupFuzzDB();
    let crashes = 0;
    for (let i = 0; i < 30; i++) {
      const subquery = `(SELECT ${randomChoice(['MAX', 'MIN', 'AVG'])}(a) FROM t1)`;
      const where = randomChoice([
        `a > ${subquery}`,
        `id IN (SELECT id FROM t1 WHERE a < 5)`,
        `EXISTS (SELECT 1 FROM t2 WHERE t2.t1_id = t1.id)`,
        generateRandomWhere()
      ]);
      const sql = `SELECT * FROM t1 WHERE ${where} LIMIT 10`;
      try {
        const r = db.execute(sql);
        assert.ok(Array.isArray(r.rows));
      } catch (e) {
        if (e.message.includes('is not a function') || e.message.includes('Cannot read') || e.message.includes('undefined')) {
          crashes++;
          console.log(`CRASH on: ${sql}\n  Error: ${e.message}`);
        }
      }
    }
    assert.equal(crashes, 0, `${crashes} queries caused internal errors`);
  });
});
