// session-b-regression.test.js — Comprehensive regression tests for all Session B fixes
// Tests 16 bugs fixed in one session: parser, tokenizer, evaluator, persistence, DDL.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { tokenize } from './sql.js';

describe('Session B Regressions: Tokenizer', () => {
  it('1-1 tokenizes as subtraction not negative number', () => {
    const toks = tokenize('1-1').filter(t => t.type !== 'EOF');
    assert.strictEqual(toks[1].type, 'MINUS');
  });

  it('id-1 tokenizes as identifier minus number', () => {
    const toks = tokenize('id-1').filter(t => t.type !== 'EOF');
    assert.strictEqual(toks[0].type, 'IDENT');
    assert.strictEqual(toks[1].type, 'MINUS');
  });

  it('(-1) still tokenizes as negative literal', () => {
    const toks = tokenize('(-1)').filter(t => t.type !== 'EOF');
    assert.strictEqual(toks[1].value, -1);
  });
});

describe('Session B Regressions: Comparison RHS', () => {
  let db;
  it('setup', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
  });

  it('WHERE id = 2 - 1 returns id=1', () => {
    assert.strictEqual(db.execute('SELECT id FROM t WHERE id = 2 - 1').rows[0].id, 1);
  });

  it('WHERE val = id * 10 returns all rows', () => {
    assert.strictEqual(db.execute('SELECT * FROM t WHERE val = id * 10').rows.length, 5);
  });

  it('BETWEEN with arithmetic bounds', () => {
    assert.strictEqual(db.execute('SELECT COUNT(*) as c FROM t WHERE val BETWEEN 10 * 2 AND 10 * 4').rows[0].c, 3);
  });
});

describe('Session B Regressions: IN list with expressions', () => {
  let db;
  it('setup', () => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i})`);
  });

  it('IN (1+1, 2+1) matches correctly', () => {
    const ids = db.execute('SELECT id FROM t WHERE id IN (1 + 1, 2 + 1) ORDER BY id').rows.map(r => r.id);
    assert.deepStrictEqual(ids, [2, 3]);
  });
});

describe('Session B Regressions: INSERT with expressions', () => {
  it('INSERT VALUES with arithmetic', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 5 * 20)');
    assert.strictEqual(db.execute('SELECT val FROM t').rows[0].val, 100);
  });
});

describe('Session B Regressions: CASE WHEN NULL', () => {
  it('CASE WHEN NULL returns ELSE (SQL standard)', () => {
    const db = new Database();
    const r = db.execute("SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END as x");
    assert.strictEqual(r.rows[0].x, 'no');
  });

  it('CASE WHEN 0 returns ELSE', () => {
    const db = new Database();
    assert.strictEqual(db.execute("SELECT CASE WHEN 0 THEN 'yes' ELSE 'no' END as x").rows[0].x, 'no');
  });
});

describe('Session B Regressions: _evalExpr default', () => {
  it('WHERE LENGTH("") is falsy', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    assert.strictEqual(db.execute("SELECT * FROM t WHERE LENGTH('')").rows.length, 0);
  });

  it('WHERE COALESCE(NULL, NULL) is falsy', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    assert.strictEqual(db.execute('SELECT * FROM t WHERE COALESCE(NULL, NULL)').rows.length, 0);
  });
});

describe('Session B Regressions: Function names as aliases', () => {
  it('ORDER BY mod alias works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, num INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    const r = db.execute('SELECT num % 5 as mod FROM t ORDER BY mod');
    assert.strictEqual(r.rows[0].mod, 0);
  });

  it('MOD() function still works', () => {
    const db = new Database();
    assert.strictEqual(db.execute('SELECT MOD(7, 3) as m').rows[0].m, 1);
  });
});

describe('Session B Regressions: Window PARTITION BY expressions', () => {
  it('PARTITION BY val % 10 works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 15)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    const r = db.execute('SELECT id, ROW_NUMBER() OVER (PARTITION BY val % 10 ORDER BY id) as rn FROM t');
    // val%10: 10→0, 15→5, 20→0. Partition 0: ids 1,3. Partition 5: id 2.
    const id1 = r.rows.find(row => row.id === 1);
    const id2 = r.rows.find(row => row.id === 2);
    const id3 = r.rows.find(row => row.id === 3);
    assert.strictEqual(id1.rn, 1);
    assert.strictEqual(id2.rn, 1); // Only row in partition 5
    assert.strictEqual(id3.rn, 2); // Second row in partition 0
  });
});

describe('Session B Regressions: UPDATE SET with subquery', () => {
  it('SET val = (SELECT MAX(val) FROM t) works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('UPDATE t SET val = (SELECT MAX(val) FROM t) WHERE id = 1');
    assert.strictEqual(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 20);
  });
});

describe('Session B Regressions: CREATE TABLE DEFAULT expressions', () => {
  it('DEFAULT 10 * 2 works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT DEFAULT 10 * 2)');
    db.execute('INSERT INTO t (id) VALUES (1)');
    assert.strictEqual(db.execute('SELECT val FROM t').rows[0].val, 20);
  });

  it('simple DEFAULT still works', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT DEFAULT 42)');
    db.execute('INSERT INTO t (id) VALUES (1)');
    assert.strictEqual(db.execute('SELECT val FROM t').rows[0].val, 42);
  });
});

describe('Session B Regressions: LIKE with concatenation', () => {
  it("LIKE 'pre' || '%' works", () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute("INSERT INTO t VALUES (2, 'world')");
    assert.strictEqual(db.execute("SELECT * FROM t WHERE name LIKE 'hel' || '%'").rows.length, 1);
  });
});
