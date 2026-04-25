// session-b-features.test.js — Tests for Session B features (Apr 25, 2026)
// Tests: db.execute(sql, params), MEDIAN, PERCENTILE_CONT/DISC, GROUPS, EXCLUDE
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('db.execute(sql, params)', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'charlie', 35)");
  });

  it('SELECT with one param', () => {
    const r = db.execute('SELECT * FROM users WHERE id = $1', [1]);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'alice');
  });

  it('SELECT with two params', () => {
    const r = db.execute('SELECT * FROM users WHERE age > $1 AND age < $2', [25, 35]);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'alice');
  });

  it('INSERT with params', () => {
    db.execute('INSERT INTO users VALUES ($1, $2, $3)', [4, 'diana', 28]);
    const r = db.execute('SELECT * FROM users WHERE id = $1', [4]);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'diana');
  });

  it('works without params (backward compat)', () => {
    const r = db.execute('SELECT * FROM users WHERE id = 1');
    assert.strictEqual(r.rows.length, 1);
  });
});

describe('EXECUTE param count validation', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('PREPARE q AS SELECT * FROM t WHERE a = $1 AND b > $2');
  });

  it('throws when too few params', () => {
    assert.throws(() => db.execute('EXECUTE q(1)'), /requires 2 parameters/);
  });

  it('works with correct param count', () => {
    const r = db.execute('EXECUTE q(1, 5)');
    assert.strictEqual(r.rows.length, 1);
  });
});

describe('MEDIAN aggregate', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE scores (id INT, score INT)');
    db.execute('INSERT INTO scores VALUES (1, 10)');
    db.execute('INSERT INTO scores VALUES (2, 20)');
    db.execute('INSERT INTO scores VALUES (3, 30)');
    db.execute('INSERT INTO scores VALUES (4, 40)');
    db.execute('INSERT INTO scores VALUES (5, 50)');
  });

  it('MEDIAN of odd count', () => {
    const r = db.execute('SELECT MEDIAN(score) as med FROM scores');
    assert.strictEqual(r.rows[0].med, 30);
  });

  it('MEDIAN of even count', () => {
    db.execute('INSERT INTO scores VALUES (6, 60)');
    const r = db.execute('SELECT MEDIAN(score) as med FROM scores');
    assert.strictEqual(r.rows[0].med, 35); // (30+40)/2
    db.execute('DELETE FROM scores WHERE id = 6');
  });

  it('MEDIAN with GROUP BY', () => {
    db.execute('CREATE TABLE grouped (cat TEXT, val INT)');
    db.execute("INSERT INTO grouped VALUES ('a', 10)");
    db.execute("INSERT INTO grouped VALUES ('a', 20)");
    db.execute("INSERT INTO grouped VALUES ('a', 30)");
    db.execute("INSERT INTO grouped VALUES ('b', 100)");
    db.execute("INSERT INTO grouped VALUES ('b', 200)");
    const r = db.execute('SELECT cat, MEDIAN(val) as med FROM grouped GROUP BY cat ORDER BY cat');
    assert.strictEqual(r.rows[0].med, 20); // median of [10,20,30]
    assert.strictEqual(r.rows[1].med, 150); // median of [100,200]
  });
});

describe('GROUPS window frame', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE wt (id INT, val INT)');
    db.execute('INSERT INTO wt VALUES (1, 10)');
    db.execute('INSERT INTO wt VALUES (2, 20)');
    db.execute('INSERT INTO wt VALUES (3, 20)');
    db.execute('INSERT INTO wt VALUES (4, 30)');
    db.execute('INSERT INTO wt VALUES (5, 30)');
  });

  it('GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING', () => {
    const r = db.execute(
      'SELECT id, val, SUM(val) OVER (ORDER BY val GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as gs FROM wt'
    );
    // Group 0: [10], Group 1: [20,20], Group 2: [30,30]
    // id=1 (group 0): groups 0+1 → 10+20+20 = 50
    assert.strictEqual(r.rows[0].gs, 50);
    // id=2 (group 1): groups 0+1+2 → 10+20+20+30+30 = 110
    assert.strictEqual(r.rows[1].gs, 110);
    // id=4 (group 2): groups 1+2 → 20+20+30+30 = 100
    assert.strictEqual(r.rows[3].gs, 100);
  });
});

describe('EXCLUDE window clause', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE et (id INT, val INT)');
    db.execute('INSERT INTO et VALUES (1, 10)');
    db.execute('INSERT INTO et VALUES (2, 20)');
    db.execute('INSERT INTO et VALUES (3, 20)');
    db.execute('INSERT INTO et VALUES (4, 30)');
  });

  it('EXCLUDE CURRENT ROW', () => {
    const r = db.execute(
      'SELECT id, val, SUM(val) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE CURRENT ROW) as xs FROM et'
    );
    assert.strictEqual(r.rows[0].xs, 0);  // exclude self, sum of nothing
    assert.strictEqual(r.rows[1].xs, 10); // sum of [10] (excluded 20)
    assert.strictEqual(r.rows[2].xs, 30); // sum of [10,20] (excluded 20)
    assert.strictEqual(r.rows[3].xs, 50); // sum of [10,20,20] (excluded 30)
  });

  it('EXCLUDE TIES', () => {
    const r = db.execute(
      'SELECT id, val, COUNT(*) OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES) as xt FROM et'
    );
    // Row 1 (val=10): no ties, count=4
    assert.strictEqual(r.rows[0].xt, 4);
    // Row 2 (val=20): exclude row 3 (tie), count=3
    assert.strictEqual(r.rows[1].xt, 3);
    // Row 3 (val=20): exclude row 2 (tie), count=3
    assert.strictEqual(r.rows[2].xt, 3);
    // Row 4 (val=30): no ties, count=4
    assert.strictEqual(r.rows[3].xt, 4);
  });
});

describe('type affinity on INSERT', () => {
  it('TEXT column stores integers as strings', () => {
    const db = new Database();
    db.execute('CREATE TABLE ta (name TEXT)');
    db.execute('INSERT INTO ta VALUES (42)');
    const r = db.execute('SELECT * FROM ta');
    assert.strictEqual(typeof r.rows[0].name, 'string');
    assert.strictEqual(r.rows[0].name, '42');
  });

  it('INT column stores strings as integers when numeric', () => {
    const db = new Database();
    db.execute('CREATE TABLE tb (val INT)');
    db.execute("INSERT INTO tb VALUES ('123')");
    const r = db.execute('SELECT * FROM tb');
    assert.strictEqual(typeof r.rows[0].val, 'number');
    assert.strictEqual(r.rows[0].val, 123);
  });
});

describe('sqliteCompare in WHERE', () => {
  it('integer NOT > empty string (type class ordering)', () => {
    const db = new Database();
    db.execute('CREATE TABLE tc (a INT, b INT)');
    db.execute('INSERT INTO tc VALUES (1, 42)');
    db.execute('INSERT INTO tc VALUES (2, -10)');
    const r = db.execute("SELECT * FROM tc WHERE b > ''");
    assert.strictEqual(r.rows.length, 0);
  });
});
