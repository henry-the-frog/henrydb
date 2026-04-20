// views.test.js — VIEW creation and querying tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Views', () => {
  it('CREATE VIEW and SELECT from it', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT, category TEXT)');
    db.execute("INSERT INTO t VALUES (1,100,'A'),(2,200,'B'),(3,300,'A')");
    db.execute('CREATE VIEW v AS SELECT * FROM t WHERE val > 150');
    const r = db.execute('SELECT * FROM v ORDER BY id');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].val, 200);
  });

  it('VIEW with aggregation', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (category TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('A',100),('A',200),('B',150)");
    db.execute('CREATE VIEW sales_summary AS SELECT category, SUM(amount) as total FROM sales GROUP BY category');
    const r = db.execute('SELECT * FROM sales_summary ORDER BY category');
    assert.equal(r.rows[0].total, 300);
    assert.equal(r.rows[1].total, 150);
  });

  it('VIEW reflects underlying table changes', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1,10)');
    db.execute('CREATE VIEW v AS SELECT * FROM t');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM v').rows[0].c, 1);
    db.execute('INSERT INTO t VALUES (2,20)');
    assert.equal(db.execute('SELECT COUNT(*) as c FROM v').rows[0].c, 2);
  });

  it('DROP VIEW', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT)');
    db.execute('CREATE VIEW v AS SELECT * FROM t');
    db.execute('DROP VIEW v');
    assert.throws(() => db.execute('SELECT * FROM v'));
  });

  it('VIEW with JOIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, name TEXT)');
    db.execute('CREATE TABLE b (id INT, a_id INT, val TEXT)');
    db.execute("INSERT INTO a VALUES (1,'alice')");
    db.execute("INSERT INTO b VALUES (1,1,'x')");
    db.execute('CREATE VIEW joined AS SELECT a.name, b.val FROM a JOIN b ON a.id = b.a_id');
    const r = db.execute('SELECT * FROM joined');
    assert.equal(r.rows[0].name, 'alice');
    assert.equal(r.rows[0].val, 'x');
  });
});
