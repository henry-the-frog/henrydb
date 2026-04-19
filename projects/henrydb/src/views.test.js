// views.test.js — Views and materialized views

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Views', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 'Eng', 80000)");
    db.execute("INSERT INTO emp VALUES (2, 'Bob', 'Sales', 60000)");
    db.execute("INSERT INTO emp VALUES (3, 'Charlie', 'Eng', 90000)");
    db.execute("INSERT INTO emp VALUES (4, 'Diana', 'Sales', 70000)");
  });

  it('CREATE VIEW and SELECT', () => {
    db.execute("CREATE VIEW eng AS SELECT * FROM emp WHERE dept = 'Eng'");
    const r = db.execute('SELECT * FROM eng ORDER BY salary');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Alice');
  });

  it('view reflects underlying data changes', () => {
    db.execute("CREATE VIEW eng AS SELECT * FROM emp WHERE dept = 'Eng'");
    db.execute("INSERT INTO emp VALUES (5, 'Eve', 'Eng', 95000)");
    const r = db.execute('SELECT COUNT(*) as cnt FROM eng');
    assert.equal(r.rows[0].cnt, 3);
  });

  it('aggregate view', () => {
    db.execute('CREATE VIEW stats AS SELECT dept, COUNT(*) as cnt, SUM(salary) as total FROM emp GROUP BY dept');
    const r = db.execute('SELECT * FROM stats ORDER BY dept');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].dept, 'Eng');
    assert.equal(r.rows[0].cnt, 2);
  });

  it('view of view', () => {
    db.execute("CREATE VIEW eng AS SELECT * FROM emp WHERE dept = 'Eng'");
    db.execute('CREATE VIEW rich_eng AS SELECT * FROM eng WHERE salary > 85000');
    const r = db.execute('SELECT name FROM rich_eng');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Charlie');
  });

  it('JOIN with view', () => {
    db.execute('CREATE VIEW dept_avg AS SELECT dept, AVG(salary) as avg_sal FROM emp GROUP BY dept');
    const r = db.execute('SELECT e.name, d.avg_sal FROM emp e JOIN dept_avg d ON e.dept = d.dept WHERE e.name = \'Alice\'');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].avg_sal, 85000);
  });

  it('DROP VIEW', () => {
    db.execute("CREATE VIEW eng AS SELECT * FROM emp WHERE dept = 'Eng'");
    db.execute('DROP VIEW eng');
    assert.throws(() => db.execute('SELECT * FROM eng'), /not found|does not exist/i);
  });

  it('CREATE OR REPLACE VIEW', () => {
    db.execute("CREATE VIEW v AS SELECT name FROM emp WHERE dept = 'Eng'");
    assert.equal(db.execute('SELECT * FROM v').rows.length, 2);
    
    db.execute("CREATE OR REPLACE VIEW v AS SELECT name FROM emp WHERE dept = 'Sales'");
    assert.equal(db.execute('SELECT * FROM v').rows.length, 2);
    assert.equal(db.execute("SELECT name FROM v WHERE name = 'Bob'").rows.length, 1);
  });
});

describe('Materialized Views', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES (1, 'East', 100), (2, 'West', 200), (3, 'East', 150)");
  });

  it('CREATE MATERIALIZED VIEW', () => {
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT region, SUM(amount) as total FROM sales GROUP BY region');
    const r = db.execute('SELECT * FROM mv ORDER BY region');
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].total, 250); // East
    assert.equal(r.rows[1].total, 200); // West
  });

  it('materialized view is snapshot — does not reflect changes', () => {
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) as cnt FROM sales');
    db.execute("INSERT INTO sales VALUES (4, 'North', 300)");
    // Without REFRESH, still shows old data
    const r = db.execute('SELECT cnt FROM mv');
    assert.equal(r.rows[0].cnt, 3); // Old count
  });

  it('REFRESH MATERIALIZED VIEW', () => {
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT region, SUM(amount) as total FROM sales GROUP BY region');
    db.execute("INSERT INTO sales VALUES (4, 'East', 50)");
    db.execute('REFRESH MATERIALIZED VIEW mv');
    const r = db.execute('SELECT total FROM mv WHERE region = \'East\'');
    assert.equal(r.rows[0].total, 300); // Updated
  });
});
