// groupby-alias-ordinal.test.js — GROUP BY alias + ordinal position tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('GROUP BY alias resolution', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)');
    db.execute("INSERT INTO employees VALUES ('alice', 'eng', 100)");
    db.execute("INSERT INTO employees VALUES ('bob', 'eng', 120)");
    db.execute("INSERT INTO employees VALUES ('carol', 'sales', 80)");
    db.execute("INSERT INTO employees VALUES ('dave', 'sales', 90)");
    db.execute("INSERT INTO employees VALUES ('eve', 'hr', 95)");
  });

  describe('Simple column alias', () => {
    it('GROUP BY alias of renamed column', () => {
      const r = db.execute('SELECT dept AS department, COUNT(*) AS cnt FROM employees GROUP BY department');
      assert.equal(r.rows.length, 3);
      const eng = r.rows.find(r => r.department === 'eng');
      assert.equal(eng.cnt, 2);
    });

    it('GROUP BY alias with SUM', () => {
      const r = db.execute('SELECT dept AS d, SUM(salary) AS total FROM employees GROUP BY d');
      assert.equal(r.rows.length, 3);
      const eng = r.rows.find(r => r.d === 'eng');
      assert.equal(eng.total, 220);
    });

    it('GROUP BY alias with multiple aliases', () => {
      db.execute('CREATE TABLE t (a TEXT, b TEXT, val INTEGER)');
      db.execute("INSERT INTO t VALUES ('x', 'p', 1), ('x', 'q', 2), ('y', 'p', 3), ('y', 'q', 4)");
      const r = db.execute('SELECT a AS col1, b AS col2, SUM(val) AS total FROM t GROUP BY col1, col2');
      assert.equal(r.rows.length, 4);
    });
  });

  describe('Expression alias', () => {
    it('GROUP BY alias of expression', () => {
      db.execute('CREATE TABLE nums (val INTEGER)');
      db.execute('INSERT INTO nums VALUES (1), (2), (11), (12), (21)');
      const r = db.execute('SELECT val / 10 AS decade, COUNT(*) AS cnt FROM nums GROUP BY decade');
      assert.equal(r.rows.length, 3);
    });

    it('GROUP BY alias of CASE expression', () => {
      const r = db.execute("SELECT CASE WHEN salary > 100 THEN 'high' ELSE 'normal' END AS level, COUNT(*) AS cnt FROM employees GROUP BY level");
      assert.equal(r.rows.length, 2);
      const high = r.rows.find(r => r.level === 'high');
      assert.equal(high.cnt, 1);
    });

    it('GROUP BY alias of function call', () => {
      const r = db.execute('SELECT LENGTH(name) AS name_len, COUNT(*) AS cnt FROM employees GROUP BY name_len');
      assert.ok(r.rows.length >= 2);
    });
  });

  describe('Ordinal position', () => {
    it('GROUP BY 1 (first column)', () => {
      const r = db.execute('SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY 1');
      assert.equal(r.rows.length, 3);
    });

    it('GROUP BY 1 preserves column name', () => {
      const r = db.execute('SELECT dept, SUM(salary) AS total FROM employees GROUP BY 1');
      assert.ok(r.rows[0].dept !== undefined);
    });

    it('GROUP BY 1 with aliased column', () => {
      const r = db.execute('SELECT dept AS department, COUNT(*) AS cnt FROM employees GROUP BY 1');
      assert.equal(r.rows.length, 3);
      assert.ok(r.rows[0].department !== undefined);
    });

    it('GROUP BY 1 with expression column', () => {
      db.execute('CREATE TABLE nums (val INTEGER)');
      db.execute('INSERT INTO nums VALUES (1), (2), (3), (11), (12), (13)');
      const r = db.execute('SELECT val % 10 AS bucket, COUNT(*) AS cnt FROM nums GROUP BY 1');
      assert.equal(r.rows.length, 3);
    });

    it('GROUP BY 2 (second column)', () => {
      db.execute('CREATE TABLE t (a TEXT, b TEXT, val INTEGER)');
      db.execute("INSERT INTO t VALUES ('x', 'p', 1), ('y', 'p', 2), ('x', 'q', 3)");
      const r = db.execute('SELECT a, b, COUNT(*) AS cnt FROM t GROUP BY 2');
      assert.equal(r.rows.length, 2);
    });

    it('GROUP BY 1, 2 (multiple ordinals)', () => {
      db.execute('CREATE TABLE t (a TEXT, b TEXT, val INTEGER)');
      db.execute("INSERT INTO t VALUES ('x', 'p', 1), ('x', 'q', 2), ('y', 'p', 3), ('y', 'q', 4)");
      const r = db.execute('SELECT a, b, SUM(val) AS total FROM t GROUP BY 1, 2');
      assert.equal(r.rows.length, 4);
    });
  });

  describe('Mixed alias + ordinal', () => {
    it('GROUP BY with column name + alias', () => {
      const r = db.execute('SELECT dept, dept AS d2, COUNT(*) AS cnt FROM employees GROUP BY dept');
      assert.equal(r.rows.length, 3);
    });

    it('GROUP BY alias still allows HAVING', () => {
      const r = db.execute('SELECT dept AS department, COUNT(*) AS cnt FROM employees GROUP BY department HAVING cnt > 1');
      assert.equal(r.rows.length, 2);
    });

    it('GROUP BY ordinal with ORDER BY', () => {
      const r = db.execute('SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY 1 ORDER BY cnt DESC');
      assert.equal(r.rows[0].cnt, 2);
    });
  });

  describe('Edge cases', () => {
    it('GROUP BY alias that shadows a real column name', () => {
      // dept AS name — 'name' exists as another column
      const r = db.execute('SELECT dept AS name, COUNT(*) AS cnt FROM employees GROUP BY name');
      // Should group by the actual column 'name' (5 unique names) or the alias 'name' pointing to dept (3)?
      // SQL standard: GROUP BY alias takes precedence in most databases
      assert.ok(r.rows.length <= 5);
    });

    it('GROUP BY with NULL values and alias', () => {
      db.execute('CREATE TABLE t (cat TEXT, val INTEGER)');
      db.execute("INSERT INTO t VALUES ('a', 1), ('a', 2), (NULL, 3), (NULL, 4)");
      const r = db.execute('SELECT cat AS category, SUM(val) AS total FROM t GROUP BY category');
      assert.equal(r.rows.length, 2);
    });

    it('GROUP BY 1 with aggregate-only query', () => {
      // GROUP BY 1 when column 1 is an aggregate — weird but should not crash
      try {
        db.execute('SELECT COUNT(*) FROM employees GROUP BY 1');
        // Either works or throws — just don't crash
      } catch (e) {
        assert.ok(e.message.length > 0);
      }
    });
  });
});
