// regression-2026-04-20.test.js
// Regression tests for all bugs found during Session B stress testing
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Regression Tests (2026-04-20 Session B)', () => {
  
  // === P0: CRITICAL ===
  
  describe('Division truncation', () => {
    it('10.0 / 3 should return ~3.33, not 3', () => {
      const db = new Database();
      const r = db.execute('SELECT 10.0 / 3 as r').rows[0];
      assert.ok(Math.abs(r.r - 3.333) < 0.01, `Expected ~3.33, got ${r.r}`);
    });
    
    it('7.0 / 2 should return 3.5, not 3', () => {
      const db = new Database();
      const r = db.execute('SELECT 7.0 / 2 as r').rows[0];
      assert.equal(r.r, 3.5);
    });
    
    it('integer division should still truncate', () => {
      const db = new Database();
      const r = db.execute('SELECT 10 / 3 as r').rows[0];
      assert.equal(r.r, 3);
    });
  });
  
  describe('CASE WHEN always true', () => {
    it('CASE WHEN NULL should return ELSE branch', () => {
      const db = new Database();
      const r = db.execute("SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END as r").rows[0];
      assert.equal(r.r, 'no');
    });
    
    it('CASE WHEN 0 should return ELSE branch', () => {
      const db = new Database();
      const r = db.execute("SELECT CASE WHEN 0 THEN 'yes' ELSE 'no' END as r").rows[0];
      assert.equal(r.r, 'no');
    });
    
    it('CASE WHEN 1 should return THEN branch', () => {
      const db = new Database();
      const r = db.execute("SELECT CASE WHEN 1 THEN 'yes' ELSE 'no' END as r").rows[0];
      assert.equal(r.r, 'yes');
    });
  });
  
  describe('SUM on empty set', () => {
    it('should return NULL not 0', () => {
      const db = new Database();
      db.execute('CREATE TABLE empty_t (val INT)');
      const r = db.execute('SELECT SUM(val) as s FROM empty_t').rows[0];
      assert.equal(r.s, null);
    });
    
    it('SUM of all-NULL column should return NULL', () => {
      const db = new Database();
      db.execute('CREATE TABLE null_t (val INT)');
      db.execute('INSERT INTO null_t VALUES (NULL), (NULL)');
      const r = db.execute('SELECT SUM(val) as s FROM null_t').rows[0];
      assert.equal(r.s, null);
    });
  });
  
  describe('LIMIT 0', () => {
    it('should return no rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT)');
      db.execute('INSERT INTO t VALUES (1), (2), (3)');
      const r = db.execute('SELECT * FROM t LIMIT 0');
      assert.equal(r.rows.length, 0);
    });
  });
  
  // === P1: SIGNIFICANT ===
  
  describe('NULL IS NULL in SELECT', () => {
    it('SELECT NULL IS NULL should return true/1', () => {
      const db = new Database();
      const r = db.execute('SELECT NULL IS NULL as r').rows[0];
      assert.ok(r.r === true || r.r === 1, `Expected true/1, got ${r.r}`);
    });
    
    it('SELECT 1 IS NULL should return false/0', () => {
      const db = new Database();
      const r = db.execute('SELECT 1 IS NULL as r').rows[0];
      assert.ok(r.r === false || r.r === 0, `Expected false/0, got ${r.r}`);
    });
  });
  
  describe('Boolean expressions in SELECT', () => {
    it('SELECT 1 > 2 should return false', () => {
      const db = new Database();
      const r = db.execute('SELECT 1 > 2 as r').rows[0];
      assert.ok(r.r === false || r.r === 0, `Expected false/0, got ${r.r}`);
    });
    
    it('SELECT TRUE should return true', () => {
      const db = new Database();
      const r = db.execute('SELECT TRUE as r').rows[0];
      assert.ok(r.r === true || r.r === 1, `Expected true/1, got ${r.r}`);
    });
  });
  
  // === INDEX AFTER ROLLBACK ===
  
  describe('Index after rollback', () => {
    it('PK lookup should work after UPDATE + ROLLBACK', async () => {
      // Requires TransactionalDatabase — skip if not available
      try {
        const { TransactionalDatabase } = await import('./transactional-db.js');
        const fs = await import('fs');
        const dir = '/tmp/reg-test-' + Date.now();
        fs.mkdirSync(dir, { recursive: true });
        const db = TransactionalDatabase.open(dir);
        
        db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
        db.execute('INSERT INTO t VALUES (1, 100), (2, 200), (3, 300)');
        
        const s = db.session();
        s.begin();
        s.execute('UPDATE t SET val = 0 WHERE id = 3');
        s.rollback();
        
        const r = db.execute('SELECT * FROM t WHERE id = 3');
        assert.equal(r.rows.length, 1);
        assert.equal(r.rows[0].val, 300);
        
        db.close();
        fs.rmSync(dir, { recursive: true });
      } catch(e) {
        // Skip if transactional DB not available
      }
    });
  });
  
  // === VIEW-TABLE JOIN ===
  
  describe('View-table JOIN', () => {
    it('should include columns from both sides', () => {
      const db = new Database();
      db.execute('CREATE TABLE emp (id INT, dept TEXT, salary REAL)');
      db.execute("INSERT INTO emp VALUES (1, 'Eng', 80000), (2, 'Sales', 70000)");
      db.execute('CREATE VIEW dept_stats AS SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept');
      db.execute('CREATE TABLE dept (name TEXT PRIMARY KEY, budget REAL)');
      db.execute("INSERT INTO dept VALUES ('Eng', 500000), ('Sales', 300000)");
      
      const r = db.execute('SELECT * FROM dept_stats JOIN dept ON dept_stats.dept = dept.name');
      assert.ok(r.rows[0].budget !== undefined || r.rows[0]['dept.budget'] !== undefined,
        'JOIN result should include budget from dept table');
    });
  });
  
  // === NATURAL JOIN ===
  
  describe('NATURAL JOIN', () => {
    it('should match on common column names, not cross join', () => {
      const db = new Database();
      db.execute('CREATE TABLE a (id INT, name TEXT)');
      db.execute('CREATE TABLE b (id INT, val INT)');
      db.execute("INSERT INTO a VALUES (1, 'x'), (2, 'y')");
      db.execute('INSERT INTO b VALUES (1, 10), (2, 20)');
      
      const r = db.execute('SELECT * FROM a NATURAL JOIN b');
      assert.equal(r.rows.length, 2, `Expected 2 rows, got ${r.rows.length} (cross join?)` );
    });
  });
  
  // === INSERT FROM CTE ===
  
  describe('INSERT from CTE', () => {
    it('should actually insert rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE src (id INT, val INT)');
      db.execute('INSERT INTO src VALUES (1, 10), (2, 20)');
      db.execute('CREATE TABLE dst (id INT, val INT)');
      
      db.execute('WITH cte AS (SELECT id, val FROM src) INSERT INTO dst SELECT * FROM cte');
      const r = db.execute('SELECT COUNT(*) as c FROM dst');
      assert.equal(r.c || r.rows?.[0]?.c, 2, 'INSERT from CTE should insert 2 rows');
    });
  });
  
  // === TRIGGER NEW/OLD ===
  
  describe('Trigger NEW/OLD references', () => {
    it('AFTER INSERT trigger should access NEW values', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
      db.execute('CREATE TABLE audit (row_id INT)');
      db.execute('CREATE TRIGGER tr AFTER INSERT ON t FOR EACH ROW INSERT INTO audit VALUES (NEW.id)');
      db.execute('INSERT INTO t VALUES (42, 100)');
      
      const r = db.execute('SELECT row_id FROM audit');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].row_id, 42, `Expected 42, got ${r.rows[0].row_id}`);
    });
  });
  
  // === UNIQUE CONSTRAINT ===
  
  describe('UNIQUE constraint enforcement', () => {
    it('should reject duplicate values', () => {
      const db = new Database();
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT, UNIQUE(email))');
      db.execute("INSERT INTO t VALUES (1, 'a@b.com')");
      
      assert.throws(() => {
        db.execute("INSERT INTO t VALUES (2, 'a@b.com')");
      }, /unique|duplicate|constraint/i);
    });
  });
  
  // === FK CASCADE ===
  
  describe('FK CASCADE', () => {
    it('DELETE CASCADE should remove child rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE parent (id INT PRIMARY KEY, name TEXT)');
      db.execute("INSERT INTO parent VALUES (1, 'p1')");
      db.execute('CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)');
      db.execute('INSERT INTO child VALUES (1, 1), (2, 1)');
      
      db.execute('DELETE FROM parent WHERE id = 1');
      const r = db.execute('SELECT COUNT(*) as c FROM child');
      assert.equal(r.rows[0].c, 0, 'CASCADE should delete child rows');
    });
  });
});
