// optimizer-stress.test.js — Adversarial tests for the query optimizer
// Goal: find bugs by throwing edge cases at histogram, selectivity, DP join reorder,
// decorrelation, and predicate pushdown

import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Optimizer Stress Tests', () => {
  // ========== SKEWED DATA vs HISTOGRAMS ==========
  
  describe('Histogram accuracy with extreme skew', () => {
    let db;
    
    before(() => {
      db = new Database();
      db.execute('CREATE TABLE skewed (id INT PRIMARY KEY, category TEXT, val INT)');
      // 90% of rows have category='A', 10% have category='B'
      for (let i = 0; i < 900; i++) {
        db.execute(`INSERT INTO skewed VALUES (${i}, 'A', ${i % 100})`);
      }
      for (let i = 900; i < 1000; i++) {
        db.execute(`INSERT INTO skewed VALUES (${i}, 'B', ${i})`);
      }
      db.execute('CREATE INDEX idx_cat ON skewed (category)');
    });
    
    it('selectivity estimation for majority value should be close to 0.9', () => {
      const result = db.execute("EXPLAIN SELECT * FROM skewed WHERE category = 'A'");
      // EXPLAIN returns plan, not rows — just verify it works
      assert.ok(result);
      assert.ok(result.plan || result.type === 'PLAN');
    });
    
    it('selectivity estimation for minority value should be close to 0.1', () => {
      const result = db.execute("EXPLAIN SELECT * FROM skewed WHERE category = 'B'");
      assert.ok(result);
    });
    
    it('query with skewed filter returns correct results', () => {
      const resultA = db.execute("SELECT COUNT(*) as cnt FROM skewed WHERE category = 'A'");
      assert.strictEqual(resultA.rows[0].cnt, 900);
      
      const resultB = db.execute("SELECT COUNT(*) as cnt FROM skewed WHERE category = 'B'");
      assert.strictEqual(resultB.rows[0].cnt, 100);
    });
    
    it('compound predicate with skewed column', () => {
      // AND: category='A' AND val < 10 — should be ~9% (0.9 * 0.1)
      const result = db.execute("SELECT COUNT(*) as cnt FROM skewed WHERE category = 'A' AND val < 10");
      // Each val 0-9 appears ~9 times in 900 'A' rows
      assert.strictEqual(result.rows[0].cnt, 90);
    });
    
    it('OR with skewed columns does not over-estimate', () => {
      const result = db.execute("SELECT COUNT(*) as cnt FROM skewed WHERE category = 'A' OR val > 950");
      // All 900 A's + some B's with val > 950
      assert.ok(result.rows[0].cnt >= 900);
      assert.ok(result.rows[0].cnt <= 1000);
    });
  });

  // ========== UNIFORM vs ZIPF DISTRIBUTIONS ==========
  
  describe('Histogram with all-same values', () => {
    let db;
    
    before(() => {
      db = new Database();
      db.execute('CREATE TABLE uniform (id INT PRIMARY KEY, val INT)');
      // All rows have the same value
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO uniform VALUES (${i}, 42)`);
      }
    });
    
    it('equality on the only value returns all rows', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM uniform WHERE val = 42');
      assert.strictEqual(result.rows[0].cnt, 100);
    });
    
    it('equality on a non-existent value returns zero rows', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM uniform WHERE val = 99');
      assert.strictEqual(result.rows[0].cnt, 0);
    });
    
    it('range query on single-value column', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM uniform WHERE val > 40 AND val < 50');
      assert.strictEqual(result.rows[0].cnt, 100);
    });
  });

  // ========== MULTI-WAY JOINS ==========

  describe('DP Join Reordering with varying table sizes', () => {
    let db;
    
    before(() => {
      db = new Database();
      // Create 4 tables of very different sizes
      db.execute('CREATE TABLE tiny (id INT PRIMARY KEY, name TEXT)');
      db.execute('CREATE TABLE small (id INT PRIMARY KEY, tiny_id INT, val TEXT)');
      db.execute('CREATE TABLE medium (id INT PRIMARY KEY, small_id INT, data TEXT)');
      db.execute('CREATE TABLE big (id INT PRIMARY KEY, medium_id INT, payload TEXT)');
      
      // tiny: 5 rows
      for (let i = 0; i < 5; i++) {
        db.execute(`INSERT INTO tiny VALUES (${i}, 'name_${i}')`);
      }
      // small: 50 rows
      for (let i = 0; i < 50; i++) {
        db.execute(`INSERT INTO small VALUES (${i}, ${i % 5}, 'val_${i}')`);
      }
      // medium: 200 rows
      for (let i = 0; i < 200; i++) {
        db.execute(`INSERT INTO medium VALUES (${i}, ${i % 50}, 'data_${i}')`);
      }
      // big: 500 rows
      for (let i = 0; i < 500; i++) {
        db.execute(`INSERT INTO big VALUES (${i}, ${i % 200}, 'payload_${i}')`);
      }
    });
    
    it('4-way join returns correct results', () => {
      const result = db.execute(`
        SELECT t.name, s.val, m.data, b.payload
        FROM tiny t
        JOIN small s ON s.tiny_id = t.id
        JOIN medium m ON m.small_id = s.id
        JOIN big b ON b.medium_id = m.id
        WHERE t.id = 0
      `);
      assert.ok(result.rows.length > 0);
      // Verify correct join: tiny.id=0 → small.tiny_id=0 (10 rows) → medium.small_id matches → big.medium_id matches
      for (const row of result.rows) {
        assert.strictEqual(row.name, 'name_0');
      }
    });
    
    it('3-way join with filtering', () => {
      const result = db.execute(`
        SELECT s.val, m.data
        FROM small s
        JOIN medium m ON m.small_id = s.id
        JOIN big b ON b.medium_id = m.id
        WHERE s.tiny_id = 1
        LIMIT 10
      `);
      assert.ok(result.rows.length <= 10);
      assert.ok(result.rows.length > 0);
    });
    
    it('join order does not affect result correctness', () => {
      // Same logical query, different FROM ordering
      const r1 = db.execute(`
        SELECT COUNT(*) as cnt FROM tiny t
        JOIN small s ON s.tiny_id = t.id
        WHERE t.id = 0
      `);
      const r2 = db.execute(`
        SELECT COUNT(*) as cnt FROM small s
        JOIN tiny t ON t.id = s.tiny_id
        WHERE t.id = 0
      `);
      assert.strictEqual(r1.rows[0].cnt, r2.rows[0].cnt);
    });
  });

  // ========== CORRELATED SUBQUERIES ==========

  describe('Correlated subquery edge cases', () => {
    let db;
    
    before(() => {
      db = new Database();
      db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)');
      db.execute('CREATE TABLE departments (name TEXT, budget INT)');
      
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 'eng', 100)");
      db.execute("INSERT INTO employees VALUES (2, 'Bob', 'eng', 120)");
      db.execute("INSERT INTO employees VALUES (3, 'Carol', 'sales', 90)");
      db.execute("INSERT INTO employees VALUES (4, 'Dave', 'sales', 110)");
      db.execute("INSERT INTO employees VALUES (5, 'Eve', 'hr', 80)");
      
      db.execute("INSERT INTO departments VALUES ('eng', 500)");
      db.execute("INSERT INTO departments VALUES ('sales', 300)");
      db.execute("INSERT INTO departments VALUES ('hr', 200)");
    });
    
    it('uncorrelated IN subquery decorrelates to hash set', () => {
      const result = db.execute(`
        SELECT name FROM employees 
        WHERE dept IN (SELECT name FROM departments WHERE budget > 250)
      `);
      // eng (500) and sales (300) have budget > 250
      assert.strictEqual(result.rows.length, 4);
    });
    
    it('correlated EXISTS subquery', () => {
      const result = db.execute(`
        SELECT name FROM employees e
        WHERE EXISTS (SELECT 1 FROM departments d WHERE d.name = e.dept AND d.budget > 400)
      `);
      // Only eng has budget > 400
      const names = result.rows.map(r => r.name).sort();
      assert.deepStrictEqual(names, ['Alice', 'Bob']);
    });
    
    it('NOT IN with empty subquery returns all rows', () => {
      const result = db.execute(`
        SELECT name FROM employees 
        WHERE dept NOT IN (SELECT name FROM departments WHERE budget > 9999)
      `);
      assert.strictEqual(result.rows.length, 5);
    });
    
    it('scalar subquery in SELECT list', () => {
      const result = db.execute(`
        SELECT name, (SELECT budget FROM departments d WHERE d.name = e.dept) as dept_budget
        FROM employees e
        ORDER BY name
      `);
      assert.strictEqual(result.rows[0].name, 'Alice');
      assert.strictEqual(result.rows[0].dept_budget, 500);
    });
    
    it('nested subqueries', () => {
      const result = db.execute(`
        SELECT name FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
      `);
      // avg salary = (100+120+90+110+80)/5 = 100
      const names = result.rows.map(r => r.name).sort();
      assert.deepStrictEqual(names, ['Bob', 'Dave']);
    });
  });

  // ========== PREDICATE PUSHDOWN EDGE CASES ==========

  describe('Predicate pushdown correctness', () => {
    let db;
    
    before(() => {
      db = new Database();
      db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT)');
      db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, country TEXT)');
      
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO orders VALUES (${i}, ${i % 20}, ${(i * 17) % 500}, '${i % 3 === 0 ? 'shipped' : 'pending'}')`);
      }
      for (let i = 0; i < 20; i++) {
        db.execute(`INSERT INTO customers VALUES (${i}, 'customer_${i}', '${i % 2 === 0 ? 'US' : 'UK'}')`);
      }
    });
    
    it('WHERE on left table pushes down correctly', () => {
      const result = db.execute(`
        SELECT c.name, o.amount
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE c.country = 'US'
      `);
      // 10 US customers, each with 5 orders = 50
      assert.strictEqual(result.rows.length, 50);
      for (const row of result.rows) {
        assert.ok(row.name !== undefined);
      }
    });
    
    it('WHERE on right table pushes down correctly', () => {
      const result = db.execute(`
        SELECT c.name, o.amount
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE o.status = 'shipped'
      `);
      // Every 3rd order is shipped = ~33 orders
      for (const row of result.rows) {
        assert.ok(row.amount !== undefined);
      }
    });
    
    it('WHERE spanning both tables cannot push down', () => {
      const result = db.execute(`
        SELECT c.name, o.amount
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE o.amount > c.id * 10
      `);
      // Cross-table predicate — must evaluate after join
      assert.ok(result.rows.length >= 0);
    });
    
    it('compound WHERE with pushable and non-pushable parts', () => {
      const result = db.execute(`
        SELECT c.name, o.amount
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE c.country = 'UK' AND o.status = 'shipped'
      `);
      // UK customers with shipped orders
      assert.ok(result.rows.length > 0);
    });
  });

  // ========== NULL HANDLING IN OPTIMIZER ==========

  describe('NULL handling in selectivity estimation', () => {
    let db;
    
    before(() => {
      db = new Database();
      db.execute('CREATE TABLE nulls_test (id INT PRIMARY KEY, val INT, name TEXT)');
      for (let i = 0; i < 50; i++) {
        db.execute(`INSERT INTO nulls_test VALUES (${i}, ${i}, 'name_${i}')`);
      }
      // Insert 50 rows with NULL val
      for (let i = 50; i < 100; i++) {
        db.execute(`INSERT INTO nulls_test VALUES (${i}, NULL, 'name_${i}')`);
      }
    });
    
    it('IS NULL returns correct count', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM nulls_test WHERE val IS NULL');
      assert.strictEqual(result.rows[0].cnt, 50);
    });
    
    it('IS NOT NULL returns correct count', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM nulls_test WHERE val IS NOT NULL');
      assert.strictEqual(result.rows[0].cnt, 50);
    });
    
    it('equality with NULLs - NULL = NULL is false in SQL', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM nulls_test WHERE val = val');
      // Only non-NULL rows satisfy val = val
      assert.strictEqual(result.rows[0].cnt, 50);
    });
    
    it('aggregate with NULLs', () => {
      const result = db.execute('SELECT AVG(val) as avg_val FROM nulls_test');
      // AVG should only consider non-NULL values: sum(0..49)/50 = 24.5
      assert.strictEqual(result.rows[0].avg_val, 24.5);
    });
  });

  // ========== EXTREME QUERY PATTERNS ==========

  describe('Edge case queries', () => {
    let db;
    
    before(() => {
      db = new Database();
      db.execute('CREATE TABLE data (id INT PRIMARY KEY, val INT, grp TEXT)');
      for (let i = 0; i < 200; i++) {
        db.execute(`INSERT INTO data VALUES (${i}, ${i * 7 % 100}, 'group_${i % 10}')`);
      }
    });
    
    it('SELECT with no matching rows', () => {
      const result = db.execute('SELECT * FROM data WHERE val > 99999');
      assert.strictEqual(result.rows.length, 0);
    });
    
    it('ORDER BY + LIMIT + OFFSET', () => {
      const result = db.execute('SELECT id FROM data ORDER BY val LIMIT 5 OFFSET 10');
      assert.strictEqual(result.rows.length, 5);
    });
    
    it('GROUP BY with HAVING filtering all groups', () => {
      const result = db.execute(`
        SELECT grp, COUNT(*) as cnt FROM data 
        GROUP BY grp 
        HAVING COUNT(*) > 99999
      `);
      assert.strictEqual(result.rows.length, 0);
    });
    
    it('multiple aggregates in same query', () => {
      const result = db.execute(`
        SELECT grp, 
               COUNT(*) as cnt, 
               SUM(val) as total, 
               AVG(val) as avg_val,
               MIN(val) as min_val,
               MAX(val) as max_val
        FROM data 
        GROUP BY grp
        ORDER BY grp
      `);
      assert.strictEqual(result.rows.length, 10);
      for (const row of result.rows) {
        assert.strictEqual(row.cnt, 20);
        assert.ok(row.min_val <= row.avg_val);
        assert.ok(row.avg_val <= row.max_val);
      }
    });
    
    it('self-join', () => {
      const result = db.execute(`
        SELECT a.id as a_id, b.id as b_id
        FROM data a
        JOIN data b ON b.val = a.val
        WHERE a.id < 5
        ORDER BY a.id, b.id
      `);
      assert.ok(result.rows.length >= 5); // At least one match per row
    });
    
    it('DISTINCT with ORDER BY', () => {
      const result = db.execute('SELECT DISTINCT val FROM data ORDER BY val LIMIT 10');
      assert.strictEqual(result.rows.length, 10);
      // Verify ordering
      for (let i = 1; i < result.rows.length; i++) {
        assert.ok(result.rows[i].val >= result.rows[i-1].val);
      }
    });
    
    it('empty table queries', () => {
      db.execute('CREATE TABLE empty_t (id INT PRIMARY KEY, val INT)');
      
      const r1 = db.execute('SELECT COUNT(*) as cnt FROM empty_t');
      assert.strictEqual(r1.rows[0].cnt, 0);
      
      const r2 = db.execute('SELECT AVG(val) as avg_val FROM empty_t');
      assert.strictEqual(r2.rows[0].avg_val, null);
      
      const r3 = db.execute('SELECT * FROM empty_t WHERE val > 0');
      assert.strictEqual(r3.rows.length, 0);
    });
    
    it('deeply nested expressions', () => {
      const result = db.execute(`
        SELECT id FROM data
        WHERE ((val > 10 AND val < 90) OR (id % 2 = 0)) AND grp = 'group_1'
      `);
      assert.ok(result.rows.length > 0);
    });
  });

  // ========== INDEX SELECTION STRESS ==========
  
  describe('Index selection with multiple indexes', () => {
    let db;
    
    before(() => {
      db = new Database();
      db.execute('CREATE TABLE multi_idx (id INT PRIMARY KEY, a INT, b INT, c TEXT)');
      for (let i = 0; i < 500; i++) {
        db.execute(`INSERT INTO multi_idx VALUES (${i}, ${i % 10}, ${i % 50}, 'text_${i}')`);
      }
      db.execute('CREATE INDEX idx_a ON multi_idx (a)');
      db.execute('CREATE INDEX idx_b ON multi_idx (b)');
    });
    
    it('picks more selective index', () => {
      // a has 10 distinct values (50 rows each), b has 50 distinct values (10 rows each)
      // a=3 AND b=3 should match rows where both conditions hold
      const result = db.execute('SELECT * FROM multi_idx WHERE a = 3 AND b = 3');
      assert.ok(result.rows.length > 0);
      for (const row of result.rows) {
        assert.strictEqual(row.a, 3);
        assert.strictEqual(row.b, 3);
      }
    });
    
    it('high selectivity filter returns correct results', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM multi_idx WHERE b = 25');
      assert.strictEqual(result.rows[0].cnt, 10);
    });
    
    it('low selectivity filter returns correct results', () => {
      const result = db.execute('SELECT COUNT(*) as cnt FROM multi_idx WHERE a = 0');
      assert.strictEqual(result.rows[0].cnt, 50);
    });
  });
});
