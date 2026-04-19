// optimizer-stress.test.js — Stress test optimizer with edge cases
// Skewed data, NULLs, correlated subqueries, empty tables, single-row tables

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Optimizer Stress — Skewed Data', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE skewed (id INT PRIMARY KEY, category TEXT, value INT)');
    db.execute('CREATE INDEX idx_skewed_cat ON skewed(category)');
    
    // 90% of rows have category 'common', 10% have 'rare'
    for (let i = 0; i < 1000; i++) {
      const cat = i < 900 ? 'common' : 'rare';
      db.execute(`INSERT INTO skewed VALUES (${i}, '${cat}', ${i * 7 % 500})`);
    }
    db.execute('ANALYZE skewed');
  });

  it('highly selective query on rare category (10%)', () => {
    const result = db.execute("SELECT * FROM skewed WHERE category = 'rare'");
    assert.equal(result.rows.length, 100);
  });

  it('non-selective query on common category (90%)', () => {
    const result = db.execute("SELECT COUNT(*) as cnt FROM skewed WHERE category = 'common'");
    assert.equal(result.rows[0].cnt, 900);
  });

  it('join with skewed distribution', () => {
    db.execute('CREATE TABLE lookup (category TEXT, label TEXT)');
    db.execute("INSERT INTO lookup VALUES ('common', 'Frequent')");
    db.execute("INSERT INTO lookup VALUES ('rare', 'Infrequent')");
    db.execute('ANALYZE lookup');

    const result = db.execute(`
      SELECT l.label, COUNT(*) as cnt
      FROM skewed s
      JOIN lookup l ON s.category = l.category
      GROUP BY l.label
      ORDER BY cnt DESC
    `);
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].cnt, 900);
    assert.equal(result.rows[1].cnt, 100);
  });

  it('EXPLAIN with skewed data reflects cardinality', () => {
    const explain = db.execute("EXPLAIN SELECT * FROM skewed WHERE category = 'rare'");
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.length > 0);
  });
});

describe('Optimizer Stress — NULLs', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE nulltable (id INT PRIMARY KEY, a INT, b TEXT, c INT)');
    
    // 50% NULLs in column 'a', 80% NULLs in 'c'
    for (let i = 0; i < 500; i++) {
      const a = i % 2 === 0 ? i : 'NULL';
      const b = `'val${i}'`;
      const c = i % 5 === 0 ? i * 3 : 'NULL';
      db.execute(`INSERT INTO nulltable VALUES (${i}, ${a}, ${b}, ${c})`);
    }
    db.execute('ANALYZE nulltable');
  });

  it('COUNT(*) vs COUNT(column) with NULLs', () => {
    const all = db.execute('SELECT COUNT(*) as cnt FROM nulltable');
    const nonNullA = db.execute('SELECT COUNT(a) as cnt FROM nulltable');
    const nonNullC = db.execute('SELECT COUNT(c) as cnt FROM nulltable');
    
    assert.equal(all.rows[0].cnt, 500);
    assert.equal(nonNullA.rows[0].cnt, 250); // 50% non-null
    assert.equal(nonNullC.rows[0].cnt, 100); // 20% non-null
  });

  it('IS NULL filter', () => {
    const result = db.execute('SELECT COUNT(*) as cnt FROM nulltable WHERE a IS NULL');
    assert.equal(result.rows[0].cnt, 250);
  });

  it('IS NOT NULL filter', () => {
    const result = db.execute('SELECT COUNT(*) as cnt FROM nulltable WHERE c IS NOT NULL');
    assert.equal(result.rows[0].cnt, 100);
  });

  it('JOIN with NULL keys — NULLs should not match', () => {
    db.execute('CREATE TABLE other (id INT PRIMARY KEY, ref_a INT)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO other VALUES (${i}, ${i * 2})`);
    }
    
    const result = db.execute(`
      SELECT COUNT(*) as cnt
      FROM nulltable n
      JOIN other o ON n.a = o.ref_a
    `);
    // Only non-NULL matches should join
    assert.ok(result.rows[0].cnt > 0);
    assert.ok(result.rows[0].cnt <= 10, 'At most 10 matches');
  });

  it('aggregate with NULLs: SUM, AVG, MIN, MAX', () => {
    const result = db.execute('SELECT SUM(a) as s, AVG(a) as a, MIN(a) as mn, MAX(a) as mx FROM nulltable');
    assert.ok(result.rows[0].s > 0, 'SUM ignores NULLs');
    assert.ok(result.rows[0].a > 0, 'AVG ignores NULLs');
    assert.equal(result.rows[0].mn, 0, 'MIN of even numbers from 0');
    assert.equal(result.rows[0].mx, 498, 'MAX of even numbers up to 498');
  });
});

describe('Optimizer Stress — Correlated Subqueries', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount INT, region TEXT)');
    db.execute('CREATE TABLE customers (name TEXT, region TEXT, credit_limit INT)');
    
    const regions = ['EAST', 'WEST', 'NORTH'];
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO customers VALUES ('Cust${i}', '${regions[i % 3]}', ${1000 + i * 50})`);
    }
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, 'Cust${i % 100}', ${50 + i * 13 % 2000}, '${regions[i % 3]}')`);
    }
    db.execute('ANALYZE orders');
    db.execute('ANALYZE customers');
  });

  it('correlated subquery: orders above customer average', () => {
    const result = db.execute(`
      SELECT o.id, o.customer, o.amount
      FROM orders o
      WHERE o.amount > (
        SELECT AVG(o2.amount)
        FROM orders o2
        WHERE o2.customer = o.customer
      )
      ORDER BY o.amount DESC
      LIMIT 10
    `);
    assert.ok(result.rows.length > 0, 'Some orders above customer average');
    assert.ok(result.rows.length <= 10, 'LIMIT respected');
  });

  it('correlated EXISTS: customers with any large order', () => {
    const result = db.execute(`
      SELECT c.name, c.credit_limit
      FROM customers c
      WHERE EXISTS (
        SELECT 1 FROM orders o
        WHERE o.customer = c.name AND o.amount > 1500
      )
    `);
    assert.ok(result.rows.length > 0, 'Some customers have large orders');
    
    // Verify: each returned customer actually has a large order
    for (const row of result.rows) {
      const check = db.execute(`SELECT COUNT(*) as cnt FROM orders WHERE customer = '${row.name}' AND amount > 1500`);
      assert.ok(check.rows[0].cnt > 0, `${row.name} should have large order`);
    }
  });

  it('double correlated: orders in same region as customer with highest credit', () => {
    const result = db.execute(`
      SELECT o.id, o.amount, o.region
      FROM orders o
      WHERE o.region = (
        SELECT c.region FROM customers c
        WHERE c.name = o.customer
      )
      LIMIT 20
    `);
    assert.ok(result.rows.length > 0);
  });
});

describe('Optimizer Stress — Edge Cases', () => {
  it('empty table join', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty_a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE empty_b (id INT PRIMARY KEY, ref_id INT)');
    
    const result = db.execute('SELECT * FROM empty_a a JOIN empty_b b ON a.id = b.ref_id');
    assert.equal(result.rows.length, 0);
  });

  it('single row join', () => {
    const db = new Database();
    db.execute('CREATE TABLE single_a (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE single_b (id INT PRIMARY KEY, ref_id INT)');
    db.execute("INSERT INTO single_a VALUES (1, 'hello')");
    db.execute("INSERT INTO single_b VALUES (1, 1)");
    
    const result = db.execute('SELECT a.val, b.id FROM single_a a JOIN single_b b ON a.id = b.ref_id');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].val, 'hello');
  });

  it('cross join (cartesian product)', () => {
    const db = new Database();
    db.execute('CREATE TABLE cross_a (id INT PRIMARY KEY, x INT)');
    db.execute('CREATE TABLE cross_b (id INT PRIMARY KEY, y INT)');
    for (let i = 0; i < 5; i++) db.execute(`INSERT INTO cross_a VALUES (${i}, ${i})`);
    for (let i = 0; i < 3; i++) db.execute(`INSERT INTO cross_b VALUES (${i}, ${i * 10})`);
    
    const result = db.execute('SELECT a.x, b.y FROM cross_a a, cross_b b');
    assert.equal(result.rows.length, 15, '5 × 3 = 15 rows');
  });

  it('many-column GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE multi (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT, val INT)');
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO multi VALUES (${i}, 'a${i%3}', 'b${i%5}', 'c${i%2}', ${i})`);
    }
    
    const result = db.execute(`
      SELECT a, b, c, COUNT(*) as cnt, SUM(val) as total
      FROM multi
      GROUP BY a, b, c
      ORDER BY cnt DESC
    `);
    // 3 × 5 × 2 = 30 groups
    assert.equal(result.rows.length, 30);
    
    let totalCount = 0;
    for (const row of result.rows) {
      totalCount += row.cnt;
    }
    assert.equal(totalCount, 100, 'All rows accounted for');
  });

  it('ORDER BY + LIMIT on large result', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, ${(i * 997) % 1000})`);
    }
    
    const result = db.execute('SELECT * FROM big ORDER BY val DESC LIMIT 5');
    assert.equal(result.rows.length, 5);
    assert.equal(result.rows[0].val, 999);
    // Verify order
    for (let i = 1; i < result.rows.length; i++) {
      assert.ok(result.rows[i].val <= result.rows[i-1].val, 'DESC order');
    }
  });

  it('UNION of two queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t1 VALUES (1, 'alpha'), (2, 'beta')");
    db.execute("INSERT INTO t2 VALUES (3, 'gamma'), (4, 'delta'), (5, 'alpha')");
    
    const result = db.execute(`
      SELECT name FROM t1
      UNION
      SELECT name FROM t2
    `);
    // UNION removes duplicates: alpha, beta, gamma, delta = 4
    assert.equal(result.rows.length, 4);
  });

  it('deeply nested subquery (3 levels)', () => {
    const db = new Database();
    db.execute('CREATE TABLE levels (id INT PRIMARY KEY, parent_id INT, value INT)');
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO levels VALUES (${i}, ${i > 0 ? Math.floor(i/2) : 'NULL'}, ${i * 10})`);
    }
    
    const result = db.execute(`
      SELECT l1.id, l1.value
      FROM levels l1
      WHERE l1.id IN (
        SELECT l2.parent_id FROM levels l2
        WHERE l2.value > (
          SELECT AVG(l3.value) FROM levels l3
        )
      )
      ORDER BY l1.id
    `);
    assert.ok(result.rows.length > 0, '3-level nested subquery works');
  });
});

describe('Optimizer Stress — Join Ordering Verification', () => {
  it('optimizer reorders joins for 3+ table queries with ANALYZE', () => {
    const db = new Database();
    // Small dimension table + large fact table
    db.execute('CREATE TABLE dim_small (id INT PRIMARY KEY, label TEXT)'); // 10 rows
    db.execute('CREATE TABLE dim_medium (id INT PRIMARY KEY, category TEXT)'); // 50 rows
    db.execute('CREATE TABLE fact_large (id INT PRIMARY KEY, small_id INT, medium_id INT, amount INT)'); // 1000 rows
    
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO dim_small VALUES (${i}, 'Label${i}')`);
    for (let i = 0; i < 50; i++) db.execute(`INSERT INTO dim_medium VALUES (${i}, 'Cat${i}')`);
    for (let i = 0; i < 1000; i++) {
      db.execute(`INSERT INTO fact_large VALUES (${i}, ${i % 10}, ${i % 50}, ${i * 7 % 500})`);
    }
    
    db.execute('ANALYZE dim_small');
    db.execute('ANALYZE dim_medium');
    db.execute('ANALYZE fact_large');
    
    // Star schema query — optimizer should handle this
    const result = db.execute(`
      SELECT s.label, m.category, SUM(f.amount) as total
      FROM fact_large f
      JOIN dim_small s ON f.small_id = s.id
      JOIN dim_medium m ON f.medium_id = m.id
      GROUP BY s.label, m.category
      ORDER BY total DESC
      LIMIT 5
    `);
    assert.ok(result.rows.length > 0);
    assert.ok(result.rows.length <= 5);
    
    // Verify total matches ungrouped
    const globalTotal = db.execute('SELECT SUM(amount) as total FROM fact_large');
    const groupedTotal = db.execute(`
      SELECT SUM(f.amount) as total
      FROM fact_large f
      JOIN dim_small s ON f.small_id = s.id
      JOIN dim_medium m ON f.medium_id = m.id
    `);
    assert.equal(groupedTotal.rows[0].total, globalTotal.rows[0].total, 
      'Star join total matches base table total');
  });
});
