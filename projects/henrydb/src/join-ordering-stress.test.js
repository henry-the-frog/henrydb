// join-ordering-stress.test.js — Adversarial stress tests for cost-based join ordering
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Join ordering stress tests', () => {
  
  it('chain join: A-B-C where A is huge, C is tiny → should start from C end', () => {
    const db = new Database();
    db.execute('CREATE TABLE huge (id INT, val INT)');
    db.execute('CREATE TABLE medium (id INT, huge_id INT)');
    db.execute('CREATE TABLE tiny (id INT, medium_id INT)');
    for (let i = 1; i <= 1000; i++) db.execute(`INSERT INTO huge VALUES (${i}, ${i % 50})`);
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO medium VALUES (${i}, ${(i % 1000) + 1})`);
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO tiny VALUES (${i}, ${(i % 100) + 1})`);
    db.execute('ANALYZE TABLE huge');
    db.execute('ANALYZE TABLE medium');
    db.execute('ANALYZE TABLE tiny');
    
    // Query in worst order
    const result = db.execute(`
      SELECT h.id, m.id as mid, t.id as tid
      FROM huge h
      JOIN medium m ON h.id = m.huge_id
      JOIN tiny t ON m.id = t.medium_id
    `);
    assert.ok(result.rows.length > 0);
    // Verify correctness
    for (const row of result.rows) {
      assert.ok(row.id != null);
      assert.ok(row.mid != null);
      assert.ok(row.tid != null);
    }
  });

  it('star join: fact table with 4 dimensions of very different sizes', () => {
    const db = new Database();
    db.execute('CREATE TABLE fact (id INT, d1_id INT, d2_id INT, d3_id INT, d4_id INT)');
    db.execute('CREATE TABLE dim1 (id INT, name TEXT)');
    db.execute('CREATE TABLE dim2 (id INT, name TEXT)');
    db.execute('CREATE TABLE dim3 (id INT, name TEXT)');
    db.execute('CREATE TABLE dim4 (id INT, name TEXT)');
    for (let i = 1; i <= 500; i++) {
      db.execute(`INSERT INTO fact VALUES (${i}, ${(i%10)+1}, ${(i%5)+1}, ${(i%3)+1}, ${(i%2)+1})`);
    }
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO dim1 VALUES (${i}, 'D1_${i}')`);
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO dim2 VALUES (${i}, 'D2_${i}')`);
    for (let i = 1; i <= 3; i++) db.execute(`INSERT INTO dim3 VALUES (${i}, 'D3_${i}')`);
    for (let i = 1; i <= 2; i++) db.execute(`INSERT INTO dim4 VALUES (${i}, 'D4_${i}')`);
    db.execute('ANALYZE TABLE fact');
    db.execute('ANALYZE TABLE dim1');
    db.execute('ANALYZE TABLE dim2');
    db.execute('ANALYZE TABLE dim3');
    db.execute('ANALYZE TABLE dim4');
    
    const result = db.execute(`
      SELECT f.id, d1.name as d1, d2.name as d2, d3.name as d3, d4.name as d4
      FROM fact f
      JOIN dim1 d1 ON f.d1_id = d1.id
      JOIN dim2 d2 ON f.d2_id = d2.id
      JOIN dim3 d3 ON f.d3_id = d3.id
      JOIN dim4 d4 ON f.d4_id = d4.id
      WHERE f.id <= 10
      ORDER BY f.id
    `);
    assert.strictEqual(result.rows.length, 10);
    for (const row of result.rows) {
      assert.ok(row.d1 && row.d2 && row.d3 && row.d4);
    }
  });

  it('self-join: same table joined to itself', () => {
    const db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, manager_id INT)');
    for (let i = 1; i <= 50; i++) {
      const mgr = i <= 5 ? 0 : ((i % 5) + 1);
      db.execute(`INSERT INTO employees VALUES (${i}, 'E${i}', ${mgr})`);
    }
    db.execute('ANALYZE TABLE employees');
    
    const result = db.execute(`
      SELECT e.name as emp, m.name as manager
      FROM employees e
      JOIN employees m ON e.manager_id = m.id
      WHERE e.id > 5
      ORDER BY e.id
    `);
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.ok(row.emp);
      assert.ok(row.manager);
    }
  });

  it('join with highly skewed foreign key distribution', () => {
    const db = new Database();
    db.execute('CREATE TABLE parents (id INT, name TEXT)');
    db.execute('CREATE TABLE children (id INT, parent_id INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO parents VALUES (${i}, 'P${i}')`);
    // 80% of children belong to parent 1, rest spread across 2-10
    for (let i = 1; i <= 200; i++) {
      const pid = i <= 160 ? 1 : ((i % 9) + 2);
      db.execute(`INSERT INTO children VALUES (${i}, ${pid})`);
    }
    db.execute('ANALYZE TABLE parents');
    db.execute('ANALYZE TABLE children');
    
    const result = db.execute(`
      SELECT p.name, COUNT(*) as cnt
      FROM parents p
      JOIN children c ON p.id = c.parent_id
      GROUP BY p.name
      ORDER BY cnt DESC
    `);
    assert.ok(result.rows.length > 0);
    // Parent 1 should have 160 children
    assert.strictEqual(result.rows[0].cnt, 160);
  });

  it('3-way join results match regardless of written order', () => {
    const db = new Database();
    db.execute('CREATE TABLE t1 (id INT, val INT)');
    db.execute('CREATE TABLE t2 (id INT, t1_id INT)');
    db.execute('CREATE TABLE t3 (id INT, t2_id INT)');
    for (let i = 1; i <= 30; i++) db.execute(`INSERT INTO t1 VALUES (${i}, ${i * 10})`);
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO t2 VALUES (${i}, ${(i % 30) + 1})`);
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t3 VALUES (${i}, ${(i % 20) + 1})`);
    db.execute('ANALYZE TABLE t1');
    db.execute('ANALYZE TABLE t2');
    db.execute('ANALYZE TABLE t3');
    
    // Query 1: t1 → t2 → t3
    const r1 = db.execute(`
      SELECT t1.id as a, t2.id as b, t3.id as c
      FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id
      ORDER BY a, b, c
    `);
    
    // Drop and recreate without stats to get naive order
    const db2 = new Database();
    db2.execute('CREATE TABLE t1 (id INT, val INT)');
    db2.execute('CREATE TABLE t2 (id INT, t1_id INT)');
    db2.execute('CREATE TABLE t3 (id INT, t2_id INT)');
    for (let i = 1; i <= 30; i++) db2.execute(`INSERT INTO t1 VALUES (${i}, ${i * 10})`);
    for (let i = 1; i <= 20; i++) db2.execute(`INSERT INTO t2 VALUES (${i}, ${(i % 30) + 1})`);
    for (let i = 1; i <= 10; i++) db2.execute(`INSERT INTO t3 VALUES (${i}, ${(i % 20) + 1})`);
    // NO ANALYZE — uses naive order
    const r2 = db2.execute(`
      SELECT t1.id as a, t2.id as b, t3.id as c
      FROM t1 JOIN t2 ON t1.id = t2.t1_id JOIN t3 ON t2.id = t3.t2_id
      ORDER BY a, b, c
    `);
    
    assert.deepStrictEqual(r1.rows, r2.rows, 'optimized and naive orders should give same results');
  });

  it('join with empty intermediate table', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT)');
    db.execute('CREATE TABLE b (id INT, a_id INT)');
    db.execute('CREATE TABLE c (id INT, b_id INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO a VALUES (${i})`);
    // b is empty — should produce 0 results
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO c VALUES (${i}, ${i})`);
    db.execute('ANALYZE TABLE a');
    db.execute('ANALYZE TABLE b');
    db.execute('ANALYZE TABLE c');
    
    const result = db.execute('SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id');
    assert.strictEqual(result.rows.length, 0);
  });

  it('join with all-same-value foreign key (ndv = 1)', () => {
    const db = new Database();
    db.execute('CREATE TABLE lookup (id INT, name TEXT)');
    db.execute('CREATE TABLE data (id INT, lookup_id INT)');
    db.execute(`INSERT INTO lookup VALUES (1, 'default')`);
    db.execute(`INSERT INTO lookup VALUES (2, 'other')`);
    for (let i = 1; i <= 100; i++) {
      // All rows reference lookup_id = 1
      db.execute(`INSERT INTO data VALUES (${i}, 1)`);
    }
    db.execute('ANALYZE TABLE lookup');
    db.execute('ANALYZE TABLE data');
    
    const result = db.execute(`
      SELECT d.id, l.name FROM data d JOIN lookup l ON d.lookup_id = l.id ORDER BY d.id
    `);
    assert.strictEqual(result.rows.length, 100);
    for (const row of result.rows) {
      assert.strictEqual(row.name, 'default');
    }
  });

  it('5-table join (maximum for DP)', () => {
    const db = new Database();
    db.execute('CREATE TABLE ta (id INT)');
    db.execute('CREATE TABLE tb (id INT, ta_id INT)');
    db.execute('CREATE TABLE tc (id INT, tb_id INT)');
    db.execute('CREATE TABLE td (id INT, tc_id INT)');
    db.execute('CREATE TABLE te (id INT, td_id INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO ta VALUES (${i})`);
    for (let i = 1; i <= 40; i++) db.execute(`INSERT INTO tb VALUES (${i}, ${(i%50)+1})`);
    for (let i = 1; i <= 30; i++) db.execute(`INSERT INTO tc VALUES (${i}, ${(i%40)+1})`);
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO td VALUES (${i}, ${(i%30)+1})`);
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO te VALUES (${i}, ${(i%20)+1})`);
    db.execute('ANALYZE TABLE ta');
    db.execute('ANALYZE TABLE tb');
    db.execute('ANALYZE TABLE tc');
    db.execute('ANALYZE TABLE td');
    db.execute('ANALYZE TABLE te');
    
    const result = db.execute(`
      SELECT ta.id as a, te.id as e
      FROM ta
      JOIN tb ON ta.id = tb.ta_id
      JOIN tc ON tb.id = tc.tb_id
      JOIN td ON tc.id = td.tc_id
      JOIN te ON td.id = te.td_id
      ORDER BY a, e
    `);
    assert.ok(result.rows.length > 0);
  });

  it('LEFT join preserved in order even when INNER joins reordered', () => {
    const db = new Database();
    db.execute('CREATE TABLE main (id INT)');
    db.execute('CREATE TABLE inner1 (id INT, main_id INT)');
    db.execute('CREATE TABLE inner2 (id INT, main_id INT)');
    db.execute('CREATE TABLE outer1 (id INT, main_id INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO main VALUES (${i})`);
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO inner1 VALUES (${i}, ${(i%100)+1})`);
    for (let i = 1; i <= 30; i++) db.execute(`INSERT INTO inner2 VALUES (${i}, ${(i%100)+1})`);
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO outer1 VALUES (${i}, ${(i%100)+1})`);
    db.execute('ANALYZE TABLE main');
    db.execute('ANALYZE TABLE inner1');
    db.execute('ANALYZE TABLE inner2');
    db.execute('ANALYZE TABLE outer1');
    
    // Mix of inner and left joins
    const result = db.execute(`
      SELECT m.id, i1.id as i1, i2.id as i2, o1.id as o1
      FROM main m
      JOIN inner1 i1 ON m.id = i1.main_id
      JOIN inner2 i2 ON m.id = i2.main_id
      LEFT JOIN outer1 o1 ON m.id = o1.main_id
      ORDER BY m.id
    `);
    assert.ok(result.rows.length > 0);
    // Some rows should have NULL o1 (LEFT JOIN)
    const nullOuter = result.rows.filter(r => r.o1 === null);
    assert.ok(nullOuter.length > 0, 'LEFT JOIN should produce some NULLs');
  });

  it('many-to-many join (M:N relationship)', () => {
    const db = new Database();
    db.execute('CREATE TABLE students (id INT, name TEXT)');
    db.execute('CREATE TABLE courses (id INT, name TEXT)');
    db.execute('CREATE TABLE enrollment (student_id INT, course_id INT)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO students VALUES (${i}, 'S${i}')`);
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO courses VALUES (${i}, 'C${i}')`);
    // Each student takes 3 random courses
    for (let s = 1; s <= 20; s++) {
      for (let c = 1; c <= 3; c++) {
        db.execute(`INSERT INTO enrollment VALUES (${s}, ${((s + c) % 10) + 1})`);
      }
    }
    db.execute('ANALYZE TABLE students');
    db.execute('ANALYZE TABLE courses');
    db.execute('ANALYZE TABLE enrollment');
    
    const result = db.execute(`
      SELECT s.name, c.name as course
      FROM students s
      JOIN enrollment e ON s.id = e.student_id
      JOIN courses c ON e.course_id = c.id
      ORDER BY s.name, c.name
    `);
    assert.strictEqual(result.rows.length, 60); // 20 students × 3 courses
  });

  it('join with WHERE filter should still produce correct results', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('CREATE TABLE customers (id INT, region TEXT)');
    db.execute('CREATE TABLE regions (name TEXT, country TEXT)');
    for (let i = 1; i <= 200; i++) {
      db.execute(`INSERT INTO orders VALUES (${i}, ${(i%20)+1}, ${i * 10})`);
    }
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO customers VALUES (${i}, 'R${(i%3)+1}')`);
    }
    db.execute(`INSERT INTO regions VALUES ('R1', 'US')`);
    db.execute(`INSERT INTO regions VALUES ('R2', 'UK')`);
    db.execute(`INSERT INTO regions VALUES ('R3', 'CA')`);
    db.execute('ANALYZE TABLE orders');
    db.execute('ANALYZE TABLE customers');
    db.execute('ANALYZE TABLE regions');
    
    const result = db.execute(`
      SELECT o.id, c.region, r.country
      FROM orders o
      JOIN customers c ON o.customer_id = c.id
      JOIN regions r ON c.region = r.name
      WHERE o.amount > 1500
      ORDER BY o.id
    `);
    assert.ok(result.rows.length > 0);
    for (const row of result.rows) {
      assert.ok(row.id > 150); // amount > 1500 means id > 150
      assert.ok(['US', 'UK', 'CA'].includes(row.country));
    }
  });
});
