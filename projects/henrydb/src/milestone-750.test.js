// milestone-750.test.js — Push to 750!
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('🎯 750 Test Milestone', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('complex multi-table analytics', () => {
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, pid INT, qty INT)');
    db.execute("INSERT INTO products VALUES (1, 'Widget', 10)");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 20)");
    db.execute('INSERT INTO orders VALUES (1, 1, 5)');
    db.execute('INSERT INTO orders VALUES (2, 2, 3)');
    db.execute('INSERT INTO orders VALUES (3, 1, 2)');
    const result = db.execute('SELECT p.name, SUM(o.qty) AS total_qty FROM products p JOIN orders o ON p.id = o.pid GROUP BY p.name ORDER BY total_qty DESC');
    assert.equal(result.rows[0].name, 'Widget');
    assert.equal(result.rows[0].total_qty, 7);
  });

  it('CTE + window + WHERE combo', () => {
    db.execute('CREATE TABLE sales (id INT PRIMARY KEY, region TEXT, amount INT)');
    for (let i = 1; i <= 6; i++) {
      db.execute(`INSERT INTO sales VALUES (${i}, '${i <= 3 ? 'North' : 'South'}', ${i * 100})`);
    }
    const result = db.execute(`
      WITH ranked AS (
        SELECT region, amount, ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
        FROM sales
      )
      SELECT * FROM ranked WHERE rn = 1 ORDER BY amount DESC
    `);
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].amount, 600); // South max
  });

  it('self-join to find duplicates', () => {
    db.execute('CREATE TABLE contacts (id INT PRIMARY KEY, email TEXT)');
    db.execute("INSERT INTO contacts VALUES (1, 'a@b.com')");
    db.execute("INSERT INTO contacts VALUES (2, 'c@d.com')");
    db.execute("INSERT INTO contacts VALUES (3, 'a@b.com')");
    const result = db.execute('SELECT DISTINCT c1.email FROM contacts c1 JOIN contacts c2 ON c1.email = c2.email AND c1.id < c2.id');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].email, 'a@b.com');
  });

  it('CASE in ORDER BY', () => {
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, priority TEXT)');
    db.execute("INSERT INTO items VALUES (1, 'low')");
    db.execute("INSERT INTO items VALUES (2, 'high')");
    db.execute("INSERT INTO items VALUES (3, 'medium')");
    const result = db.execute("SELECT id, priority, CASE priority WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END AS sort_order FROM items ORDER BY sort_order");
    assert.equal(result.rows[0].priority, 'high');
  });

  it('🎯 750th test — nested subquery with aggregation', () => {
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, student TEXT, subject TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES (1, 'Alice', 'Math', 90)");
    db.execute("INSERT INTO scores VALUES (2, 'Alice', 'Science', 85)");
    db.execute("INSERT INTO scores VALUES (3, 'Bob', 'Math', 75)");
    db.execute("INSERT INTO scores VALUES (4, 'Bob', 'Science', 80)");
    db.execute("INSERT INTO scores VALUES (5, 'Charlie', 'Math', 95)");
    db.execute("INSERT INTO scores VALUES (6, 'Charlie', 'Science', 92)");
    // Students whose average is above the class average
    const result = db.execute(`
      SELECT student, AVG(score) AS avg_score
      FROM scores
      GROUP BY student
      HAVING AVG(score) > (SELECT AVG(score) FROM scores)
    `);
    // Class avg: (90+85+75+80+95+92)/6 = 86.17
    // Alice: 87.5, Bob: 77.5, Charlie: 93.5
    // Above class avg: Alice and Charlie
    assert.equal(result.rows.length, 2);
  });
});
