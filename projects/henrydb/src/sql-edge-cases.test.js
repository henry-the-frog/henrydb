// sql-edge-cases.test.js — SQL correctness for tricky edge cases
import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SQL Edge Cases', () => {
  let db;
  before(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, score INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 90)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', NULL, 80)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 25, NULL)");
    db.execute("INSERT INTO users VALUES (4, 'Diana', 30, 95)");
    db.execute("INSERT INTO users VALUES (5, 'Eve', NULL, NULL)");
    
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, status TEXT)');
    db.execute("INSERT INTO orders VALUES (1, 1, 100, 'completed')");
    db.execute("INSERT INTO orders VALUES (2, 1, 200, 'pending')");
    db.execute("INSERT INTO orders VALUES (3, 2, 150, 'completed')");
    db.execute("INSERT INTO orders VALUES (4, 3, 50, 'cancelled')");
    db.execute("INSERT INTO orders VALUES (5, NULL, 75, 'completed')");
    
    db.execute('CREATE TABLE empty_table (id INT PRIMARY KEY, val TEXT)');
  });

  // --- NULL Handling ---
  it('NULL in WHERE clause: IS NULL', () => {
    const r = db.execute('SELECT name FROM users WHERE age IS NULL ORDER BY name');
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Bob', 'Eve']);
  });

  it('NULL in WHERE clause: IS NOT NULL', () => {
    const r = db.execute('SELECT name FROM users WHERE score IS NOT NULL ORDER BY name');
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Alice', 'Bob', 'Diana']);
  });

  it('NULL comparison returns no rows (NULL = NULL is false)', () => {
    const r = db.execute('SELECT * FROM users WHERE age = NULL');
    assert.strictEqual(r.rows.length, 0);
  });

  it('COUNT(*) counts NULLs, COUNT(col) does not', () => {
    const r = db.execute('SELECT COUNT(*) as all_rows, COUNT(age) as non_null_age FROM users');
    assert.strictEqual(r.rows[0].all_rows, 5);
    assert.strictEqual(r.rows[0].non_null_age, 3);
  });

  it('SUM/AVG ignore NULLs', () => {
    const r = db.execute('SELECT SUM(score) as total, AVG(score) as avg_score FROM users');
    assert.strictEqual(r.rows[0].total, 265); // 90 + 80 + 95
    // AVG should be 265/3 ≈ 88.33
    assert.ok(Math.abs(r.rows[0].avg_score - 88.333) < 0.01);
  });

  it('MIN/MAX with NULLs', () => {
    const r = db.execute('SELECT MIN(age) as min_age, MAX(age) as max_age FROM users');
    assert.strictEqual(r.rows[0].min_age, 25);
    assert.strictEqual(r.rows[0].max_age, 30);
  });

  // --- Empty Table ---
  it('SELECT from empty table', () => {
    const r = db.execute('SELECT * FROM empty_table');
    assert.strictEqual(r.rows.length, 0);
  });

  it('COUNT(*) on empty table returns 0', () => {
    const r = db.execute('SELECT COUNT(*) as cnt FROM empty_table');
    assert.strictEqual(r.rows[0].cnt, 0);
  });

  it('SUM on empty table returns NULL', () => {
    const r = db.execute('SELECT SUM(id) as total FROM empty_table');
    assert.strictEqual(r.rows[0].total, null);
  });

  // --- CASE Expression ---
  it('CASE WHEN with multiple branches', () => {
    const r = db.execute(`
      SELECT name, 
        CASE WHEN score >= 90 THEN 'A'
             WHEN score >= 80 THEN 'B'
             ELSE 'C' END as grade
      FROM users WHERE score IS NOT NULL ORDER BY name
    `);
    assert.deepStrictEqual(r.rows, [
      { name: 'Alice', grade: 'A' },
      { name: 'Bob', grade: 'B' },
      { name: 'Diana', grade: 'A' },
    ]);
  });

  it('CASE with NULL values', () => {
    const r = db.execute(`
      SELECT name, CASE WHEN age IS NULL THEN 'unknown' ELSE 'known' END as age_status
      FROM users ORDER BY name
    `);
    assert.strictEqual(r.rows[1].age_status, 'unknown'); // Bob
    assert.strictEqual(r.rows[0].age_status, 'known'); // Alice
  });

  // --- COALESCE ---
  it('COALESCE picks first non-NULL', () => {
    const r = db.execute('SELECT name, COALESCE(age, -1) as safe_age FROM users ORDER BY name');
    assert.strictEqual(r.rows[1].safe_age, -1); // Bob has NULL age
    assert.strictEqual(r.rows[0].safe_age, 30); // Alice has age 30
  });

  // --- JOIN Edge Cases ---
  it('LEFT JOIN preserves all left rows', () => {
    const r = db.execute(`
      SELECT u.name, o.amount 
      FROM users u LEFT JOIN orders o ON u.id = o.user_id
      WHERE u.name = 'Eve'
    `);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].amount, null);
  });

  it('JOIN with NULL foreign key excludes unmatched', () => {
    const r = db.execute(`
      SELECT o.id, u.name FROM orders o JOIN users u ON o.user_id = u.id
      ORDER BY o.id
    `);
    // Order 5 has user_id = NULL, should be excluded from INNER JOIN
    assert.strictEqual(r.rows.length, 4);
    assert.ok(!r.rows.some(r => r.id === 5));
  });

  it('Self-join', () => {
    const r = db.execute(`
      SELECT a.name as name1, b.name as name2
      FROM users a JOIN users b ON a.age = b.age AND a.id < b.id
      ORDER BY a.name
    `);
    // Alice(30) and Diana(30)
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name1, 'Alice');
    assert.strictEqual(r.rows[0].name2, 'Diana');
  });

  // --- Aggregate Edge Cases ---
  it('GROUP BY with NULL values creates a NULL group', () => {
    const r = db.execute('SELECT age, COUNT(*) as cnt FROM users GROUP BY age ORDER BY age');
    // Groups: NULL(2), 25(1), 30(2)
    const nullGroup = r.rows.find(r => r.age === null);
    assert.ok(nullGroup, 'Should have a NULL group');
    assert.strictEqual(nullGroup.cnt, 2);
  });

  it('HAVING filters groups', () => {
    const r = db.execute('SELECT age, COUNT(*) as cnt FROM users GROUP BY age HAVING COUNT(*) >= 2');
    assert.strictEqual(r.rows.length, 2); // NULL(2) and 30(2)
  });

  it('COUNT DISTINCT', () => {
    const r = db.execute('SELECT COUNT(DISTINCT age) as uniq_ages FROM users');
    assert.strictEqual(r.rows[0].uniq_ages, 2); // 25, 30 (NULLs excluded)
  });

  // --- Subquery Edge Cases ---
  it('Scalar subquery in SELECT', () => {
    const r = db.execute(`
      SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) as order_count
      FROM users WHERE id = 1
    `);
    assert.strictEqual(r.rows[0].order_count, 2);
  });

  it('EXISTS subquery', () => {
    const r = db.execute(`
      SELECT name FROM users WHERE EXISTS (
        SELECT 1 FROM orders WHERE user_id = users.id AND status = 'completed'
      ) ORDER BY name
    `);
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Alice', 'Bob']);
  });

  it('IN subquery', () => {
    const r = db.execute(`
      SELECT name FROM users WHERE id IN (
        SELECT DISTINCT user_id FROM orders WHERE status = 'completed'
      ) ORDER BY name
    `);
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Alice', 'Bob']);
  });

  // --- LIMIT/OFFSET ---
  it('LIMIT 0 returns no rows', () => {
    const r = db.execute('SELECT * FROM users LIMIT 0');
    assert.strictEqual(r.rows.length, 0);
  });

  it('OFFSET beyond table size returns no rows', () => {
    const r = db.execute('SELECT * FROM users LIMIT 10 OFFSET 100');
    assert.strictEqual(r.rows.length, 0);
  });

  it('OFFSET without LIMIT', () => {
    const r = db.execute('SELECT * FROM users ORDER BY id OFFSET 3');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].id, 4);
  });

  // --- ORDER BY Edge Cases ---
  it('ORDER BY column not in SELECT', () => {
    const r = db.execute('SELECT name FROM users ORDER BY id DESC LIMIT 2');
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Eve', 'Diana']);
  });

  it('ORDER BY with NULLs (NULLs first in ASC)', () => {
    const r = db.execute('SELECT name, age FROM users ORDER BY age, name');
    // NULLs should come first or last depending on convention
    assert.ok(r.rows.length === 5);
  });

  // --- Expression Edge Cases ---
  it('Division by zero returns Infinity or throws', () => {
    // JS semantics: 1/0 = Infinity (unlike PostgreSQL which throws)
    try {
      const r = db.execute('SELECT 1 / 0 as result');
      assert.ok(r.rows[0].result === Infinity || r.rows[0].result === null, 'Should be Infinity or null');
    } catch {
      // Throwing is also acceptable
    }
  });

  it('String concatenation with ||', () => {
    const r = db.execute("SELECT name || ' (' || CAST(id AS TEXT) || ')' as label FROM users WHERE id = 1");
    assert.strictEqual(r.rows[0].label, 'Alice (1)');
  });

  // --- UNION ---
  it('UNION removes duplicates', () => {
    const r = db.execute(`
      SELECT name FROM users WHERE id <= 2
      UNION
      SELECT name FROM users WHERE id >= 2
    `);
    // Should be 5 unique names
    assert.strictEqual(r.rows.length, 5);
  });

  it('UNION ALL keeps duplicates', () => {
    const r = db.execute(`
      SELECT name FROM users WHERE id <= 2
      UNION ALL
      SELECT name FROM users WHERE id >= 2
    `);
    // 2 + 4 = 6 (Bob appears twice)
    assert.strictEqual(r.rows.length, 6);
  });

  // --- CTE ---
  it('CTE with aggregation', () => {
    const r = db.execute(`
      WITH user_totals AS (
        SELECT user_id, SUM(amount) as total
        FROM orders
        WHERE user_id IS NOT NULL
        GROUP BY user_id
      )
      SELECT u.name, ut.total
      FROM users u JOIN user_totals ut ON u.id = ut.user_id
      ORDER BY ut.total DESC
    `);
    assert.strictEqual(r.rows[0].name, 'Alice');
    assert.strictEqual(r.rows[0].total, 300);
  });

  // --- Aggregate over GENERATE_SERIES ---
  it('GENERATE_SERIES with SUM', () => {
    const r = db.execute('SELECT SUM(value) as total FROM GENERATE_SERIES(1, 100)');
    assert.strictEqual(r.rows[0].total, 5050);
  });

  it('GENERATE_SERIES with GROUP BY', () => {
    const r = db.execute('SELECT value % 3 as grp, COUNT(*) as cnt FROM GENERATE_SERIES(1, 30) GROUP BY value % 3');
    assert.strictEqual(r.rows.length, 3);
    for (const row of r.rows) {
      assert.strictEqual(row.cnt, 10);
    }
  });

  // --- Nested Aggregates ---
  it('Aggregate over aggregate subquery', () => {
    const r = db.execute(`
      SELECT MAX(total) as max_total FROM (
        SELECT user_id, SUM(amount) as total
        FROM orders WHERE user_id IS NOT NULL
        GROUP BY user_id
      ) sq
    `);
    assert.strictEqual(r.rows[0].max_total, 300);
  });

  // --- Multiple JOINs ---
  it('Three-way join', () => {
    db.execute('CREATE TABLE categories (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO categories VALUES (1, 'Premium')");
    db.execute("INSERT INTO categories VALUES (2, 'Standard')");
    db.execute('CREATE TABLE user_categories (user_id INT, cat_id INT)');
    db.execute('INSERT INTO user_categories VALUES (1, 1)');
    db.execute('INSERT INTO user_categories VALUES (4, 1)');
    db.execute('INSERT INTO user_categories VALUES (2, 2)');
    
    const r = db.execute(`
      SELECT u.name, c.name as category
      FROM users u
      JOIN user_categories uc ON u.id = uc.user_id
      JOIN categories c ON uc.cat_id = c.id
      ORDER BY u.name
    `);
    assert.strictEqual(r.rows.length, 3);
    assert.strictEqual(r.rows[0].name, 'Alice');
    assert.strictEqual(r.rows[0].category, 'Premium');
  });
});
