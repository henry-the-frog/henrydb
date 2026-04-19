// sql-compliance-edge.test.js — SQL compliance edge cases from real DB workloads
// Tests patterns commonly found in ORMs, analytics tools, and web applications

import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SQL Compliance Edge Cases', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT, dept TEXT, salary INT)");
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT, status TEXT, created_at TEXT)");
    db.execute("CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price INT, category TEXT)");
    db.execute("CREATE TABLE order_items (order_id INT, product_id INT, qty INT)");
    
    // Seed data
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 'Engineering', 90000)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, 'Sales', 70000)");
    db.execute("INSERT INTO users VALUES (3, 'Carol', 35, 'Engineering', 95000)");
    db.execute("INSERT INTO users VALUES (4, 'Dave', 28, 'Marketing', 65000)");
    db.execute("INSERT INTO users VALUES (5, 'Eve', 32, 'Sales', 80000)");
    
    db.execute("INSERT INTO orders VALUES (1, 1, 100, 'completed', '2024-01-01')");
    db.execute("INSERT INTO orders VALUES (2, 1, 200, 'completed', '2024-02-01')");
    db.execute("INSERT INTO orders VALUES (3, 2, 150, 'pending', '2024-03-01')");
    db.execute("INSERT INTO orders VALUES (4, 3, 300, 'completed', '2024-01-15')");
    db.execute("INSERT INTO orders VALUES (5, 4, 50, 'cancelled', '2024-02-20')");
    
    db.execute("INSERT INTO products VALUES (1, 'Widget', 10, 'A')");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 25, 'B')");
    db.execute("INSERT INTO products VALUES (3, 'Doohickey', 5, 'A')");
    
    db.execute("INSERT INTO order_items VALUES (1, 1, 5)");
    db.execute("INSERT INTO order_items VALUES (1, 2, 2)");
    db.execute("INSERT INTO order_items VALUES (2, 3, 10)");
    db.execute("INSERT INTO order_items VALUES (3, 1, 3)");
    db.execute("INSERT INTO order_items VALUES (4, 2, 1)");
  });
  
  // === GROUP BY with expressions ===
  
  it('GROUP BY with CASE expression', () => {
    const r = db.execute(`
      SELECT CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END AS bracket,
             COUNT(*) AS cnt
      FROM users
      GROUP BY CASE WHEN salary > 80000 THEN 'high' ELSE 'low' END
      ORDER BY cnt DESC
    `);
    assert.equal(r.rows.length, 2);
  });
  
  it('GROUP BY ordinal position', () => {
    const r = db.execute("SELECT dept, COUNT(*) AS cnt FROM users GROUP BY 1 ORDER BY 2 DESC");
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].dept, 'Engineering');
    assert.equal(r.rows[0].cnt, 2);
  });
  
  // === Subquery patterns ===
  
  it('scalar subquery in SELECT', () => {
    const r = db.execute(`
      SELECT u.name, 
             (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count
      FROM users u
      ORDER BY order_count DESC, u.name
    `);
    assert.equal(r.rows.length, 5);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].order_count, 2);
  });
  
  it('IN with subquery', () => {
    const r = db.execute(`
      SELECT name FROM users 
      WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed')
      ORDER BY name
    `);
    assert.ok(r.rows.length >= 2);
    assert.deepEqual(r.rows.map(x => x.name), ['Alice', 'Carol']);
  });
  
  it('EXISTS subquery', () => {
    const r = db.execute(`
      SELECT name FROM users u
      WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 100)
      ORDER BY name
    `);
    assert.ok(r.rows.length >= 2);
  });
  
  it('NOT EXISTS subquery', () => {
    const r = db.execute(`
      SELECT name FROM users u
      WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)
      ORDER BY name
    `);
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Eve');
  });

  // === JOIN patterns ===
  
  it('three-way JOIN', () => {
    const r = db.execute(`
      SELECT u.name, p.name AS product, oi.qty
      FROM users u
      INNER JOIN orders o ON o.user_id = u.id
      INNER JOIN order_items oi ON oi.order_id = o.id
      INNER JOIN products p ON p.id = oi.product_id
      ORDER BY u.name, p.name
    `);
    assert.ok(r.rows.length >= 4);
  });
  
  it('LEFT JOIN with NULL handling', () => {
    const r = db.execute(`
      SELECT u.name, COALESCE(SUM(o.amount), 0) AS total_spent
      FROM users u
      LEFT JOIN orders o ON o.user_id = u.id
      GROUP BY u.name
      ORDER BY total_spent DESC
    `);
    assert.equal(r.rows.length, 5);
    // Eve has no orders
    const eve = r.rows.find(x => x.name === 'Eve');
    assert.equal(eve.total_spent, 0);
  });
  
  it('self JOIN', () => {
    const r = db.execute(`
      SELECT u1.name, u2.name AS colleague
      FROM users u1
      INNER JOIN users u2 ON u1.dept = u2.dept AND u1.id < u2.id
      ORDER BY u1.name
    `);
    assert.ok(r.rows.length >= 1);
  });
  
  // === Aggregate patterns ===
  
  it('HAVING with aggregate', () => {
    const r = db.execute(`
      SELECT dept, AVG(salary) AS avg_sal
      FROM users
      GROUP BY dept
      HAVING AVG(salary) > 75000
      ORDER BY avg_sal DESC
    `);
    assert.ok(r.rows.length >= 1);
    assert.equal(r.rows[0].dept, 'Engineering');
  });
  
  it('multiple aggregates in one query', () => {
    const r = db.execute(`
      SELECT dept, 
             COUNT(*) AS cnt, 
             MIN(salary) AS min_sal, 
             MAX(salary) AS max_sal, 
             AVG(salary) AS avg_sal,
             SUM(salary) AS total_sal
      FROM users
      GROUP BY dept
      ORDER BY dept
    `);
    assert.equal(r.rows.length, 3);
    const eng = r.rows.find(x => x.dept === 'Engineering');
    assert.equal(eng.cnt, 2);
    assert.equal(eng.min_sal, 90000);
    assert.equal(eng.max_sal, 95000);
  });
  
  // === Expression edge cases ===
  
  it('CASE WHEN with multiple conditions', () => {
    const r = db.execute(`
      SELECT name, 
             CASE 
               WHEN age < 26 THEN 'junior'
               WHEN age BETWEEN 26 AND 32 THEN 'mid'
               ELSE 'senior'
             END AS level
      FROM users
      ORDER BY name
    `);
    assert.equal(r.rows[0].level, 'mid'); // Alice, 30
    assert.equal(r.rows[1].level, 'junior'); // Bob, 25
    assert.equal(r.rows[2].level, 'senior'); // Carol, 35
  });
  
  it('arithmetic in SELECT and WHERE', () => {
    const r = db.execute(`
      SELECT name, salary * 12 AS annual, salary / 1000 AS k
      FROM users
      WHERE salary * 12 > 900000
      ORDER BY annual DESC
    `);
    assert.ok(r.rows.length >= 1);
    assert.equal(r.rows[0].annual, 95000 * 12);
  });
  
  it('string concatenation with ||', () => {
    const r = db.execute("SELECT name || ' (' || dept || ')' AS display FROM users WHERE id = 1");
    assert.equal(r.rows[0].display, 'Alice (Engineering)');
  });
  
  // === NULL handling ===
  
  it('IS NULL and IS NOT NULL', () => {
    db.execute("INSERT INTO users VALUES (6, 'Frank', NULL, 'HR', NULL)");
    
    const r1 = db.execute("SELECT name FROM users WHERE age IS NULL");
    assert.equal(r1.rows.length, 1);
    assert.equal(r1.rows[0].name, 'Frank');
    
    const r2 = db.execute("SELECT name FROM users WHERE salary IS NOT NULL ORDER BY name");
    assert.equal(r2.rows.length, 5);
  });
  
  it('COALESCE with NULL', () => {
    db.execute("INSERT INTO users VALUES (6, 'Frank', NULL, 'HR', NULL)");
    const r = db.execute("SELECT name, COALESCE(salary, 0) AS sal FROM users WHERE id = 6");
    assert.equal(r.rows[0].sal, 0);
  });
  
  // === DISTINCT ===
  
  it('SELECT DISTINCT', () => {
    const r = db.execute("SELECT DISTINCT dept FROM users ORDER BY dept");
    assert.equal(r.rows.length, 3);
  });
  
  it('COUNT(DISTINCT)', () => {
    const r = db.execute("SELECT COUNT(DISTINCT dept) AS dept_count FROM users");
    assert.equal(r.rows[0].dept_count, 3);
  });
  
  // === UNION / set operations ===
  
  it('UNION removes duplicates', () => {
    const r = db.execute(`
      SELECT dept FROM users WHERE id = 1
      UNION
      SELECT dept FROM users WHERE id = 3
    `);
    assert.equal(r.rows.length, 1); // Both Engineering
  });
  
  it('UNION ALL keeps duplicates', () => {
    const r = db.execute(`
      SELECT dept FROM users WHERE id = 1
      UNION ALL
      SELECT dept FROM users WHERE id = 3
    `);
    assert.equal(r.rows.length, 2);
  });
  
  // === ORDER BY edge cases ===
  
  it('ORDER BY expression', () => {
    const r = db.execute("SELECT name, salary FROM users ORDER BY salary * -1");
    assert.equal(r.rows[0].name, 'Carol'); // Highest salary first (ORDER BY -salary ASC)
    assert.equal(r.rows[r.rows.length - 1].name, 'Dave'); // Lowest salary last
  });
  
  it('ORDER BY alias', () => {
    const r = db.execute("SELECT name, salary AS pay FROM users ORDER BY pay DESC");
    assert.equal(r.rows[0].name, 'Carol');
  });
  
  // === LIMIT/OFFSET ===
  
  it('LIMIT with OFFSET', () => {
    const r = db.execute("SELECT name FROM users ORDER BY name LIMIT 2 OFFSET 1");
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'Bob');
    assert.equal(r.rows[1].name, 'Carol');
  });
  
  // === UPDATE with complex expressions ===
  
  it('UPDATE with arithmetic', () => {
    db.execute("UPDATE users SET salary = salary + salary / 10 WHERE dept = 'Engineering'");
    const r = db.execute("SELECT salary FROM users WHERE id = 1");
    assert.equal(r.rows[0].salary, 99000);
  });
  
  it('UPDATE with CASE', () => {
    db.execute(`
      UPDATE users SET salary = CASE 
        WHEN dept = 'Engineering' THEN salary + 10000
        WHEN dept = 'Sales' THEN salary + 5000
        ELSE salary
      END
    `);
    const r = db.execute("SELECT salary FROM users WHERE id = 1");
    assert.equal(r.rows[0].salary, 100000);
  });
  
  // === DELETE patterns ===
  
  it('DELETE with subquery', () => {
    db.execute("DELETE FROM orders WHERE user_id NOT IN (SELECT id FROM users WHERE dept = 'Engineering')");
    const r = db.execute("SELECT COUNT(*) AS cnt FROM orders");
    assert.equal(r.rows[0].cnt, 3); // Only Alice(1) and Carol(3) orders remain
  });
  
  // === INSERT patterns ===
  
  it('INSERT with DEFAULT values', () => {
    db.execute("CREATE TABLE logs (id INT, msg TEXT DEFAULT 'no message', level INT DEFAULT 0)");
    db.execute("INSERT INTO logs (id) VALUES (1)");
    const r = db.execute("SELECT * FROM logs WHERE id = 1");
    assert.equal(r.rows[0].msg, 'no message');
    assert.equal(r.rows[0].level, 0);
  });
  
  it('INSERT ... SELECT', () => {
    db.execute("CREATE TABLE user_backup (id INT, name TEXT)");
    db.execute("INSERT INTO user_backup SELECT id, name FROM users WHERE dept = 'Engineering'");
    const r = db.execute("SELECT COUNT(*) AS cnt FROM user_backup");
    assert.equal(r.rows[0].cnt, 2);
  });
});

describe('Real-world ORM Patterns', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
  });
  
  it('Prisma-style create with RETURNING', () => {
    db.execute("CREATE TABLE posts (id SERIAL PRIMARY KEY, title TEXT NOT NULL, content TEXT, published BOOLEAN DEFAULT false)");
    const r = db.execute("INSERT INTO posts (title, content) VALUES ('Hello', 'World') RETURNING *");
    assert.equal(r.rows[0].title, 'Hello');
    assert.equal(r.rows[0].published, false);
    assert.ok(r.rows[0].id > 0);
  });
  
  it('Sequelize-style findAll with conditions', () => {
    db.execute("CREATE TABLE tasks (id INT PRIMARY KEY, name TEXT, status TEXT, priority INT)");
    db.execute("INSERT INTO tasks VALUES (1, 'A', 'open', 1)");
    db.execute("INSERT INTO tasks VALUES (2, 'B', 'done', 2)");
    db.execute("INSERT INTO tasks VALUES (3, 'C', 'open', 3)");
    
    const r = db.execute(`
      SELECT * FROM tasks 
      WHERE status = 'open' AND priority >= 1 
      ORDER BY priority ASC 
      LIMIT 10
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'A');
  });
  
  it('TypeORM-style migration with multiple operations', () => {
    db.execute("CREATE TABLE migrations (id INT PRIMARY KEY, name TEXT, executed_at TEXT)");
    db.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)");
    db.execute("INSERT INTO settings VALUES ('version', '1.0')");
    
    // Simulate migration
    db.execute("CREATE TABLE new_feature (id INT PRIMARY KEY, data TEXT)");
    db.execute("INSERT INTO new_feature VALUES (1, 'test')");
    db.execute("UPDATE settings SET value = '1.1' WHERE key = 'version'");
    db.execute("INSERT INTO migrations (id, name, executed_at) VALUES (1, 'add_new_feature', '2024-01-01')");
    
    const ver = db.execute("SELECT value FROM settings WHERE key = 'version'");
    assert.equal(ver.rows[0].value, '1.1');
    
    const mig = db.execute("SELECT COUNT(*) AS cnt FROM migrations");
    assert.equal(mig.rows[0].cnt, 1);
  });
  
  it('Drizzle-style upsert pattern', () => {
    db.execute("CREATE TABLE sessions (id TEXT PRIMARY KEY, user_id INT, data TEXT, updated_at TEXT DEFAULT 'now')");
    
    // First insert
    db.execute("INSERT INTO sessions (id, user_id, data) VALUES ('abc', 1, 'initial') ON CONFLICT (id) DO UPDATE SET data = 'updated'");
    const r1 = db.execute("SELECT data FROM sessions WHERE id = 'abc'");
    assert.equal(r1.rows[0].data, 'initial');
    
    // Upsert — should update
    db.execute("INSERT INTO sessions (id, user_id, data) VALUES ('abc', 1, 'new') ON CONFLICT (id) DO UPDATE SET data = 'updated'");
    const r2 = db.execute("SELECT data FROM sessions WHERE id = 'abc'");
    assert.equal(r2.rows[0].data, 'updated');
  });
});
