// sql-through-mvcc.test.js — Run SQL features through TransactionalDatabase (MVCC)
// Verifies that advanced SQL works correctly with MVCC visibility
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-sql-mvcc-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

function setup() {
  db = fresh();
  db.execute('CREATE TABLE users (id INT, name TEXT, age INT, dept TEXT)');
  db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT, status TEXT)');
  db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 'Engineering')");
  db.execute("INSERT INTO users VALUES (2, 'Bob', 25, 'Marketing')");
  db.execute("INSERT INTO users VALUES (3, 'Carol', 35, 'Engineering')");
  db.execute("INSERT INTO users VALUES (4, 'Dave', 28, 'Marketing')");
  db.execute("INSERT INTO users VALUES (5, 'Eve', 32, 'Engineering')");
  db.execute("INSERT INTO orders VALUES (100, 1, 500, 'completed')");
  db.execute("INSERT INTO orders VALUES (101, 1, 300, 'pending')");
  db.execute("INSERT INTO orders VALUES (102, 2, 200, 'completed')");
  db.execute("INSERT INTO orders VALUES (103, 3, 800, 'completed')");
  db.execute("INSERT INTO orders VALUES (104, 4, 150, 'cancelled')");
}

describe('SQL Through MVCC', () => {
  afterEach(cleanup);

  describe('JOINs', () => {
    it('INNER JOIN through MVCC', () => {
      setup();
      const r = db.execute('SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC');
      assert.equal(r.rows.length, 5);
      assert.equal(r.rows[0].amount, 800); // Carol
      assert.equal(r.rows[0].name, 'Carol');
    });

    it('LEFT JOIN through MVCC', () => {
      setup();
      const r = db.execute('SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY u.name');
      assert.ok(r.rows.length >= 5);
      const eve = r.rows.find(row => row.name === 'Eve');
      assert.ok(eve, 'Eve should appear in LEFT JOIN even without orders');
      assert.equal(eve.amount, null);
    });

    it('self-join through MVCC', () => {
      setup();
      const r = db.execute(`
        SELECT a.name as emp1, b.name as emp2
        FROM users a JOIN users b ON a.dept = b.dept AND a.id < b.id
        ORDER BY a.name
      `);
      assert.ok(r.rows.length > 0);
    });

    it('multi-table join through MVCC', () => {
      setup();
      db.execute('CREATE TABLE depts (name TEXT, budget INT)');
      db.execute("INSERT INTO depts VALUES ('Engineering', 100000)");
      db.execute("INSERT INTO depts VALUES ('Marketing', 50000)");
      const r = db.execute(`
        SELECT u.name, o.amount, d.budget
        FROM users u
        JOIN orders o ON u.id = o.user_id
        JOIN depts d ON u.dept = d.name
        WHERE o.status = 'completed'
        ORDER BY o.amount DESC
      `);
      assert.ok(r.rows.length >= 3);
      assert.equal(r.rows[0].name, 'Carol');
    });
  });

  describe('Subqueries', () => {
    it('scalar subquery in WHERE', () => {
      setup();
      const r = db.execute('SELECT name FROM users WHERE age > (SELECT AVG(age) FROM users) ORDER BY name');
      assert.ok(r.rows.length > 0);
      // Average age = 30, so Carol (35) and Eve (32) qualify
      assert.ok(r.rows.some(row => row.name === 'Carol'));
      assert.ok(r.rows.some(row => row.name === 'Eve'));
    });

    it('IN subquery', () => {
      setup();
      const r = db.execute(`
        SELECT name FROM users
        WHERE id IN (SELECT user_id FROM orders WHERE status = 'completed')
        ORDER BY name
      `);
      assert.ok(r.rows.length >= 2);
      assert.ok(r.rows.some(row => row.name === 'Alice'));
      assert.ok(r.rows.some(row => row.name === 'Carol'));
    });

    it('EXISTS subquery', () => {
      setup();
      const r = db.execute(`
        SELECT name FROM users u
        WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id AND o.amount > 400)
        ORDER BY name
      `);
      assert.ok(r.rows.some(row => row.name === 'Alice'));
      assert.ok(r.rows.some(row => row.name === 'Carol'));
    });

    it('subquery in SELECT', () => {
      setup();
      const r = db.execute(`
        SELECT name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) as order_count
        FROM users u
        ORDER BY name
      `);
      const alice = r.rows.find(row => row.name === 'Alice');
      assert.equal(alice.order_count, 2);
    });
  });

  describe('CTEs', () => {
    it('basic CTE', () => {
      setup();
      const r = db.execute(`
        WITH eng AS (SELECT * FROM users WHERE dept = 'Engineering')
        SELECT name, age FROM eng ORDER BY age DESC
      `);
      assert.equal(r.rows.length, 3);
      assert.equal(r.rows[0].name, 'Carol');
    });

    it('CTE with aggregation', () => {
      setup();
      const r = db.execute(`
        WITH order_totals AS (
          SELECT user_id, SUM(amount) as total
          FROM orders
          WHERE status = 'completed'
          GROUP BY user_id
        )
        SELECT u.name, ot.total
        FROM users u JOIN order_totals ot ON u.id = ot.user_id
        ORDER BY ot.total DESC
      `);
      assert.ok(r.rows.length >= 2);
      assert.equal(r.rows[0].name, 'Carol'); // 800
    });

    it('multiple CTEs', () => {
      setup();
      const r = db.execute(`
        WITH
          eng AS (SELECT id, name FROM users WHERE dept = 'Engineering'),
          eng_orders AS (SELECT e.name, o.amount FROM eng e JOIN orders o ON e.id = o.user_id)
        SELECT name, SUM(amount) as total FROM eng_orders GROUP BY name ORDER BY total DESC
      `);
      assert.ok(r.rows.length > 0);
    });
  });

  describe('Aggregates & GROUP BY', () => {
    it('GROUP BY with HAVING', () => {
      setup();
      const r = db.execute(`
        SELECT dept, COUNT(*) as cnt, AVG(age) as avg_age
        FROM users
        GROUP BY dept
        HAVING COUNT(*) > 2
      `);
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].dept, 'Engineering');
      assert.equal(r.rows[0].cnt, 3);
    });

    it('multiple aggregates', () => {
      setup();
      const r = db.execute(`
        SELECT
          COUNT(*) as cnt,
          SUM(amount) as total,
          MIN(amount) as min_amt,
          MAX(amount) as max_amt,
          AVG(amount) as avg_amt
        FROM orders
        WHERE status = 'completed'
      `);
      assert.equal(r.rows[0].cnt, 3);
      assert.equal(r.rows[0].total, 1500); // 500+200+800
      assert.equal(r.rows[0].min_amt, 200);
      assert.equal(r.rows[0].max_amt, 800);
    });

    it('GROUP BY with JOIN', () => {
      setup();
      const r = db.execute(`
        SELECT u.dept, SUM(o.amount) as total_orders
        FROM users u
        JOIN orders o ON u.id = o.user_id
        GROUP BY u.dept
        ORDER BY total_orders DESC
      `);
      assert.ok(r.rows.length >= 1);
    });
  });

  describe('MVCC Interaction', () => {
    it('JOIN sees correct MVCC state after UPDATE', () => {
      setup();
      db.execute("UPDATE users SET dept = 'Marketing' WHERE name = 'Carol'");
      const r = db.execute(`
        SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept ORDER BY dept
      `);
      const eng = r.rows.find(row => row.dept === 'Engineering');
      const mkt = r.rows.find(row => row.dept === 'Marketing');
      assert.equal(eng.cnt, 2); // Alice, Eve
      assert.equal(mkt.cnt, 3); // Bob, Carol, Dave
    });

    it('subquery sees correct state after DELETE', () => {
      setup();
      db.execute("DELETE FROM orders WHERE status = 'cancelled'");
      const r = db.execute(`
        SELECT name FROM users
        WHERE id NOT IN (SELECT user_id FROM orders)
        ORDER BY name
      `);
      // Dave's only order was cancelled and deleted, so he should appear
      // Eve never had orders
      assert.ok(r.rows.some(row => row.name === 'Eve'));
      assert.ok(r.rows.some(row => row.name === 'Dave'));
    });

    it('CTE sees mid-transaction state', () => {
      setup();
      const s = db.session();
      s.begin();
      s.execute("INSERT INTO users VALUES (6, 'Frank', 40, 'Engineering')");
      const r = s.execute(`
        WITH eng AS (SELECT * FROM users WHERE dept = 'Engineering')
        SELECT COUNT(*) as cnt FROM eng
      `);
      assert.equal(r.rows[0].cnt, 4); // Alice, Carol, Eve + Frank
      s.rollback();
      s.close();
      // After rollback, Frank shouldn't exist
      const r2 = db.execute(`
        WITH eng AS (SELECT * FROM users WHERE dept = 'Engineering')
        SELECT COUNT(*) as cnt FROM eng
      `);
      assert.equal(r2.rows[0].cnt, 3);
    });

    it('aggregates reflect MVCC-deleted rows', () => {
      setup();
      const s1 = db.session();
      const s2 = db.session();
      s1.begin();
      s2.begin();
      // s1 deletes an order
      s1.execute("DELETE FROM orders WHERE id = 100");
      // s2 still sees the old state
      const r1 = s2.execute('SELECT SUM(amount) as total FROM orders');
      assert.equal(r1.rows[0].total, 1950); // All orders visible to s2
      s1.commit();
      s2.commit();
      // New query should reflect deletion
      const r2 = db.execute('SELECT SUM(amount) as total FROM orders');
      assert.equal(r2.rows[0].total, 1450); // 300+200+800+150
      s1.close();
      s2.close();
    });
  });
});
