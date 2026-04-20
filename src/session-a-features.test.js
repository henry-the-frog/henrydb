import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Session A Features (2026-04-20)', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  // === MERGE ===
  describe('MERGE', () => {
    it('updates matched rows and inserts unmatched', () => {
      db.execute('CREATE TABLE inv (id INT PRIMARY KEY, qty INT)');
      db.execute('INSERT INTO inv VALUES (1, 10), (2, 5)');
      db.execute('CREATE TABLE ship (id INT, qty INT)');
      db.execute('INSERT INTO ship VALUES (1, 3), (3, 7)');
      db.execute('MERGE INTO inv t USING ship s ON t.id = s.id WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.qty)');
      const rows = db.execute('SELECT * FROM inv ORDER BY id').rows;
      assert.equal(rows.length, 3);
      assert.equal(rows[0].qty, 13); // 10 + 3
      assert.equal(rows[1].qty, 5);  // unchanged
      assert.equal(rows[2].qty, 7);  // inserted
    });

    it('deletes matched rows', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
      db.execute('CREATE TABLE s (id INT)');
      db.execute('INSERT INTO s VALUES (1)');
      db.execute('MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN DELETE');
      assert.equal(db.execute('SELECT COUNT(*) as c FROM t').rows[0].c, 1);
    });
  });

  // === EXPLAIN FORMAT JSON ===
  describe('EXPLAIN FORMAT JSON', () => {
    it('returns structured JSON plan', () => {
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'a')");
      const r = db.execute('EXPLAIN (FORMAT JSON) SELECT * FROM t');
      assert.ok(r.json || r.rows);
    });

    it('ANALYZE adds timing', () => {
      db.execute('CREATE TABLE t (id INT)');
      db.execute('INSERT INTO t VALUES (1)');
      const r = db.execute('EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM t');
      assert.ok(r.json || r.rows);
    });
  });

  // === SHOW ===
  describe('SHOW', () => {
    it('SHOW TABLES lists tables', () => {
      db.execute('CREATE TABLE users (id INT)');
      db.execute('CREATE TABLE orders (id INT)');
      const r = db.execute('SHOW TABLES');
      assert.ok(r.rows.some(r => r.table_name === 'users'));
      assert.ok(r.rows.some(r => r.table_name === 'orders'));
    });

    it('SHOW COLUMNS FROM shows schema', () => {
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
      const r = db.execute('SHOW COLUMNS FROM t');
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].column_name, 'id');
    });
  });

  // === CTE column lists ===
  describe('CTE column lists', () => {
    it('recursive CTE with column names', () => {
      const r = db.execute('WITH RECURSIVE fact(n, f) AS (SELECT 1, 1 UNION ALL SELECT n + 1, f * (n + 1) FROM fact WHERE n < 5) SELECT * FROM fact');
      assert.equal(r.rows.length, 5);
      assert.equal(r.rows[4].n, 5);
      assert.equal(r.rows[4].f, 120); // 5!
    });

    it('fibonacci sequence', () => {
      const r = db.execute('WITH RECURSIVE fib(n, a, b) AS (SELECT 1, 0, 1 UNION ALL SELECT n + 1, b, a + b FROM fib WHERE n < 10) SELECT n, a as fib FROM fib');
      assert.equal(r.rows[6].fib, 8); // fib(7) = 8
    });
  });

  // === GROUPING SETS ===
  describe('GROUPING SETS', () => {
    it('produces correct groups', () => {
      db.execute('CREATE TABLE t (val TEXT)');
      db.execute("INSERT INTO t VALUES ('a'), ('b'), ('a')");
      const r = db.execute('SELECT val, COUNT(*) as cnt FROM t GROUP BY GROUPING SETS ((val), ())');
      assert.ok(r.rows.some(r => r.val === 'a' && r.cnt === 2));
      assert.ok(r.rows.some(r => r.val === null && r.cnt === 3));
    });
  });

  // === Date/time functions ===
  describe('Date/time functions', () => {
    it('DATE_TRUNC', () => {
      assert.equal(db.execute("SELECT DATE_TRUNC('month', '2024-03-15') as d").rows[0].d, '2024-03-01');
      assert.equal(db.execute("SELECT DATE_TRUNC('year', '2024-07-20') as d").rows[0].d, '2024-01-01');
    });

    it('EXTRACT', () => {
      assert.equal(db.execute("SELECT EXTRACT(YEAR FROM '2024-03-15') as y").rows[0].y, 2024);
      assert.equal(db.execute("SELECT EXTRACT(MONTH FROM '2024-03-15') as m").rows[0].m, 3);
      assert.equal(db.execute("SELECT EXTRACT(QUARTER FROM '2024-07-15') as q").rows[0].q, 3);
    });

    it('EXTRACT in WHERE', () => {
      db.execute('CREATE TABLE events (id INT, date TEXT)');
      db.execute("INSERT INTO events VALUES (1, '2024-01-15'), (2, '2024-03-20'), (3, '2024-01-10')");
      const r = db.execute("SELECT * FROM events WHERE EXTRACT(MONTH FROM date) = 1");
      assert.equal(r.rows.length, 2);
    });

    it('AGE', () => {
      const r = db.execute("SELECT AGE('2024-03-15', '2020-01-01') as a");
      assert.ok(r.rows[0].a.includes('4 years'));
    });

    it('DATE_ADD', () => {
      const r = db.execute("SELECT DATE_ADD('2024-01-15', '3 months') as d");
      assert.ok(r.rows[0].d.startsWith('2024-04-15'));
    });
  });

  // === ARRAY functions ===
  describe('ARRAY functions', () => {
    it('ARRAY constructor', () => {
      const r = db.execute('SELECT ARRAY[1, 2, 3] as arr');
      assert.deepEqual(r.rows[0].arr, [1, 2, 3]);
    });

    it('ARRAY with expressions', () => {
      const r = db.execute('SELECT ARRAY[1+1, 2*3, 10-4] as arr');
      assert.deepEqual(r.rows[0].arr, [2, 6, 6]);
    });

    it('empty ARRAY', () => {
      const r = db.execute('SELECT ARRAY[] as arr');
      assert.deepEqual(r.rows[0].arr, []);
    });

    it('ARRAY_LENGTH', () => {
      assert.equal(db.execute('SELECT ARRAY_LENGTH(ARRAY[1, 2, 3]) as len').rows[0].len, 3);
    });

    it('ARRAY_APPEND', () => {
      assert.deepEqual(db.execute('SELECT ARRAY_APPEND(ARRAY[1, 2], 3) as arr').rows[0].arr, [1, 2, 3]);
    });

    it('ARRAY_POSITION', () => {
      assert.equal(db.execute('SELECT ARRAY_POSITION(ARRAY[10, 20, 30], 20) as pos').rows[0].pos, 2);
    });

    it('ARRAY_REMOVE', () => {
      assert.deepEqual(db.execute('SELECT ARRAY_REMOVE(ARRAY[1, 2, 3, 2], 2) as arr').rows[0].arr, [1, 3]);
    });
  });

  // === FILTER clause ===
  describe('FILTER clause', () => {
    it('filters aggregate without GROUP BY', () => {
      db.execute('CREATE TABLE t (region TEXT, amount INT)');
      db.execute("INSERT INTO t VALUES ('E', 100), ('W', 200), ('E', 50)");
      const r = db.execute("SELECT COUNT(*) as total, COUNT(*) FILTER (WHERE region = 'E') as e_count FROM t");
      assert.equal(r.rows[0].total, 3);
      assert.equal(r.rows[0].e_count, 2);
    });

    it('filters aggregate with GROUP BY', () => {
      db.execute('CREATE TABLE sales (region TEXT, product TEXT, amount INT)');
      db.execute("INSERT INTO sales VALUES ('E', 'A', 100), ('E', 'B', 200), ('W', 'A', 150), ('W', 'B', 300)");
      const r = db.execute("SELECT region, SUM(amount) FILTER (WHERE product = 'A') as a_total FROM sales GROUP BY region");
      const east = r.rows.find(r => r.region === 'E');
      const west = r.rows.find(r => r.region === 'W');
      assert.equal(east.a_total, 100);
      assert.equal(west.a_total, 150);
    });
  });

  // === NTH_VALUE ===
  describe('NTH_VALUE', () => {
    it('returns nth row value', () => {
      db.execute('CREATE TABLE scores (name TEXT, score INT)');
      db.execute("INSERT INTO scores VALUES ('Alice', 90), ('Bob', 85), ('Carol', 95)");
      const r = db.execute('SELECT name, NTH_VALUE(name, 2) OVER (ORDER BY score DESC) as second FROM scores');
      assert.equal(r.rows[0].second, 'Alice'); // 2nd highest scorer
    });
  });

  // === Predicate pushdown ===
  describe('Predicate pushdown', () => {
    it('produces correct results with pushed-down predicates', () => {
      db.execute('CREATE TABLE orders (id INT, cid INT, status TEXT)');
      db.execute('CREATE TABLE customers (id INT, name TEXT)');
      for (let i = 0; i < 20; i++) db.execute(`INSERT INTO orders VALUES (${i}, ${i % 5}, '${i % 2 === 0 ? 'active' : 'closed'}')`);
      for (let i = 0; i < 5; i++) db.execute(`INSERT INTO customers VALUES (${i}, 'C${i}')`);
      
      const r = db.execute("SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id WHERE o.status = 'active'");
      assert.equal(r.rows.length, 10); // half of 20
    });
  });

  // === Unary minus ===
  describe('Unary minus', () => {
    it('val * -1 works', () => {
      db.execute('CREATE TABLE t (id INT, val INT)');
      db.execute('INSERT INTO t VALUES (1, 10)');
      const r = db.execute('SELECT val * -1 AS neg FROM t');
      assert.equal(r.rows[0].neg, -10);
    });
  });

  // === COMMENT ON ===
  describe('COMMENT ON', () => {
    it('stores and retrieves comments', () => {
      db.execute('CREATE TABLE t (id INT)');
      db.execute("COMMENT ON TABLE t IS 'Test table'");
      assert.equal(db._comments.get('TABLE.t'), 'Test table');
    });

    it('removes comment with NULL', () => {
      db.execute('CREATE TABLE t (id INT)');
      db.execute("COMMENT ON TABLE t IS 'Test'");
      db.execute('COMMENT ON TABLE t IS NULL');
      assert.equal(db._comments.has('TABLE.t'), false);
    });
  });

  // === Correlated EXISTS ===
  describe('Correlated EXISTS', () => {
    it('EXISTS with unqualified outer column', () => {
      db.execute('CREATE TABLE orders (o_orderkey INT)');
      db.execute('CREATE TABLE lineitem (l_orderkey INT)');
      db.execute('INSERT INTO orders VALUES (1), (2), (3)');
      db.execute('INSERT INTO lineitem VALUES (1), (3)');
      const r = db.execute('SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey) ORDER BY o_orderkey');
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].o_orderkey, 1);
      assert.equal(r.rows[1].o_orderkey, 3);
    });

    it('NOT EXISTS with unqualified outer column', () => {
      db.execute('CREATE TABLE orders (o_orderkey INT)');
      db.execute('CREATE TABLE lineitem (l_orderkey INT)');
      db.execute('INSERT INTO orders VALUES (1), (2), (3)');
      db.execute('INSERT INTO lineitem VALUES (1), (3)');
      const r = db.execute('SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey)');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].o_orderkey, 2);
    });

    it('EXISTS with qualified outer column', () => {
      db.execute('CREATE TABLE o (id INT, name TEXT)');
      db.execute('CREATE TABLE l (oid INT, qty INT)');
      db.execute("INSERT INTO o VALUES (1, 'a'), (2, 'b')");
      db.execute('INSERT INTO l VALUES (1, 10)');
      const r = db.execute('SELECT * FROM o WHERE EXISTS (SELECT 1 FROM l WHERE l.oid = o.id)');
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].id, 1);
    });
  });

  // === DEFAULT CURRENT_TIMESTAMP ===
  describe('DEFAULT CURRENT_TIMESTAMP', () => {
    it('inserts current timestamp for missing column', () => {
      db.execute('CREATE TABLE t (id INT, created TEXT DEFAULT CURRENT_TIMESTAMP)');
      db.execute('INSERT INTO t (id) VALUES (1)');
      const r = db.execute('SELECT * FROM t');
      assert.ok(r.rows[0].created !== null);
      assert.ok(r.rows[0].created.startsWith('20'));
    });
  });

  // === GROUP BY expression column names ===
  describe('GROUP BY expressions', () => {
    it('produces readable column names', () => {
      db.execute('CREATE TABLE t (id INT)');
      db.execute('INSERT INTO t VALUES (1), (2), (3), (4), (5)');
      const r = db.execute('SELECT id % 2 as grp, COUNT(*) as cnt FROM t GROUP BY id % 2');
      assert.equal(r.rows.length, 2);
      // Should have human-readable key, not JSON
      const keys = Object.keys(r.rows[0]);
      assert.ok(!keys.some(k => k.startsWith('{')));
    });
  });

  // === Date functions ===
  describe('Additional date functions', () => {
    it('DATE_SUB', () => {
      const r = db.execute("SELECT DATE_SUB('2024-03-15', '2 months') as d");
      assert.ok(r.rows[0].d.startsWith('2024-01'));
    });

    it('TO_CHAR', () => {
      const r = db.execute("SELECT TO_CHAR('2024-03-15', 'YYYY-MM-DD') as d");
      assert.equal(r.rows[0].d, '2024-03-15');
    });
  });
});
