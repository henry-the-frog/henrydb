import { describe, it, before } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';
import { execSync } from 'child_process';

function sqliteQuery(sql) {
  try {
    const result = execSync(`sqlite3 :memory: "${sql}"`, { encoding: 'utf8', timeout: 5000 }).trim();
    if (result === '') return null;
    // Handle multiple rows: split by newline
    const rows = result.split('\n');
    if (rows.length === 1) {
      const parts = rows[0].split('|').map(v => {
        const n = parseFloat(v);
        return isNaN(n) ? v : n;
      });
      return parts.length === 1 ? parts[0] : parts;
    }
    // Multiple rows: return array of first column values
    return rows.map(r => {
      const parts = r.split('|');
      const v = parts[0];
      const n = parseFloat(v);
      return isNaN(n) ? v : n;
    });
  } catch {
    return 'ERROR';
  }
}

function henryQuery(db, sql) {
  try {
    const result = db.execute(sql);
    if (!result.rows || result.rows.length === 0) return null;
    if (result.rows.length === 1) {
      const vals = Object.values(result.rows[0]);
      return vals.length === 1 ? vals[0] : vals;
    }
    // Multiple rows: return array of first column values
    return result.rows.map(r => Object.values(r)[0]);
  } catch {
    return 'ERROR';
  }
}

function compare(sqlite, henry) {
  if (sqlite === 'ERROR' && henry === 'ERROR') return true;
  if (typeof sqlite === 'number' && typeof henry === 'number') {
    return Math.abs(sqlite - henry) < 0.001;
  }
  if (Array.isArray(sqlite) && Array.isArray(henry)) {
    if (sqlite.length !== henry.length) return false;
    // Sort both for order-independent comparison
    const ss = [...sqlite].sort();
    const hs = [...henry].sort();
    return ss.every((v, i) => {
      if (typeof v === 'number' && typeof hs[i] === 'number') return Math.abs(v - hs[i]) < 0.001;
      return v === hs[i];
    });
  }
  return sqlite === henry;
}

describe('Extended Differential Fuzzer', () => {
  describe('WHERE clause arithmetic', () => {
    const cases = [
      // Setup + query pairs
      {
        setup: "CREATE TABLE t (a INT, b INT, c INT); INSERT INTO t VALUES (10, 3, 4); INSERT INTO t VALUES (5, 7, 2); INSERT INTO t VALUES (20, 1, 6);",
        queries: [
          "SELECT a FROM t WHERE a + b * c > 20",
          "SELECT a FROM t WHERE a * b + c < 40",
          "SELECT a FROM t WHERE a - b * 2 > 0",
          "SELECT COUNT(*) AS cnt FROM t WHERE a + b > 10",
          "SELECT SUM(a) AS total FROM t WHERE b * c > 5",
        ]
      },
    ];

    for (const { setup, queries } of cases) {
      for (const query of queries) {
        it(query, () => {
          const db = new Database();
          for (const stmt of setup.split(';').filter(s => s.trim())) {
            db.execute(stmt.trim());
          }
          // Run in SQLite: combine setup + query, replacing INT with INTEGER
          const sqliteSetup = setup.replace(/\bINT\b/g, 'INTEGER');
          const fullSql = sqliteSetup + ' ' + query.replace(/\bINT\b/g, 'INTEGER') + ';';
          const sqlite = sqliteQuery(fullSql);
          const henry = henryQuery(db, query);
          assert.ok(compare(sqlite, henry), `HenryDB=${henry}, SQLite=${sqlite}`);
        });
      }
    }
  });

  describe('Aggregate expressions', () => {
    it('SUM with arithmetic', () => {
      const db = new Database();
      db.execute("CREATE TABLE sales (price REAL, qty INT)");
      db.execute("INSERT INTO sales VALUES (10, 5)");
      db.execute("INSERT INTO sales VALUES (20, 3)");
      db.execute("INSERT INTO sales VALUES (15, 4)");
      
      const r = db.execute("SELECT SUM(price * qty) AS total FROM sales");
      // 50 + 60 + 60 = 170
      assert.equal(r.rows[0].total, 170);
      
      const sqlite = sqliteQuery("CREATE TABLE sales (price REAL, qty INT); INSERT INTO sales VALUES (10, 5); INSERT INTO sales VALUES (20, 3); INSERT INTO sales VALUES (15, 4); SELECT SUM(price * qty) FROM sales;");
      assert.equal(r.rows[0].total, sqlite);
    });

    it('AVG with arithmetic', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (a INT, b INT)");
      db.execute("INSERT INTO t VALUES (10, 2)");
      db.execute("INSERT INTO t VALUES (20, 4)");
      
      const r = db.execute("SELECT AVG(a + b) AS avg_val FROM t");
      // (12 + 24) / 2 = 18
      assert.equal(r.rows[0].avg_val, 18);
    });
  });

  describe('ORDER BY with arithmetic', () => {
    it('ORDER BY expression', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (a INT, b INT)");
      db.execute("INSERT INTO t VALUES (3, 10)");
      db.execute("INSERT INTO t VALUES (1, 20)");
      db.execute("INSERT INTO t VALUES (2, 15)");
      
      const r = db.execute("SELECT a, b FROM t ORDER BY a * 2 + b");
      // Scores: 3*2+10=16, 1*2+20=22, 2*2+15=19
      assert.equal(r.rows[0].a, 3);  // 16
      assert.equal(r.rows[1].a, 2);  // 19
      assert.equal(r.rows[2].a, 1);  // 22
    });
  });

  describe('JOIN ON with arithmetic', () => {
    it('JOIN with computed condition', () => {
      const db = new Database();
      db.execute("CREATE TABLE a (id INT, val INT)");
      db.execute("CREATE TABLE b (id INT, factor INT)");
      db.execute("INSERT INTO a VALUES (1, 10)");
      db.execute("INSERT INTO a VALUES (2, 20)");
      db.execute("INSERT INTO b VALUES (1, 2)");
      db.execute("INSERT INTO b VALUES (2, 3)");
      
      const r = db.execute("SELECT a.id, a.val * b.factor AS product FROM a JOIN b ON a.id = b.id");
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[0].product, 20);  // 10 * 2
      assert.equal(r.rows[1].product, 60);  // 20 * 3
    });
  });

  describe('HAVING with arithmetic', () => {
    it('HAVING clause with expression', () => {
      const db = new Database();
      db.execute("CREATE TABLE sales (category TEXT, amount INT)");
      db.execute("INSERT INTO sales VALUES ('A', 10)");
      db.execute("INSERT INTO sales VALUES ('A', 20)");
      db.execute("INSERT INTO sales VALUES ('B', 5)");
      db.execute("INSERT INTO sales VALUES ('B', 3)");
      
      const r = db.execute("SELECT category, SUM(amount) AS total FROM sales GROUP BY category HAVING SUM(amount) > 10");
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].category, 'A');
    });
  });

  describe('Generated columns + expression indexes combo', () => {
    it('generated column used in expression index', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      db.execute("INSERT INTO products (price, tax) VALUES (200, 20)");
      db.execute("INSERT INTO products (price, tax) VALUES (50, 5)");
      
      // Create expression index on the generated column
      db.execute("CREATE INDEX idx_total ON products (total)");
      
      // Should use index for lookup
      const r = db.execute("SELECT * FROM products WHERE total = 110");
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].price, 100);
    });
  });
});
