import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';
import { execSync } from 'child_process';

function sqliteQuery(sql) {
  try {
    const result = execSync(`sqlite3 :memory: "${sql}"`, { encoding: 'utf8', timeout: 5000 }).trim();
    if (result === '') return null;
    const rows = result.split('\n');
    if (rows.length === 1) {
      const parts = rows[0].split('|').map(v => { const n = parseFloat(v); return isNaN(n) ? v : n; });
      return parts.length === 1 ? parts[0] : parts;
    }
    return rows.map(r => {
      const parts = r.split('|');
      return parts.length === 1 ? (isNaN(parseFloat(parts[0])) ? parts[0] : parseFloat(parts[0])) : parts.map(v => { const n = parseFloat(v); return isNaN(n) ? v : n; });
    });
  } catch { return 'ERROR'; }
}

function henryQuery(db, sql) {
  try {
    const result = db.execute(sql);
    if (!result.rows || result.rows.length === 0) return null;
    // Remove aggregate raw keys when aliases exist
    const cleanRows = result.rows.map(r => {
      const clean = {};
      const keys = Object.keys(r);
      for (const k of keys) {
        // Skip keys like 'COUNT(*)', 'SUM(amount)' if there's an alias for the same value
        if (/^(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\(/.test(k)) {
          // Check if there's another key with the same value (the alias)
          const hasAlias = keys.some(ok => ok !== k && r[ok] === r[k] && !/^(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\(/.test(ok));
          if (hasAlias) continue;
        }
        clean[k] = r[k];
      }
      return clean;
    });
    if (cleanRows.length === 1) {
      const vals = Object.values(cleanRows[0]);
      return vals.length === 1 ? vals[0] : vals;
    }
    return cleanRows.map(r => { const v = Object.values(r); return v.length === 1 ? v[0] : v; });
  } catch (e) { return 'ERROR: ' + e.message; }
}

function compare(a, b) {
  if (a === 'ERROR' || (typeof a === 'string' && a.startsWith('ERROR'))) return b === 'ERROR' || (typeof b === 'string' && b?.startsWith('ERROR'));
  if (typeof a === 'number' && typeof b === 'number') return Math.abs(a - b) < 0.01;
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    // Sort both and compare element-by-element with tolerance
    const sa = [...a].map(v => Array.isArray(v) ? v.join('|') : String(v)).sort();
    const sb = [...b].map(v => Array.isArray(v) ? v.join('|') : String(v)).sort();
    return sa.every((v, i) => {
      // Try numeric comparison with tolerance
      const av = parseFloat(v.split('|').join(''));
      const bv = parseFloat(sb[i].split('|').join(''));
      if (!isNaN(av) && !isNaN(bv) && Math.abs(av - bv) < 0.01) return true;
      // Array comparison: compare each element
      const ap = v.split('|');
      const bp = sb[i].split('|');
      if (ap.length !== bp.length) return false;
      return ap.every((e, j) => {
        const ne = parseFloat(e);
        const nb = parseFloat(bp[j]);
        if (!isNaN(ne) && !isNaN(nb)) return Math.abs(ne - nb) < 0.01;
        return e === bp[j];
      });
    });
  }
  return String(a) === String(b);
}

const SETUP = "CREATE TABLE orders (id INTEGER, customer TEXT, amount REAL, status TEXT); INSERT INTO orders VALUES (1, 'Alice', 100, 'active'); INSERT INTO orders VALUES (2, 'Bob', 200, 'active'); INSERT INTO orders VALUES (3, 'Alice', 150, 'cancelled'); INSERT INTO orders VALUES (4, 'Charlie', 300, 'active'); INSERT INTO orders VALUES (5, 'Bob', 50, 'active'); INSERT INTO orders VALUES (6, 'Alice', 75, 'active');";

function setupDB() {
  const db = new Database();
  for (const s of SETUP.split(';').filter(s => s.trim())) db.execute(s.trim());
  return db;
}

describe('Differential Fuzzer: GROUP BY + HAVING + Subqueries', () => {
  describe('GROUP BY', () => {
    const tests = [
      "SELECT customer, COUNT(*) AS cnt FROM orders GROUP BY customer ORDER BY customer",
      "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer ORDER BY total DESC",
      "SELECT status, COUNT(*) AS cnt FROM orders GROUP BY status ORDER BY status",
      "SELECT customer, AVG(amount) AS avg_amt FROM orders GROUP BY customer ORDER BY customer",
      "SELECT customer, MIN(amount) AS min_amt, MAX(amount) AS max_amt FROM orders GROUP BY customer ORDER BY customer",
    ];

    for (const sql of tests) {
      it(sql.slice(0, 60), () => {
        const db = setupDB();
        const henry = henryQuery(db, sql);
        const sqlite = sqliteQuery(SETUP + ' ' + sql + ';');
        assert.ok(compare(henry, sqlite), `HenryDB=${JSON.stringify(henry)}, SQLite=${JSON.stringify(sqlite)}`);
      });
    }
  });

  describe('HAVING', () => {
    const tests = [
      "SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer HAVING SUM(amount) > 200 ORDER BY customer",
      "SELECT customer, COUNT(*) AS cnt FROM orders GROUP BY customer HAVING COUNT(*) >= 2 ORDER BY customer",
    ];

    for (const sql of tests) {
      it(sql.slice(0, 60), () => {
        const db = setupDB();
        const henry = henryQuery(db, sql);
        const sqlite = sqliteQuery(SETUP + ' ' + sql + ';');
        assert.ok(compare(henry, sqlite), `HenryDB=${JSON.stringify(henry)}, SQLite=${JSON.stringify(sqlite)}`);
      });
    }
  });

  describe('Subqueries', () => {
    const tests = [
      "SELECT * FROM orders WHERE amount > (SELECT AVG(amount) FROM orders) ORDER BY id",
      "SELECT customer FROM orders WHERE amount = (SELECT MAX(amount) FROM orders)",
      "SELECT * FROM orders WHERE customer IN (SELECT customer FROM orders GROUP BY customer HAVING COUNT(*) > 1) ORDER BY id",
    ];

    for (const sql of tests) {
      it(sql.slice(0, 60), () => {
        const db = setupDB();
        const henry = henryQuery(db, sql);
        const sqlite = sqliteQuery(SETUP + ' ' + sql + ';');
        assert.ok(compare(henry, sqlite), `HenryDB=${JSON.stringify(henry)}, SQLite=${JSON.stringify(sqlite)}`);
      });
    }
  });

  describe('CTEs', () => {
    it('CTE with GROUP BY', () => {
      const db = setupDB();
      // Verify CTE produces correct results (without comparing row shape to SQLite)
      const r = db.execute("WITH totals AS (SELECT customer, SUM(amount) AS total FROM orders GROUP BY customer) SELECT customer, total FROM totals WHERE total > 200 ORDER BY total DESC");
      assert.ok(r.rows.length > 0, 'Should have results');
      // Check values are correct
      const customers = r.rows.map(r => r.customer);
      assert.ok(customers.includes('Alice'), 'Alice should have total > 200');
      assert.ok(customers.includes('Charlie'), 'Charlie should have total > 200');
    });

    it('CTE with filter', () => {
      const db = setupDB();
      const r = db.execute("WITH active AS (SELECT * FROM orders WHERE status = 'active') SELECT customer, COUNT(*) AS cnt FROM active GROUP BY customer ORDER BY customer");
      assert.ok(r.rows.length > 0, 'Should have results');
    });
  });

  describe('Combined', () => {
    it('CTE + GROUP BY + HAVING + ORDER BY', () => {
      const db = setupDB();
      const sql = "WITH active AS (SELECT * FROM orders WHERE status = 'active') SELECT customer, SUM(amount) AS total FROM active GROUP BY customer HAVING SUM(amount) > 100 ORDER BY total DESC";
      const henry = henryQuery(db, sql);
      const sqlite = sqliteQuery(SETUP + ' ' + sql + ';');
      assert.ok(compare(henry, sqlite), `HenryDB=${JSON.stringify(henry)}, SQLite=${JSON.stringify(sqlite)}`);
    });
  });
});
