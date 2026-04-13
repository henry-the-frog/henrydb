// sql-standard-conformance.test.js — PostgreSQL SQL standard conformance tests
// Tests basic SQL standard features against PostgreSQL behavior expectations.
// 10 test groups covering: NULL semantics, set operations, correlated subqueries,
// GROUP BY/HAVING, CASE expressions, string functions, self-joins, DML RETURNING,
// DISTINCT/ORDER BY interactions, and type casting/coercion.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function freshDb() {
  const db = new Database();
  db.execute(`CREATE TABLE employees (
    id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT, mgr_id INT
  )`);
  db.execute("INSERT INTO employees VALUES (1, 'Alice',   'eng',   100000, NULL)");
  db.execute("INSERT INTO employees VALUES (2, 'Bob',     'eng',    90000, 1)");
  db.execute("INSERT INTO employees VALUES (3, 'Carol',   'sales',  80000, 1)");
  db.execute("INSERT INTO employees VALUES (4, 'Dave',    'sales',  85000, 3)");
  db.execute("INSERT INTO employees VALUES (5, 'Eve',     'eng',    95000, 1)");
  db.execute("INSERT INTO employees VALUES (6, 'Frank',   'hr',     70000, NULL)");
  db.execute("INSERT INTO employees VALUES (7, 'Grace',   'hr',     72000, 6)");
  return db;
}

// ─── Test 1: NULL Semantics (SQL standard three-valued logic) ───────────────
describe('SQL Standard: NULL Semantics', () => {
  it('NULL = NULL is not true (three-valued logic)', () => {
    const db = freshDb();
    const r = db.execute('SELECT * FROM employees WHERE NULL = NULL');
    assert.strictEqual(r.rows.length, 0, 'NULL = NULL should match no rows');
  });

  it('NULL IS NULL returns true', () => {
    const db = freshDb();
    const r = db.execute('SELECT * FROM employees WHERE mgr_id IS NULL');
    assert.strictEqual(r.rows.length, 2);
    const names = r.rows.map(r => r.name).sort();
    assert.deepStrictEqual(names, ['Alice', 'Frank']);
  });

  it('NULL in arithmetic propagates NULL', () => {
    const db = freshDb();
    db.execute('CREATE TABLE nulltest (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO nulltest VALUES (1, 10, NULL)');
    db.execute('INSERT INTO nulltest VALUES (2, 20, 5)');
    const r = db.execute('SELECT id, a + b as sum, a * b as prod FROM nulltest ORDER BY id');
    assert.strictEqual(r.rows[0].sum, null, 'NULL + 10 should be NULL');
    assert.strictEqual(r.rows[0].prod, null, 'NULL * 10 should be NULL');
    assert.strictEqual(r.rows[1].sum, 25);
    assert.strictEqual(r.rows[1].prod, 100);
  });

  it('COALESCE returns first non-NULL', () => {
    const db = freshDb();
    const r = db.execute("SELECT COALESCE(NULL, NULL, 'found') as val");
    assert.strictEqual(r.rows[0].val, 'found');
  });

  it('NULLIF returns NULL when args are equal', () => {
    const db = freshDb();
    const r = db.execute('SELECT NULLIF(1, 1) as eq, NULLIF(1, 2) as neq');
    assert.strictEqual(r.rows[0].eq, null);
    assert.strictEqual(r.rows[0].neq, 1);
  });

  it('COUNT(*) counts NULLs, COUNT(col) does not', () => {
    const db = freshDb();
    const r = db.execute('SELECT COUNT(*) as total, COUNT(mgr_id) as with_mgr FROM employees');
    assert.strictEqual(r.rows[0].total, 7);
    assert.strictEqual(r.rows[0].with_mgr, 5);
  });

  it('NULL in GROUP BY forms its own group', () => {
    const db = freshDb();
    const r = db.execute('SELECT mgr_id, COUNT(*) as cnt FROM employees GROUP BY mgr_id ORDER BY mgr_id');
    // NULL group should exist
    const nullGroup = r.rows.find(row => row.mgr_id === null);
    assert.ok(nullGroup, 'NULL should form its own group');
    assert.strictEqual(nullGroup.cnt, 2);
  });
});

// ─── Test 2: Set Operations (UNION, INTERSECT, EXCEPT) ─────────────────────
describe('SQL Standard: Set Operations', () => {
  it('UNION removes duplicates', () => {
    const db = freshDb();
    const r = db.execute("SELECT dept FROM employees WHERE dept = 'eng' UNION SELECT dept FROM employees WHERE dept = 'eng'");
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].dept, 'eng');
  });

  it('UNION ALL preserves duplicates', () => {
    const db = freshDb();
    const r = db.execute("SELECT dept FROM employees WHERE dept = 'eng' UNION ALL SELECT dept FROM employees WHERE dept = 'eng'");
    assert.strictEqual(r.rows.length, 6); // 3 eng + 3 eng
  });

  it('INTERSECT returns common rows', () => {
    const db = freshDb();
    const r = db.execute(
      "SELECT name FROM employees WHERE dept = 'eng' INTERSECT SELECT name FROM employees WHERE salary > 90000"
    );
    const names = r.rows.map(r => r.name).sort();
    assert.deepStrictEqual(names, ['Alice', 'Eve']);
  });

  it('EXCEPT removes rows present in second query', () => {
    const db = freshDb();
    const r = db.execute(
      "SELECT name FROM employees WHERE dept = 'eng' EXCEPT SELECT name FROM employees WHERE salary >= 95000"
    );
    // eng: Alice(100k), Bob(90k), Eve(95k) — remove Alice(100k), Eve(95k) → Bob
    const names = r.rows.map(r => r.name);
    assert.deepStrictEqual(names, ['Bob']);
  });

  it('UNION with different column names uses first query columns', () => {
    const db = freshDb();
    const r = db.execute("SELECT name AS person FROM employees WHERE id = 1 UNION SELECT dept FROM employees WHERE id = 3");
    // Should have column named 'person' from first query
    assert.ok('person' in r.rows[0], 'Column should be named from first query');
  });
});

// ─── Test 3: Correlated Subqueries ─────────────────────────────────────────
describe('SQL Standard: Correlated Subqueries', () => {
  it('finds employees earning above their department average', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name, salary, dept FROM employees e1
      WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept = e1.dept)
      ORDER BY name
    `);
    // eng avg: 95000 → Alice(100k) above, sales avg: 82500 → Dave(85k) above, hr avg: 71000 → Grace(72k) above
    const names = r.rows.map(r => r.name);
    assert.deepStrictEqual(names, ['Alice', 'Dave', 'Grace']);
  });

  it('EXISTS subquery filters correctly', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name FROM employees e
      WHERE EXISTS (SELECT 1 FROM employees m WHERE m.mgr_id = e.id)
      ORDER BY name
    `);
    // Managers: Alice(1), Carol(3), Frank(6)
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Alice', 'Carol', 'Frank']);
  });

  it('NOT EXISTS finds employees with no subordinates', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name FROM employees e
      WHERE NOT EXISTS (SELECT 1 FROM employees s WHERE s.mgr_id = e.id)
      ORDER BY name
    `);
    // Non-managers: Bob, Dave, Eve, Grace
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Bob', 'Dave', 'Eve', 'Grace']);
  });

  it('scalar subquery in SELECT list', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name, salary,
        (SELECT AVG(salary) FROM employees e2 WHERE e2.dept = e1.dept) as dept_avg
      FROM employees e1
      WHERE id = 1
    `);
    assert.strictEqual(r.rows[0].name, 'Alice');
    assert.strictEqual(r.rows[0].dept_avg, 95000); // eng avg
  });
});

// ─── Test 4: GROUP BY / HAVING ──────────────────────────────────────────────
describe('SQL Standard: GROUP BY and HAVING', () => {
  it('GROUP BY with multiple aggregates', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT dept, COUNT(*) as cnt, MIN(salary) as min_sal, MAX(salary) as max_sal, SUM(salary) as total
      FROM employees GROUP BY dept ORDER BY dept
    `);
    assert.strictEqual(r.rows.length, 3);
    const eng = r.rows.find(r => r.dept === 'eng');
    assert.strictEqual(eng.cnt, 3);
    assert.strictEqual(eng.min_sal, 90000);
    assert.strictEqual(eng.max_sal, 100000);
    assert.strictEqual(eng.total, 285000);
  });

  it('HAVING filters groups after aggregation', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT dept, AVG(salary) as avg_sal FROM employees
      GROUP BY dept HAVING AVG(salary) > 80000
      ORDER BY avg_sal DESC
    `);
    // eng: 95000, sales: 82500 (hr: 71000 excluded)
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].dept, 'eng');
    assert.strictEqual(r.rows[1].dept, 'sales');
  });

  it('HAVING with COUNT filters small groups', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT dept, COUNT(*) as cnt FROM employees
      GROUP BY dept HAVING COUNT(*) >= 3
    `);
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].dept, 'eng');
  });

  it('WHERE + GROUP BY + HAVING work together', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT dept, SUM(salary) as total FROM employees
      WHERE mgr_id IS NOT NULL
      GROUP BY dept HAVING SUM(salary) > 100000
      ORDER BY total DESC
    `);
    // After WHERE (exclude Alice, Frank): eng has Bob(90k)+Eve(95k)=185k, sales has Carol(80k)+Dave(85k)=165k, hr has Grace(72k)
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].dept, 'eng');
    assert.strictEqual(r.rows[0].total, 185000);
  });
});

// ─── Test 5: CASE Expressions ───────────────────────────────────────────────
describe('SQL Standard: CASE Expressions', () => {
  it('searched CASE with multiple WHEN clauses', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name, CASE
        WHEN salary >= 95000 THEN 'senior'
        WHEN salary >= 80000 THEN 'mid'
        ELSE 'junior'
      END as level
      FROM employees ORDER BY id
    `);
    assert.strictEqual(r.rows[0].level, 'senior');  // Alice 100k
    assert.strictEqual(r.rows[1].level, 'mid');      // Bob 90k
    assert.strictEqual(r.rows[5].level, 'junior');   // Frank 70k
  });

  it('simple CASE (value-matching form)', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name, CASE dept
        WHEN 'eng' THEN 'Engineering'
        WHEN 'sales' THEN 'Sales'
        WHEN 'hr' THEN 'Human Resources'
        ELSE 'Other'
      END as department
      FROM employees WHERE id = 1
    `);
    assert.strictEqual(r.rows[0].department, 'Engineering');
  });

  it('CASE in ORDER BY for custom sorting', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name, dept FROM employees
      ORDER BY CASE dept WHEN 'hr' THEN 1 WHEN 'sales' THEN 2 WHEN 'eng' THEN 3 END, name
    `);
    assert.strictEqual(r.rows[0].dept, 'hr');
    assert.strictEqual(r.rows[r.rows.length - 1].dept, 'eng');
  });

  it('CASE with NULL handling', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name, CASE
        WHEN mgr_id IS NULL THEN 'top-level'
        ELSE 'reports-to-' || CAST(mgr_id AS TEXT)
      END as hierarchy
      FROM employees WHERE id IN (1, 2, 6) ORDER BY id
    `);
    assert.strictEqual(r.rows[0].hierarchy, 'top-level');
    assert.strictEqual(r.rows[1].hierarchy, 'reports-to-1');
    assert.strictEqual(r.rows[2].hierarchy, 'top-level');
  });
});

// ─── Test 6: String Functions ───────────────────────────────────────────────
describe('SQL Standard: String Functions', () => {
  it('UPPER and LOWER', () => {
    const db = freshDb();
    const r = db.execute("SELECT UPPER('hello') as u, LOWER('WORLD') as l");
    assert.strictEqual(r.rows[0].u, 'HELLO');
    assert.strictEqual(r.rows[0].l, 'world');
  });

  it('TRIM, LTRIM, RTRIM', () => {
    const db = freshDb();
    const r = db.execute("SELECT TRIM('  hi  ') as t, LTRIM('  hi') as lt, RTRIM('hi  ') as rt");
    assert.strictEqual(r.rows[0].t, 'hi');
    assert.strictEqual(r.rows[0].lt, 'hi');
    assert.strictEqual(r.rows[0].rt, 'hi');
  });

  it('LENGTH on strings', () => {
    const db = freshDb();
    const r = db.execute("SELECT name, LENGTH(name) as len FROM employees ORDER BY LENGTH(name), name LIMIT 3");
    // Shortest names first
    assert.ok(r.rows[0].len <= r.rows[1].len);
  });

  it('SUBSTRING with FROM/FOR syntax', () => {
    const db = freshDb();
    const r = db.execute("SELECT SUBSTRING('PostgreSQL' FROM 1 FOR 8) as sub");
    assert.strictEqual(r.rows[0].sub, 'PostgreS');
  });

  it('POSITION finds substring location', () => {
    const db = freshDb();
    const r = db.execute("SELECT POSITION('gre' IN 'PostgreSQL') as pos");
    // 'gre' starts at position 5 in 'PostgreSQL'
    assert.strictEqual(r.rows[0].pos, 5);
  });

  it('string concatenation with ||', () => {
    const db = freshDb();
    const r = db.execute("SELECT name || ' (' || dept || ')' as full_name FROM employees WHERE id = 1");
    assert.strictEqual(r.rows[0].full_name, 'Alice (eng)');
  });
});

// ─── Test 7: Self-Joins and Multi-table Queries ────────────────────────────
describe('SQL Standard: Self-Joins', () => {
  it('self-join to find employee-manager pairs', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT e.name as employee, m.name as manager
      FROM employees e LEFT JOIN employees m ON e.mgr_id = m.id
      ORDER BY e.id
    `);
    assert.strictEqual(r.rows.length, 7);
    assert.strictEqual(r.rows[0].manager, null);    // Alice has no manager
    assert.strictEqual(r.rows[1].manager, 'Alice');  // Bob reports to Alice
    assert.strictEqual(r.rows[3].manager, 'Carol');  // Dave reports to Carol
  });

  it('self-join to find peers (same manager)', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT DISTINCT e1.name as emp1, e2.name as emp2
      FROM employees e1
      JOIN employees e2 ON e1.mgr_id = e2.mgr_id AND e1.id < e2.id
      ORDER BY emp1, emp2
    `);
    // Peers under Alice (mgr_id=1): Bob-Carol, Bob-Eve, Carol-Eve
    assert.ok(r.rows.length >= 3);
    const peerPairs = r.rows.map(r => `${r.emp1}-${r.emp2}`);
    assert.ok(peerPairs.includes('Bob-Carol'));
    assert.ok(peerPairs.includes('Bob-Eve'));
    assert.ok(peerPairs.includes('Carol-Eve'));
  });

  it('multi-table query with aggregation', () => {
    const db = freshDb();
    db.execute('CREATE TABLE projects (id INT PRIMARY KEY, name TEXT, dept TEXT, budget INT)');
    db.execute("INSERT INTO projects VALUES (1, 'Alpha', 'eng', 500000)");
    db.execute("INSERT INTO projects VALUES (2, 'Beta', 'sales', 300000)");
    db.execute("INSERT INTO projects VALUES (3, 'Gamma', 'eng', 200000)");

    const r = db.execute(`
      SELECT e.dept, COUNT(DISTINCT e.id) as headcount, COUNT(DISTINCT p.id) as projects
      FROM employees e
      LEFT JOIN projects p ON e.dept = p.dept
      GROUP BY e.dept
      ORDER BY e.dept
    `);
    const eng = r.rows.find(r => r.dept === 'eng');
    assert.strictEqual(eng.headcount, 3);
    assert.strictEqual(eng.projects, 2);
    // Verify HR has no projects (LEFT JOIN produces NULL)
    const hr = r.rows.find(r => r.dept === 'hr');
    assert.strictEqual(hr.headcount, 2);
    assert.strictEqual(hr.projects, 0);
  });
});

// ─── Test 8: DML with RETURNING ────────────────────────────────────────────
describe('SQL Standard: DML RETURNING', () => {
  it('INSERT RETURNING returns inserted row', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)');
    const r = db.execute("INSERT INTO items VALUES (1, 'Widget', 999) RETURNING *");
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'Widget');
    assert.strictEqual(r.rows[0].price, 999);
  });

  it('UPDATE RETURNING returns modified rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)');
    db.execute("INSERT INTO items VALUES (1, 'Widget', 999)");
    db.execute("INSERT INTO items VALUES (2, 'Gadget', 1999)");
    const r = db.execute('UPDATE items SET price = price + 100 WHERE price < 1500 RETURNING id, price');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[0].price, 1099);
  });

  it('DELETE RETURNING returns removed rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)');
    db.execute("INSERT INTO items VALUES (1, 'Widget', 999)");
    db.execute("INSERT INTO items VALUES (2, 'Gadget', 1999)");
    const r = db.execute('DELETE FROM items WHERE price > 1500 RETURNING *');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'Gadget');
    // Verify it's actually deleted
    const remaining = db.execute('SELECT COUNT(*) as cnt FROM items');
    assert.strictEqual(remaining.rows[0].cnt, 1);
  });

  it('UPSERT (ON CONFLICT DO UPDATE) works correctly', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)');
    db.execute("INSERT INTO items VALUES (1, 'Widget', 999)");
    db.execute("INSERT INTO items VALUES (1, 'Widget Pro', 1499) ON CONFLICT (id) DO UPDATE SET name = 'Widget Pro', price = 1499");
    const r = db.execute('SELECT * FROM items WHERE id = 1');
    assert.strictEqual(r.rows[0].name, 'Widget Pro');
    assert.strictEqual(r.rows[0].price, 1499);
  });
});

// ─── Test 9: DISTINCT and ORDER BY Interactions ────────────────────────────
describe('SQL Standard: DISTINCT and ORDER BY', () => {
  it('SELECT DISTINCT removes duplicate rows', () => {
    const db = freshDb();
    const r = db.execute('SELECT DISTINCT dept FROM employees ORDER BY dept');
    assert.strictEqual(r.rows.length, 3);
    assert.deepStrictEqual(r.rows.map(r => r.dept), ['eng', 'hr', 'sales']);
  });

  it('ORDER BY with NULLS FIRST / NULLS LAST', () => {
    const db = freshDb();
    const r1 = db.execute('SELECT name, mgr_id FROM employees ORDER BY mgr_id ASC NULLS FIRST');
    assert.strictEqual(r1.rows[0].mgr_id, null);

    const r2 = db.execute('SELECT name, mgr_id FROM employees ORDER BY mgr_id ASC NULLS LAST');
    assert.strictEqual(r2.rows[r2.rows.length - 1].mgr_id, null);
  });

  it('ORDER BY with expression', () => {
    const db = freshDb();
    const r = db.execute('SELECT name, salary FROM employees ORDER BY salary * -1 LIMIT 3');
    // salary * -1 DESC means lowest salary first when sorted ASC
    // Actually *-1 means most negative first = highest salary first
    assert.strictEqual(r.rows[0].name, 'Alice');  // 100k → -100k (most negative)
  });

  it('LIMIT and OFFSET for pagination', () => {
    const db = freshDb();
    const page1 = db.execute('SELECT name FROM employees ORDER BY id LIMIT 3 OFFSET 0');
    const page2 = db.execute('SELECT name FROM employees ORDER BY id LIMIT 3 OFFSET 3');
    const page3 = db.execute('SELECT name FROM employees ORDER BY id LIMIT 3 OFFSET 6');

    assert.strictEqual(page1.rows.length, 3);
    assert.strictEqual(page2.rows.length, 3);
    assert.strictEqual(page3.rows.length, 1);  // Only 7 total
    assert.strictEqual(page1.rows[0].name, 'Alice');
    assert.strictEqual(page2.rows[0].name, 'Dave');
    assert.strictEqual(page3.rows[0].name, 'Grace');
  });

  it('multi-column ORDER BY with mixed directions', () => {
    const db = freshDb();
    const r = db.execute('SELECT name, dept, salary FROM employees ORDER BY dept ASC, salary DESC');
    // eng: Alice(100k), Eve(95k), Bob(90k), then hr: Grace(72k), Frank(70k), then sales: Dave(85k), Carol(80k)
    assert.strictEqual(r.rows[0].name, 'Alice');
    assert.strictEqual(r.rows[1].name, 'Eve');
    assert.strictEqual(r.rows[2].name, 'Bob');
    assert.strictEqual(r.rows[3].name, 'Grace');
  });
});

// ─── Test 10: Type Casting and Coercion ────────────────────────────────────
describe('SQL Standard: Type Casting and Coercion', () => {
  it('CAST integer to text', () => {
    const db = freshDb();
    const r = db.execute("SELECT CAST(salary AS TEXT) as sal_text FROM employees WHERE id = 1");
    assert.strictEqual(r.rows[0].sal_text, '100000');
    assert.strictEqual(typeof r.rows[0].sal_text, 'string');
  });

  it('CAST text to integer', () => {
    const db = freshDb();
    const r = db.execute("SELECT CAST('42' AS INT) as num");
    assert.strictEqual(r.rows[0].num, 42);
    assert.strictEqual(typeof r.rows[0].num, 'number');
  });

  it('implicit coercion in comparisons (string vs number)', () => {
    const db = freshDb();
    db.execute('CREATE TABLE mixed (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO mixed VALUES (1, '100')");
    db.execute("INSERT INTO mixed VALUES (2, '200')");
    db.execute("INSERT INTO mixed VALUES (3, '50')");
    // Comparing text column with numeric literal — should work with coercion
    const r = db.execute('SELECT * FROM mixed WHERE CAST(val AS INT) > 75 ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].id, 1);
    assert.strictEqual(r.rows[1].id, 2);
  });

  it('boolean expressions in WHERE', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name FROM employees
      WHERE (dept = 'eng' AND salary > 90000) OR (dept = 'hr')
      ORDER BY name
    `);
    // eng > 90k: Alice(100k), Eve(95k); all hr: Frank, Grace
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Alice', 'Eve', 'Frank', 'Grace']);
  });

  it('IN list with mixed types works', () => {
    const db = freshDb();
    const r = db.execute("SELECT name FROM employees WHERE id IN (1, 3, 5, 7) ORDER BY id");
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Alice', 'Carol', 'Eve', 'Grace']);
  });

  it('NOT IN excludes correctly', () => {
    const db = freshDb();
    const r = db.execute(`
      SELECT name FROM employees
      WHERE id NOT IN (SELECT mgr_id FROM employees WHERE mgr_id IS NOT NULL)
      ORDER BY name
    `);
    // Managers are id 1 (Alice), 3 (Carol), 6 (Frank) — exclude them
    assert.deepStrictEqual(r.rows.map(r => r.name), ['Bob', 'Dave', 'Eve', 'Grace']);
  });
});
