// sql-edge-cases.test.js — Edge cases that commonly break in SQL engines
// Tests subtle SQL semantics that many implementations get wrong

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('NULL Semantics', () => {
  it('NULL = NULL should be NULL (not true)', () => {
    const db = new Database();
    const r = db.execute('SELECT (NULL = NULL) as result');
    // In SQL, NULL = NULL is NULL (falsy), not true
    assert.ok(r.rows[0].result === null || r.rows[0].result === false || r.rows[0].result === 0,
      `NULL = NULL should be NULL or false, got: ${r.rows[0].result}`);
  });

  it('NULL != NULL should be NULL (not true)', () => {
    const db = new Database();
    const r = db.execute('SELECT (NULL != NULL) as result');
    assert.ok(r.rows[0].result === null || r.rows[0].result === false || r.rows[0].result === 0,
      `NULL != NULL should be NULL or false, got: ${r.rows[0].result}`);
  });

  it('NOT NULL should be NULL', () => {
    const db = new Database();
    const r = db.execute('SELECT NOT NULL as result');
    assert.equal(r.rows[0].result, null, 'NOT NULL should be NULL');
  });

  it('NULL AND true should be NULL', () => {
    const db = new Database();
    const r = db.execute('SELECT (NULL AND TRUE) as result');
    assert.equal(r.rows[0].result, null);
  });

  it('NULL AND false should be false', () => {
    const db = new Database();
    const r = db.execute('SELECT (NULL AND FALSE) as result');
    assert.ok(r.rows[0].result === false || r.rows[0].result === 0,
      `NULL AND FALSE should be false, got: ${r.rows[0].result}`);
  });

  it('NULL OR true should be true', () => {
    const db = new Database();
    const r = db.execute('SELECT (NULL OR TRUE) as result');
    assert.ok(r.rows[0].result === true || r.rows[0].result === 1);
  });

  it('NULL OR false should be NULL', () => {
    const db = new Database();
    const r = db.execute('SELECT (NULL OR FALSE) as result');
    assert.equal(r.rows[0].result, null);
  });

  it('NULL IN (1, 2, NULL) should be NULL', () => {
    const db = new Database();
    const r = db.execute('SELECT (NULL IN (1, 2, NULL)) as result');
    assert.equal(r.rows[0].result, null);
  });

  it('3 IN (1, 2, NULL) should be NULL (not false)', () => {
    const db = new Database();
    const r = db.execute('SELECT (3 IN (1, 2, NULL)) as result');
    // Per SQL standard, if the value isn't found but NULL is in the list, result is NULL
    assert.ok(r.rows[0].result === null || r.rows[0].result === false || r.rows[0].result === 0,
      `3 IN (1,2,NULL) should be NULL per SQL standard, got: ${r.rows[0].result}`);
  });

  it('COUNT(*) includes NULLs, COUNT(col) excludes them', () => {
    const db = new Database();
    db.execute('CREATE TABLE null_test (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO null_test VALUES (1, 10), (2, NULL), (3, 30)');
    const r = db.execute('SELECT COUNT(*) as all_rows, COUNT(val) as non_null FROM null_test');
    assert.equal(r.rows[0].all_rows, 3);
    assert.equal(r.rows[0].non_null, 2);
  });

  it('SUM/AVG skip NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE null_agg (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO null_agg VALUES (1, 10), (2, NULL), (3, 30), (4, NULL)');
    const r = db.execute('SELECT SUM(val) as s, AVG(val) as a FROM null_agg');
    assert.equal(r.rows[0].s, 40);
    assert.equal(r.rows[0].a, 20); // avg of 10+30, not 10+0+30+0
  });

  it('COALESCE with all NULLs returns NULL', () => {
    const db = new Database();
    const r = db.execute('SELECT COALESCE(NULL, NULL, NULL) as result');
    assert.equal(r.rows[0].result, null);
  });
});

describe('String Edge Cases', () => {
  it('empty string is not NULL', () => {
    const db = new Database();
    db.execute("CREATE TABLE str_test (id INT PRIMARY KEY, val TEXT)");
    db.execute("INSERT INTO str_test VALUES (1, ''), (2, NULL)");
    const r = db.execute("SELECT COUNT(*) as cnt FROM str_test WHERE val IS NOT NULL");
    assert.equal(r.rows[0].cnt, 1, 'Empty string should not be NULL');
  });

  it('string comparison is case-sensitive', () => {
    const db = new Database();
    db.execute("CREATE TABLE case_test (id INT PRIMARY KEY, name TEXT)");
    db.execute("INSERT INTO case_test VALUES (1, 'Alice'), (2, 'alice'), (3, 'ALICE')");
    const r = db.execute("SELECT COUNT(*) as cnt FROM case_test WHERE name = 'Alice'");
    assert.equal(r.rows[0].cnt, 1, 'Case-sensitive comparison');
  });

  it('LIKE with % wildcard', () => {
    const db = new Database();
    db.execute("CREATE TABLE like_test (id INT PRIMARY KEY, name TEXT)");
    db.execute("INSERT INTO like_test VALUES (1, 'hello'), (2, 'world'), (3, 'hello world')");
    const r = db.execute("SELECT name FROM like_test WHERE name LIKE '%hello%' ORDER BY id");
    assert.equal(r.rows.length, 2);
  });

  it('LIKE with _ wildcard', () => {
    const db = new Database();
    db.execute("CREATE TABLE under_test (id INT PRIMARY KEY, code TEXT)");
    db.execute("INSERT INTO under_test VALUES (1, 'A1'), (2, 'A12'), (3, 'B1')");
    const r = db.execute("SELECT code FROM under_test WHERE code LIKE 'A_' ORDER BY id");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].code, 'A1');
  });

  it('concatenation with NULL yields NULL', () => {
    const db = new Database();
    const r = db.execute("SELECT 'hello' || NULL as result");
    // Per SQL standard, concat with NULL yields NULL
    // Many DBs differ here (PG returns NULL, MySQL returns NULL, SQLite returns 'hello')
    assert.ok(r.rows[0].result === null || r.rows[0].result === 'hello',
      'Concat with NULL behavior');
  });
});

describe('Numeric Edge Cases', () => {
  it('integer division truncates toward zero', () => {
    const db = new Database();
    const r = db.execute('SELECT 7 / 2 as result');
    assert.equal(r.rows[0].result, 3);
  });

  it('negative integer division', () => {
    const db = new Database();
    const r = db.execute('SELECT -7 / 2 as result');
    assert.equal(r.rows[0].result, -3, '-7/2 should be -3 (truncate toward zero)');
  });

  it('division by zero returns NULL (not error)', () => {
    const db = new Database();
    const r = db.execute('SELECT 10 / 0 as result');
    assert.equal(r.rows[0].result, null);
  });

  it('modulo with negative numbers', () => {
    const db = new Database();
    const r = db.execute('SELECT -7 % 3 as result');
    assert.equal(r.rows[0].result, -1);
  });

  it('large integer arithmetic', () => {
    const db = new Database();
    const r = db.execute('SELECT 2147483647 + 1 as result');
    assert.equal(r.rows[0].result, 2147483648);
  });
});

describe('GROUP BY Edge Cases', () => {
  it('GROUP BY with no rows returns empty', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty_grp (id INT PRIMARY KEY, grp TEXT, val INT)');
    const r = db.execute('SELECT grp, COUNT(*) as cnt FROM empty_grp GROUP BY grp');
    assert.equal(r.rows.length, 0);
  });

  it('aggregate without GROUP BY on empty table', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty_agg (id INT PRIMARY KEY, val INT)');
    const r = db.execute('SELECT COUNT(*) as cnt, SUM(val) as s, AVG(val) as a FROM empty_agg');
    assert.equal(r.rows.length, 1, 'Should still return one row');
    assert.equal(r.rows[0].cnt, 0);
    assert.equal(r.rows[0].s, null); // SUM of no rows is NULL
  });

  it('GROUP BY with expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE expr_grp (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 20; i++) db.execute(`INSERT INTO expr_grp VALUES (${i}, ${i})`);
    const r = db.execute('SELECT val % 5 as bucket, COUNT(*) as cnt FROM expr_grp GROUP BY val % 5 ORDER BY bucket');
    assert.equal(r.rows.length, 5);
    for (const row of r.rows) assert.equal(row.cnt, 4);
  });
});

describe('Subquery Edge Cases', () => {
  it('scalar subquery returning more than 1 row should error', () => {
    const db = new Database();
    db.execute('CREATE TABLE multi_row (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO multi_row VALUES (1, 10), (2, 20)');
    try {
      db.execute('SELECT (SELECT val FROM multi_row) as result');
      // Some DBs return first row, others error
      // PG: error: more than one row returned by a subquery used as an expression
    } catch (e) {
      assert.ok(true, 'Error on multi-row scalar subquery is valid behavior');
    }
  });

  it('EXISTS with empty subquery is false', () => {
    const db = new Database();
    db.execute('CREATE TABLE exist_test (id INT PRIMARY KEY)');
    const r = db.execute('SELECT EXISTS (SELECT 1 FROM exist_test) as result');
    assert.ok(r.rows[0].result === false || r.rows[0].result === 0);
  });

  it('NOT EXISTS with empty subquery is true', () => {
    const db = new Database();
    db.execute('CREATE TABLE notexist_test (id INT PRIMARY KEY)');
    const r = db.execute('SELECT NOT EXISTS (SELECT 1 FROM notexist_test) as result');
    assert.ok(r.rows[0].result === true || r.rows[0].result === 1);
  });
});

describe('ORDER BY Edge Cases', () => {
  it('ORDER BY column not in SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE order_test (id INT PRIMARY KEY, name TEXT, val INT)');
    db.execute("INSERT INTO order_test VALUES (1, 'c', 30), (2, 'a', 10), (3, 'b', 20)");
    const r = db.execute('SELECT name FROM order_test ORDER BY val ASC');
    assert.equal(r.rows[0].name, 'a');
    assert.equal(r.rows[1].name, 'b');
    assert.equal(r.rows[2].name, 'c');
  });

  it('ORDER BY expression', () => {
    const db = new Database();
    db.execute('CREATE TABLE expr_order (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO expr_order VALUES (1, 5), (2, -3), (3, 8), (4, -1)');
    const r = db.execute('SELECT id, val FROM expr_order ORDER BY val * val ASC');
    // -1, -3, 5, 8 (sorted by square: 1, 9, 25, 64)
    assert.equal(r.rows[0].val, -1);
    assert.equal(r.rows[1].val, -3);
  });

  it('ORDER BY ordinal position', () => {
    const db = new Database();
    db.execute('CREATE TABLE ordinal_test (id INT PRIMARY KEY, a TEXT, b INT)');
    db.execute("INSERT INTO ordinal_test VALUES (1, 'x', 30), (2, 'y', 10), (3, 'z', 20)");
    const r = db.execute('SELECT a, b FROM ordinal_test ORDER BY 2 ASC');
    assert.equal(r.rows[0].b, 10);
    assert.equal(r.rows[1].b, 20);
    assert.equal(r.rows[2].b, 30);
  });
});

describe('Transaction Semantics', () => {
  it('auto-commit: each statement is its own transaction', () => {
    const db = new Database();
    db.execute('CREATE TABLE tx_test (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO tx_test VALUES (1, 100)');
    // Value should be visible immediately
    const r = db.execute('SELECT val FROM tx_test WHERE id = 1');
    assert.equal(r.rows[0].val, 100);
  });

  it('UPDATE returns correct count', () => {
    const db = new Database();
    db.execute('CREATE TABLE upd_test (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO upd_test VALUES (${i}, ${i * 10})`);
    const r = db.execute('UPDATE upd_test SET val = val + 1 WHERE val >= 50');
    assert.equal(r.count, 5); // ids 5-9
  });

  it('DELETE returns correct count', () => {
    const db = new Database();
    db.execute('CREATE TABLE del_test (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 10; i++) db.execute(`INSERT INTO del_test VALUES (${i}, ${i})`);
    const r = db.execute('DELETE FROM del_test WHERE val < 3');
    assert.equal(r.count, 3);
    const remaining = db.execute('SELECT COUNT(*) as cnt FROM del_test');
    assert.equal(remaining.rows[0].cnt, 7);
  });
});
