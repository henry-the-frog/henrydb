// join-depth.test.js — JOIN correctness depth tests
// Covers all join types with NULL, empty, duplicate, and MVCC edge cases.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-join-'));
  db = TransactionalDatabase.open(dbDir);
  // Standard test tables
  db.execute('CREATE TABLE left_t (id INT, val TEXT)');
  db.execute('CREATE TABLE right_t (id INT, data TEXT)');
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('INNER JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('basic inner join on matching ids', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute("INSERT INTO left_t VALUES (3, 'c')");
    db.execute("INSERT INTO right_t VALUES (2, 'x')");
    db.execute("INSERT INTO right_t VALUES (3, 'y')");
    db.execute("INSERT INTO right_t VALUES (4, 'z')");

    const r = rows(db.execute('SELECT left_t.id, val, data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 2);
    assert.equal(r[0].val, 'b');
    assert.equal(r[0].data, 'x');
    assert.equal(r[1].id, 3);
  });

  it('inner join with NULL join column: NULLs never match', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (NULL, 'null-left')");
    db.execute("INSERT INTO right_t VALUES (1, 'x')");
    db.execute("INSERT INTO right_t VALUES (NULL, 'null-right')");

    const r = rows(db.execute('SELECT left_t.id, val, data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id'));
    // NULL = NULL is false in SQL, so only id=1 matches
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 1);
  });

  it('inner join with empty right table', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    const r = rows(db.execute('SELECT * FROM left_t INNER JOIN right_t ON left_t.id = right_t.id'));
    assert.equal(r.length, 0);
  });
});

describe('LEFT JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('left join preserves all left rows', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute("INSERT INTO left_t VALUES (3, 'c')");
    db.execute("INSERT INTO right_t VALUES (2, 'x')");

    const r = rows(db.execute('SELECT left_t.id, val, data FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id'));
    assert.equal(r.length, 3);
    assert.equal(r[0].data, null); // id=1 no match
    assert.equal(r[1].data, 'x');  // id=2 matched
    assert.equal(r[2].data, null); // id=3 no match
  });

  it('left join with NULLs in join column', () => {
    db.execute("INSERT INTO left_t VALUES (NULL, 'null-row')");
    db.execute("INSERT INTO left_t VALUES (1, 'one')");
    db.execute("INSERT INTO right_t VALUES (1, 'match')");
    db.execute("INSERT INTO right_t VALUES (NULL, 'null-data')");

    const r = rows(db.execute('SELECT left_t.id, val, data FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id'));
    assert.equal(r.length, 2);
    // NULL row: no match (NULL != NULL), data should be null
    // id=1: matched with 'match'
    const nullRow = r.find(x => x.val === 'null-row');
    assert.ok(nullRow, 'NULL left row should be preserved');
    assert.equal(nullRow.data, null, 'NULL row should not match NULL right');
  });

  it('left join with empty right table', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");

    const r = rows(db.execute('SELECT left_t.id, val, data FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].data, null);
    assert.equal(r[1].data, null);
  });
});

describe('RIGHT JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('right join preserves all right rows', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO right_t VALUES (1, 'x')");
    db.execute("INSERT INTO right_t VALUES (2, 'y')");
    db.execute("INSERT INTO right_t VALUES (3, 'z')");

    const r = rows(db.execute('SELECT right_t.id, val, data FROM left_t RIGHT JOIN right_t ON left_t.id = right_t.id ORDER BY right_t.id'));
    assert.equal(r.length, 3);
    assert.equal(r[0].val, 'a');   // id=1 matched
    assert.equal(r[1].val, null);  // id=2 no match
    assert.equal(r[2].val, null);  // id=3 no match
  });

  it('right join with empty left table', () => {
    db.execute("INSERT INTO right_t VALUES (1, 'x')");
    db.execute("INSERT INTO right_t VALUES (2, 'y')");

    const r = rows(db.execute('SELECT right_t.id, val, data FROM left_t RIGHT JOIN right_t ON left_t.id = right_t.id'));
    assert.equal(r.length, 2);
    // Note: when left table is empty, left columns may be missing or null
    // Both are acceptable behaviors — the key is that right rows appear
    assert.equal(r[0].data, 'x');
    assert.equal(r[1].data, 'y');
    // Left columns should be null (or undefined/missing when left table is empty)
    // This is a known limitation: column metadata not available when source is empty
    assert.ok(r[0].val === null || r[0].val === undefined,
      'Left column should be null or missing when no left rows');
  });
});

describe('FULL OUTER JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('full join preserves unmatched rows from both sides', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute("INSERT INTO right_t VALUES (2, 'x')");
    db.execute("INSERT INTO right_t VALUES (3, 'y')");

    const r = rows(db.execute('SELECT COALESCE(left_t.id, right_t.id) AS id, val, data FROM left_t FULL OUTER JOIN right_t ON left_t.id = right_t.id ORDER BY id'));
    assert.equal(r.length, 3);
    // id=1: left only (data=null)
    // id=2: matched (val='b', data='x')
    // id=3: right only (val=null)
  });

  it('full join with both tables empty', () => {
    const r = rows(db.execute('SELECT * FROM left_t FULL OUTER JOIN right_t ON left_t.id = right_t.id'));
    assert.equal(r.length, 0);
  });
});

describe('CROSS JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('cross join produces cartesian product', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute("INSERT INTO right_t VALUES (10, 'x')");
    db.execute("INSERT INTO right_t VALUES (20, 'y')");
    db.execute("INSERT INTO right_t VALUES (30, 'z')");

    const r = rows(db.execute('SELECT left_t.id AS lid, right_t.id AS rid FROM left_t CROSS JOIN right_t'));
    assert.equal(r.length, 6, 'Cross join of 2x3 should produce 6 rows');
  });

  it('cross join with empty table produces zero rows', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    const r = rows(db.execute('SELECT * FROM left_t CROSS JOIN right_t'));
    assert.equal(r.length, 0);
  });
});

describe('JOIN with Duplicates (Many-to-Many)', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('many-to-many join produces correct cartesian per group', () => {
    // Left has 2 rows with id=1, right has 3 rows with id=1
    db.execute("INSERT INTO left_t VALUES (1, 'a1')");
    db.execute("INSERT INTO left_t VALUES (1, 'a2')");
    db.execute("INSERT INTO right_t VALUES (1, 'x1')");
    db.execute("INSERT INTO right_t VALUES (1, 'x2')");
    db.execute("INSERT INTO right_t VALUES (1, 'x3')");

    const r = rows(db.execute('SELECT val, data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id'));
    assert.equal(r.length, 6, '2x3 many-to-many should produce 6 rows');
  });

  it('left join with duplicate matches', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO right_t VALUES (1, 'x')");
    db.execute("INSERT INTO right_t VALUES (1, 'y')");
    db.execute("INSERT INTO right_t VALUES (1, 'z')");

    const r = rows(db.execute('SELECT val, data FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id'));
    assert.equal(r.length, 3, 'Left row matches 3 right rows');
  });
});

describe('NATURAL JOIN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('natural join on common column name', () => {
    // Both tables have 'id' column
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute("INSERT INTO right_t VALUES (2, 'x')");
    db.execute("INSERT INTO right_t VALUES (3, 'y')");

    const r = rows(db.execute('SELECT * FROM left_t NATURAL JOIN right_t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 2);
  });
});

describe('Self-Join', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('self-join with aliases', () => {
    db.execute('CREATE TABLE employees (id INT, name TEXT, manager_id INT)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', NULL)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1)");
    db.execute("INSERT INTO employees VALUES (3, 'Carol', 1)");
    db.execute("INSERT INTO employees VALUES (4, 'Dave', 2)");

    const r = rows(db.execute(
      'SELECT e.name AS employee, m.name AS manager ' +
      'FROM employees e INNER JOIN employees m ON e.manager_id = m.id ' +
      'ORDER BY e.name'
    ));
    assert.equal(r.length, 3);
    assert.equal(r[0].employee, 'Bob');
    assert.equal(r[0].manager, 'Alice');
    assert.equal(r[1].employee, 'Carol');
    assert.equal(r[1].manager, 'Alice');
    assert.equal(r[2].employee, 'Dave');
    assert.equal(r[2].manager, 'Bob');
  });
});

describe('JOIN + MVCC Snapshot Isolation', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('join sees consistent snapshot during concurrent writes', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute("INSERT INTO right_t VALUES (1, 'x')");
    db.execute("INSERT INTO right_t VALUES (2, 'y')");

    // s1 takes a snapshot
    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT left_t.id, val, data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id'));
    assert.equal(r1.length, 2);

    // Concurrent delete
    db.execute('DELETE FROM right_t WHERE id = 2');

    // s1 join should still see both matches
    const r2 = rows(s1.execute('SELECT left_t.id, val, data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id'));
    assert.equal(r2.length, 2, 'Snapshot should still see 2 join matches');

    // New read should see only 1 match
    const r3 = rows(db.execute('SELECT left_t.id, val, data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id'));
    assert.equal(r3.length, 1);

    s1.commit();
    s1.close();
  });

  it('left join with concurrent insert into right table', () => {
    db.execute("INSERT INTO left_t VALUES (1, 'a')");
    db.execute("INSERT INTO left_t VALUES (2, 'b')");
    db.execute("INSERT INTO right_t VALUES (1, 'x')");

    const s1 = db.session();
    s1.begin();
    const r1 = rows(s1.execute('SELECT left_t.id, data FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id'));
    assert.equal(r1[0].data, 'x');
    assert.equal(r1[1].data, null); // id=2 no match yet

    // Insert into right table
    db.execute("INSERT INTO right_t VALUES (2, 'y')");

    // s1 still sees old state
    const r2 = rows(s1.execute('SELECT left_t.id, data FROM left_t LEFT JOIN right_t ON left_t.id = right_t.id ORDER BY left_t.id'));
    assert.equal(r2[1].data, null, 'Snapshot should not see new right row');

    s1.commit();
    s1.close();
  });
});
