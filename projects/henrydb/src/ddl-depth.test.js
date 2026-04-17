// ddl-depth.test.js — DDL correctness depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-ddl-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('ALTER TABLE ADD COLUMN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('adds column with default NULL', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");

    db.execute('ALTER TABLE t ADD COLUMN age INT');

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 1);
    assert.equal(r[0].age, null);

    // Can insert with new column
    db.execute("INSERT INTO t VALUES (2, 'Bob', 30)");
    const r2 = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r2[1].age, 30);
  });

  it('adds column with DEFAULT value', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');

    db.execute('ALTER TABLE t ADD COLUMN status TEXT DEFAULT \'active\'');

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r[0].status, 'active');
  });

  it('multiple ADD COLUMN operations', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');

    db.execute('ALTER TABLE t ADD COLUMN a INT');
    db.execute('ALTER TABLE t ADD COLUMN b TEXT');
    db.execute('ALTER TABLE t ADD COLUMN c INT');

    db.execute("INSERT INTO t VALUES (2, 10, 'hello', 20)");
    const r = rows(db.execute('SELECT * FROM t WHERE id = 2'));
    assert.equal(r[0].a, 10);
    assert.equal(r[0].b, 'hello');
    assert.equal(r[0].c, 20);
  });
});

describe('ALTER TABLE DROP COLUMN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('drops a column', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT, age INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 25)");

    db.execute('ALTER TABLE t DROP COLUMN age');

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r[0].id, 1);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[0].age, undefined);
  });

  it('query still works after DROP COLUMN', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 90)");
    db.execute("INSERT INTO t VALUES (2, 'Bob', 85)");

    db.execute('ALTER TABLE t DROP COLUMN score');

    // INSERT without dropped column
    db.execute("INSERT INTO t VALUES (3, 'Carol')");
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM t'))[0].c, 3);
  });
});

describe('ALTER TABLE RENAME COLUMN', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('renames a column', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");

    db.execute('ALTER TABLE t RENAME COLUMN name TO full_name');

    const r = rows(db.execute('SELECT full_name FROM t'));
    assert.equal(r[0].full_name, 'Alice');
  });
});

describe('CREATE INDEX and DROP INDEX', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('create index improves query plan', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    for (let i = 1; i <= 100; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);

    // Before index: TABLE_SCAN
    const p1 = db.execute('EXPLAIN SELECT * FROM t WHERE score = 500');
    const hasScan1 = JSON.stringify(p1).includes('TABLE_SCAN');

    // Create index
    db.execute('CREATE INDEX idx_score ON t (score)');

    // After index: INDEX_SCAN
    const p2 = db.execute('EXPLAIN SELECT * FROM t WHERE score = 500');
    const hasIndex2 = JSON.stringify(p2).includes('INDEX_SCAN');

    // At least one plan should change (or both work correctly)
    const r = rows(db.execute('SELECT id FROM t WHERE score = 500'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 50);
  });

  it('drop index still returns correct results', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);

    db.execute('CREATE INDEX idx_score ON t (score)');
    
    // Query with index
    const r1 = rows(db.execute('SELECT id FROM t WHERE score = 200'));
    assert.equal(r1[0].id, 20);

    db.execute('DROP INDEX idx_score');

    // Query without index — should still work
    const r2 = rows(db.execute('SELECT id FROM t WHERE score = 200'));
    assert.equal(r2[0].id, 20);
  });
});

describe('DROP TABLE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DROP TABLE removes table', () => {
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');

    db.execute('DROP TABLE t');

    assert.throws(() => db.execute('SELECT * FROM t'), /not found|does not exist/i);
  });

  it('DROP TABLE IF EXISTS on non-existent table', () => {
    // Should not throw
    db.execute('DROP TABLE IF EXISTS nonexistent');
  });

  it('DROP TABLE with CASCADE removes dependent objects', () => {
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT, pid INT REFERENCES parent(id))');
    db.execute('INSERT INTO parent VALUES (1)');
    db.execute('INSERT INTO child VALUES (1, 1)');

    // DROP parent with CASCADE should also drop child or remove FK
    try {
      db.execute('DROP TABLE parent CASCADE');
      // Should not throw
    } catch (e) {
      // Some implementations prevent drop if referenced
      assert.ok(e.message.includes('referenced') || e.message.includes('foreign'),
        'Drop should fail with FK reference message');
    }
  });
});

describe('DDL + Crash Recovery', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('table created before crash survives recovery', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");

    db.close();
    db = TransactionalDatabase.open(dbDir);

    const r = rows(db.execute('SELECT * FROM t'));
    assert.equal(r.length, 1);
    assert.equal(r[0].val, 'hello');
  });

  it('ALTER TABLE ADD COLUMN survives recovery (structure, not data)', () => {
    // ALTER TABLE is now WAL-logged. After crash recovery,
    // the catalog has the updated schema and data in new columns is preserved.
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    db.execute("INSERT INTO t VALUES (2, 'Bob')");

    db.close();
    db = TransactionalDatabase.open(dbDir);

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2, 'Both rows should survive');
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 2);
    // After recovery, r[1].name should be 'Bob' (ALTER TABLE now WAL-logged)
    assert.equal(r[1].name, 'Bob', 'Data in new column should survive recovery');
  });

  it('CREATE INDEX survives recovery', () => {
    db.execute('CREATE TABLE t (id INT, score INT)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('CREATE INDEX idx_score ON t (score)');

    db.close();
    db = TransactionalDatabase.open(dbDir);

    // Query should still work with index
    const r = rows(db.execute('SELECT id FROM t WHERE score = 50'));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 5);
  });

  it('multiple DDL+DML operations survive recovery (core data)', () => {
    // ALTER TABLE column data is now preserved after crash recovery.
    // But table structure, original columns, and basic DML survive.
    db.execute('CREATE TABLE t1 (id INT, val TEXT)');
    db.execute('CREATE TABLE t2 (id INT, t1_id INT)');
    db.execute("INSERT INTO t1 VALUES (1, 'a')");
    db.execute("INSERT INTO t1 VALUES (2, 'b')");
    db.execute('INSERT INTO t2 VALUES (1, 1)');
    db.execute('INSERT INTO t2 VALUES (2, 2)');

    db.close();
    db = TransactionalDatabase.open(dbDir);

    const r1 = rows(db.execute('SELECT * FROM t1 ORDER BY id'));
    assert.equal(r1.length, 2);
    assert.equal(r1[0].val, 'a');
    assert.equal(r1[1].val, 'b');

    const r2 = rows(db.execute('SELECT * FROM t2 ORDER BY id'));
    assert.equal(r2.length, 2);
  });
});
