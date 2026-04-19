// constraints-comprehensive.test.js — Comprehensive constraint validation tests

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('PRIMARY KEY Constraints', () => {
  it('rejects duplicate PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 'b')"), /UNIQUE|PRIMARY|duplicate/i);
  });

  it('rejects NULL PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
    assert.throws(() => db.execute("INSERT INTO t VALUES (NULL, 'a')"), /NULL|NOT NULL/i);
  });

  it('composite PK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, val TEXT, PRIMARY KEY (a, b))');
    db.execute("INSERT INTO t VALUES (1, 1, 'a')");
    db.execute("INSERT INTO t VALUES (1, 2, 'b')"); // Different composite key
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 1, 'c')"), /UNIQUE|PRIMARY|duplicate/i);
  });
});

describe('UNIQUE Constraints', () => {
  it('single column UNIQUE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)');
    db.execute("INSERT INTO t VALUES (1, 'a@test.com')");
    assert.throws(() => db.execute("INSERT INTO t VALUES (2, 'a@test.com')"), /UNIQUE|duplicate/i);
  });

  it('UNIQUE allows multiple NULLs', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, email TEXT UNIQUE)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, NULL)'); // Multiple NULLs should be allowed
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 2);
  });

  it('multi-column UNIQUE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, c TEXT, UNIQUE(a, b))');
    db.execute("INSERT INTO t VALUES (1, 1, 'first')");
    db.execute("INSERT INTO t VALUES (1, 2, 'ok')"); // Different composite
    db.execute("INSERT INTO t VALUES (2, 1, 'ok')"); // Different composite
    assert.throws(() => db.execute("INSERT INTO t VALUES (1, 1, 'dup')"), /UNIQUE|duplicate/i);
  });

  it('multi-column UNIQUE with NULL', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (a INT, b INT, UNIQUE(a, b))');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (1, NULL)'); // NULLs make the tuple unique
    const r = db.execute('SELECT COUNT(*) as cnt FROM t');
    assert.equal(r.rows[0].cnt, 2);
  });
});

describe('NOT NULL Constraints', () => {
  it('rejects NULL insert', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    assert.throws(() => db.execute('INSERT INTO t VALUES (1, NULL)'), /NOT NULL/i);
  });

  it('rejects NULL update', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db.execute("INSERT INTO t VALUES (1, 'valid')");
    assert.throws(() => db.execute('UPDATE t SET name = NULL WHERE id = 1'), /NOT NULL/i);
  });

  it('allows empty string (not NULL)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db.execute("INSERT INTO t VALUES (1, '')");
    const r = db.execute('SELECT name FROM t WHERE id = 1');
    assert.equal(r.rows[0].name, '');
  });
});

describe('CHECK Constraints', () => {
  it('single column CHECK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT CHECK (val > 0))');
    db.execute('INSERT INTO t VALUES (1, 10)');
    assert.throws(() => db.execute('INSERT INTO t VALUES (2, -5)'), /CHECK/i);
  });

  it('CHECK on update', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT CHECK (val > 0))');
    db.execute('INSERT INTO t VALUES (1, 10)');
    assert.throws(() => db.execute('UPDATE t SET val = -1 WHERE id = 1'), /CHECK/i);
    // Verify unchanged
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 10);
  });

  it('CHECK allows NULL (constraint not violated)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT CHECK (val > 0))');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    // NULL should pass CHECK — per SQL standard, NULL in CHECK is treated as true
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, null);
  });
});

describe('DEFAULT Values', () => {
  it('numeric default', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT DEFAULT 42)');
    db.execute('INSERT INTO t (id) VALUES (1)');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 42);
  });

  it('string default', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, status TEXT DEFAULT 'active')");
    db.execute('INSERT INTO t (id) VALUES (1)');
    assert.equal(db.execute('SELECT status FROM t WHERE id = 1').rows[0].status, 'active');
  });

  it('CURRENT_TIMESTAMP default', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, created TEXT DEFAULT CURRENT_TIMESTAMP)');
    db.execute('INSERT INTO t (id) VALUES (1)');
    const r = db.execute('SELECT created FROM t WHERE id = 1');
    assert.ok(r.rows[0].created.includes('T'), 'Should be ISO timestamp');
  });

  it('explicit NULL overrides default', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT DEFAULT 42)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, null);
  });
});

describe('FOREIGN KEY Constraints', () => {
  it('rejects invalid FK reference', () => {
    const db = new Database();
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))');
    db.execute('INSERT INTO parent VALUES (1)');
    assert.throws(() => db.execute('INSERT INTO child VALUES (1, 99)'), /Foreign key|not found/i);
  });

  it('allows NULL FK', () => {
    const db = new Database();
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))');
    db.execute('INSERT INTO child VALUES (1, NULL)');
    assert.equal(db.execute('SELECT parent_id FROM child WHERE id = 1').rows[0].parent_id, null);
  });

  it('prevents deletion of referenced parent', () => {
    const db = new Database();
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))');
    db.execute('INSERT INTO parent VALUES (1), (2)');
    db.execute('INSERT INTO child VALUES (1, 1)');
    assert.throws(() => db.execute('DELETE FROM parent WHERE id = 1'), /referenced|Cannot delete/i);
    // Non-referenced parent can be deleted
    db.execute('DELETE FROM parent WHERE id = 2');
  });

  it('valid FK insert', () => {
    const db = new Database();
    db.execute('CREATE TABLE parent (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE child (id INT PRIMARY KEY, parent_id INT REFERENCES parent(id))');
    db.execute('INSERT INTO parent VALUES (1), (2), (3)');
    db.execute('INSERT INTO child VALUES (1, 1), (2, 2), (3, 3)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM child').rows[0].cnt, 3);
  });
});

describe('Triggers', () => {
  it('AFTER INSERT trigger fires', () => {
    const db = new Database();
    db.execute('CREATE TABLE main_t (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE log_t (action TEXT, ref_id INT)');
    db.execute("CREATE TRIGGER tr1 AFTER INSERT ON main_t FOR EACH ROW INSERT INTO log_t VALUES ('INSERT', NEW.id)");
    db.execute('INSERT INTO main_t VALUES (1, 100)');
    const r = db.execute('SELECT * FROM log_t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].action, 'INSERT');
    assert.equal(r.rows[0].ref_id, 1);
  });

  it('AFTER UPDATE trigger fires', () => {
    const db = new Database();
    db.execute('CREATE TABLE main_t (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE log_t (action TEXT, old_val INT, new_val INT)');
    db.execute("CREATE TRIGGER tr1 AFTER UPDATE ON main_t FOR EACH ROW INSERT INTO log_t VALUES ('UPDATE', OLD.val, NEW.val)");
    db.execute('INSERT INTO main_t VALUES (1, 100)');
    db.execute('UPDATE main_t SET val = 200 WHERE id = 1');
    const r = db.execute('SELECT * FROM log_t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].old_val, 100);
    assert.equal(r.rows[0].new_val, 200);
  });

  it('AFTER DELETE trigger fires', () => {
    const db = new Database();
    db.execute('CREATE TABLE main_t (id INT PRIMARY KEY, val INT)');
    db.execute('CREATE TABLE log_t (action TEXT, ref_id INT)');
    db.execute("CREATE TRIGGER tr1 AFTER DELETE ON main_t FOR EACH ROW INSERT INTO log_t VALUES ('DELETE', OLD.id)");
    db.execute('INSERT INTO main_t VALUES (1, 100)');
    db.execute('DELETE FROM main_t WHERE id = 1');
    const r = db.execute('SELECT * FROM log_t');
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].action, 'DELETE');
  });

  it('multiple triggers on same event', () => {
    const db = new Database();
    db.execute('CREATE TABLE main_t (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE log1 (msg TEXT)');
    db.execute('CREATE TABLE log2 (msg TEXT)');
    db.execute("CREATE TRIGGER tr1 AFTER INSERT ON main_t FOR EACH ROW INSERT INTO log1 VALUES ('first')");
    db.execute("CREATE TRIGGER tr2 AFTER INSERT ON main_t FOR EACH ROW INSERT INTO log2 VALUES ('second')");
    db.execute('INSERT INTO main_t VALUES (1)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM log1').rows[0].cnt, 1);
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM log2').rows[0].cnt, 1);
  });
});

describe('ALTER TABLE Constraints', () => {
  it('ADD COLUMN with DEFAULT', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a'), (2, 'b')");
    db.execute("ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'active'");
    const r = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(r.rows[0].status, 'active');
    assert.equal(r.rows[1].status, 'active');
  });

  it('ADD COLUMN with DEFAULT populates new inserts', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("ALTER TABLE t ADD COLUMN val INT DEFAULT 0");
    // Use column-list syntax to trigger default
    db.execute("INSERT INTO t (id, name) VALUES (2, 'b')");
    const r = db.execute('SELECT val FROM t WHERE id = 2');
    assert.equal(r.rows[0].val, 0);
  });

  it('DROP COLUMN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT, extra INT)');
    db.execute("INSERT INTO t VALUES (1, 'a', 42)");
    db.execute('ALTER TABLE t DROP COLUMN extra');
    const r = db.execute('SELECT * FROM t WHERE id = 1');
    assert.ok(!('extra' in r.rows[0]), 'Column should be dropped');
    assert.equal(r.rows[0].name, 'a');
  });

  it('RENAME COLUMN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, old_name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'hello')");
    db.execute('ALTER TABLE t RENAME COLUMN old_name TO new_name');
    const r = db.execute('SELECT new_name FROM t WHERE id = 1');
    assert.equal(r.rows[0].new_name, 'hello');
  });
});
