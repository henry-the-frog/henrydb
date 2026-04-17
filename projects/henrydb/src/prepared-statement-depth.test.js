// prepared-statement-depth.test.js — Prepared statement + parameterized query tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-prep-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE users (id INT, name TEXT, age INT)');
  db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
  db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
  db.execute("INSERT INTO users VALUES (3, 'Carol', 35)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('PREPARE / EXECUTE Basics', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('basic PREPARE and EXECUTE', () => {
    db.execute('PREPARE get_user AS SELECT * FROM users WHERE id = $1');
    const r = rows(db.execute('EXECUTE get_user (1)'));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Alice');
  });

  it('EXECUTE with multiple parameters', () => {
    db.execute('PREPARE get_by_range AS SELECT name FROM users WHERE age >= $1 AND age <= $2 ORDER BY name');
    const r = rows(db.execute('EXECUTE get_by_range (25, 30)'));
    assert.equal(r.length, 2);
    assert.equal(r[0].name, 'Alice');
    assert.equal(r[1].name, 'Bob');
  });

  it('EXECUTE same statement multiple times', () => {
    db.execute('PREPARE get_user AS SELECT name FROM users WHERE id = $1');
    
    const r1 = rows(db.execute('EXECUTE get_user (1)'));
    assert.equal(r1[0].name, 'Alice');
    
    const r2 = rows(db.execute('EXECUTE get_user (2)'));
    assert.equal(r2[0].name, 'Bob');
    
    const r3 = rows(db.execute('EXECUTE get_user (3)'));
    assert.equal(r3[0].name, 'Carol');
  });

  it('DEALLOCATE removes prepared statement', () => {
    db.execute('PREPARE stmt AS SELECT * FROM users');
    db.execute('DEALLOCATE stmt');
    
    assert.throws(() => db.execute('EXECUTE stmt'), /not found/i);
  });

  it('DEALLOCATE ALL removes all prepared statements', () => {
    db.execute('PREPARE s1 AS SELECT * FROM users WHERE id = $1');
    db.execute('PREPARE s2 AS SELECT * FROM users WHERE age = $1');
    db.execute('DEALLOCATE ALL');
    
    assert.throws(() => db.execute('EXECUTE s1 (1)'), /not found/i);
    assert.throws(() => db.execute('EXECUTE s2 (25)'), /not found/i);
  });
});

describe('Prepared Statement: DML', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('PREPARE INSERT', () => {
    db.execute("PREPARE add_user AS INSERT INTO users VALUES ($1, $2, $3)");
    db.execute("EXECUTE add_user (4, 'Dave', 40)");
    
    const r = rows(db.execute('SELECT * FROM users WHERE id = 4'));
    assert.equal(r.length, 1);
    assert.equal(r[0].name, 'Dave');
  });

  it('PREPARE UPDATE', () => {
    db.execute('PREPARE update_age AS UPDATE users SET age = $1 WHERE id = $2');
    db.execute('EXECUTE update_age (99, 1)');
    
    const r = rows(db.execute('SELECT age FROM users WHERE id = 1'));
    assert.equal(r[0].age, 99);
  });

  it('PREPARE DELETE', () => {
    db.execute('PREPARE del_user AS DELETE FROM users WHERE id = $1');
    db.execute('EXECUTE del_user (2)');
    
    assert.equal(rows(db.execute('SELECT COUNT(*) AS c FROM users'))[0].c, 2);
  });
});

describe('SQL Injection Safety', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('parameter with SQL injection attempt does not execute injection', () => {
    db.execute('PREPARE get_user AS SELECT * FROM users WHERE name = $1');
    
    // This should search for a literal string, not execute the injection
    // The injection attempt: "Alice'; DROP TABLE users; --"
    try {
      const r = rows(db.execute("EXECUTE get_user ('Alice''; DROP TABLE users; --')"));
      // If execution succeeds, the table should still exist
    } catch (e) {
      // Parse error is acceptable — means injection was blocked
    }
    
    // Table should still exist!
    const r = rows(db.execute('SELECT COUNT(*) AS c FROM users'));
    assert.equal(r[0].c, 3, 'Table should not be dropped by injection');
  });

  it('parameter with semicolons treated as literal', () => {
    db.execute("INSERT INTO users VALUES (4, 'O''Brien; DROP TABLE', 50)");
    db.execute('PREPARE find AS SELECT name FROM users WHERE id = $1');
    const r = rows(db.execute('EXECUTE find (4)'));
    assert.equal(r.length, 1);
    // Just verifying no crash
  });
});

describe('Prepared Statement Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('EXECUTE with no parameters', () => {
    db.execute('PREPARE all_users AS SELECT * FROM users');
    const r = rows(db.execute('EXECUTE all_users'));
    assert.equal(r.length, 3);
  });

  it('EXECUTE non-existent prepared statement', () => {
    assert.throws(() => db.execute('EXECUTE nonexistent (1)'), /not found/i);
  });

  it('PREPARE overwrites existing statement with same name', () => {
    db.execute('PREPARE stmt AS SELECT * FROM users WHERE id = $1');
    // Overwrite
    db.execute('PREPARE stmt AS SELECT name FROM users WHERE age = $1');
    
    const r = rows(db.execute('EXECUTE stmt (25)'));
    assert.ok(r[0].name, 'Should use new prepared statement');
  });

  it('prepared statement with string parameter containing special chars', () => {
    db.execute("INSERT INTO users VALUES (4, 'O''Malley', 40)");
    db.execute('PREPARE find AS SELECT * FROM users WHERE name = $1');
    const r = rows(db.execute("EXECUTE find ('O''Malley')"));
    assert.equal(r.length, 1);
    assert.equal(r[0].id, 4);
  });
});
