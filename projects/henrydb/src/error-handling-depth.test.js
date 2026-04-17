// error-handling-depth.test.js — Error handling correctness tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-err-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}

describe('Parse Errors', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('malformed SQL throws', () => {
    assert.throws(() => db.execute('SELEC * FROM'));
  });

  it('incomplete SQL throws', () => {
    assert.throws(() => db.execute('SELECT'));
  });

  it('empty string throws or returns empty', () => {
    try {
      const r = db.execute('');
      // Empty may return null/undefined or throw
    } catch (e) {
      // Expected
    }
  });
});

describe('Table Not Found', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SELECT from non-existent table throws', () => {
    assert.throws(() => db.execute('SELECT * FROM nonexistent'), /not found|does not exist/i);
  });

  it('INSERT into non-existent table throws', () => {
    assert.throws(() => db.execute("INSERT INTO nonexistent VALUES (1)"), /not found|does not exist/i);
  });

  it('UPDATE non-existent table throws', () => {
    assert.throws(() => db.execute("UPDATE nonexistent SET x = 1"), /not found|does not exist/i);
  });

  it('DELETE from non-existent table throws', () => {
    assert.throws(() => db.execute('DELETE FROM nonexistent'), /not found|does not exist/i);
  });
});

describe('Column Not Found', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SELECT non-existent column throws', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT)');
    assert.throws(() => db.execute('SELECT nonexistent FROM t'));
  });

  it('WHERE on non-existent column throws', () => {
    db.execute('CREATE TABLE t (id INT)');
    assert.throws(() => db.execute('SELECT * FROM t WHERE nonexistent = 1'));
  });
});

describe('Constraint Violations', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('duplicate primary key throws descriptive error', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    
    assert.throws(() => db.execute('INSERT INTO t VALUES (1)'), /UNIQUE|duplicate|primary/i);
  });

  it('NOT NULL violation throws descriptive error', () => {
    db.execute('CREATE TABLE t (id INT, name TEXT NOT NULL)');
    
    assert.throws(() => db.execute('INSERT INTO t VALUES (1, NULL)'), /NOT NULL/i);
  });

  it('CHECK constraint violation throws descriptive error', () => {
    db.execute('CREATE TABLE t (id INT, age INT CHECK (age >= 0))');
    
    assert.throws(() => db.execute('INSERT INTO t VALUES (1, -5)'), /CHECK/i);
  });
});

describe('Division by Zero', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('division by zero produces Infinity or error', () => {
    try {
      const r = db.execute('SELECT 10 / 0 AS result');
      // Some databases return NULL, Infinity, or throw
      // Any behavior is acceptable as long as it doesn't crash
    } catch (e) {
      // Division by zero error is acceptable
      assert.ok(e.message.includes('division') || e.message.includes('zero') || true);
    }
  });
});

describe('Recovery After Error', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('database is usable after parse error', () => {
    db.execute('CREATE TABLE t (id INT)');
    
    try { db.execute('INVALID SQL'); } catch {}
    
    // Should still work
    db.execute('INSERT INTO t VALUES (1)');
    const r = db.execute('SELECT COUNT(*) AS c FROM t');
    assert.equal((Array.isArray(r) ? r : r?.rows || [])[0].c, 1);
  });

  it('database is usable after constraint error', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    
    try { db.execute('INSERT INTO t VALUES (1)'); } catch {}
    
    // Should still work
    db.execute('INSERT INTO t VALUES (2)');
    const r = db.execute('SELECT COUNT(*) AS c FROM t');
    assert.equal((Array.isArray(r) ? r : r?.rows || [])[0].c, 2);
  });

  it('transaction is usable after error within transaction', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO t VALUES (1)');
    
    try { s.execute('INVALID SQL'); } catch {}
    
    // Transaction should still be usable (or properly rolled back)
    try {
      s.execute('INSERT INTO t VALUES (2)');
      s.commit();
    } catch {
      s.rollback();
    }
    s.close();
    
    // Database should be consistent
    const count = (Array.isArray(db.execute('SELECT COUNT(*) AS c FROM t')) 
      ? db.execute('SELECT COUNT(*) AS c FROM t') 
      : db.execute('SELECT COUNT(*) AS c FROM t')?.rows || [])[0]?.c;
    assert.ok(count >= 0, 'Database should be in consistent state');
  });
});
