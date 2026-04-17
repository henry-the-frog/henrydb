// savepoint-depth.test.js — Transaction savepoint depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-sp-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE t (id INT, val TEXT)');
  db.execute("INSERT INTO t VALUES (1, 'original')");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Basic Savepoint', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SAVEPOINT and RELEASE', () => {
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (2, 'second')");
    s.execute('SAVEPOINT sp1');
    s.execute("INSERT INTO t VALUES (3, 'third')");
    s.execute('RELEASE SAVEPOINT sp1');
    s.commit();
    s.close();

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 3, 'All rows should be committed');
  });

  it('ROLLBACK TO SAVEPOINT undoes partial work', () => {
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (2, 'keep')");
    s.execute('SAVEPOINT sp1');
    s.execute("INSERT INTO t VALUES (3, 'rollback_me')");
    s.execute('ROLLBACK TO SAVEPOINT sp1');
    s.commit();
    s.close();

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2, 'Only 2 rows: original + keep');
    assert.equal(r[1].val, 'keep');
  });

  it('work after ROLLBACK TO SAVEPOINT is kept', () => {
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (2, 'before_sp')");
    s.execute('SAVEPOINT sp1');
    s.execute("INSERT INTO t VALUES (3, 'rolled_back')");
    s.execute('ROLLBACK TO SAVEPOINT sp1');
    // Work after rollback to savepoint
    s.execute("INSERT INTO t VALUES (4, 'after_rollback')");
    s.commit();
    s.close();

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 3); // original + before_sp + after_rollback
    assert.ok(!r.some(x => x.val === 'rolled_back'));
    assert.ok(r.some(x => x.val === 'after_rollback'));
  });
});

describe('Nested Savepoints', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('nested savepoints: rollback inner keeps outer', () => {
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (2, 'outer')");
    s.execute('SAVEPOINT sp_outer');
    s.execute("INSERT INTO t VALUES (3, 'inner')");
    s.execute('SAVEPOINT sp_inner');
    s.execute("INSERT INTO t VALUES (4, 'innermost')");
    
    // Rollback inner savepoint
    s.execute('ROLLBACK TO SAVEPOINT sp_inner');
    s.commit();
    s.close();

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    // Should have: original(1), outer(2), inner(3)
    // innermost(4) was rolled back
    assert.ok(r.some(x => x.val === 'inner'), 'Inner should survive');
    assert.ok(!r.some(x => x.val === 'innermost'), 'Innermost should be rolled back');
  });

  it('rollback outer savepoint undoes inner work too', () => {
    const s = db.session();
    s.begin();
    s.execute('SAVEPOINT sp_outer');
    s.execute("INSERT INTO t VALUES (2, 'outer_work')");
    s.execute('SAVEPOINT sp_inner');
    s.execute("INSERT INTO t VALUES (3, 'inner_work')");
    
    // Rollback outer — should undo both
    s.execute('ROLLBACK TO SAVEPOINT sp_outer');
    s.commit();
    s.close();

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 1, 'Only original should remain');
    assert.equal(r[0].val, 'original');
  });
});

describe('Savepoint + Full Rollback', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('full rollback undoes everything including savepoints', () => {
    const s = db.session();
    s.begin();
    s.execute("INSERT INTO t VALUES (2, 'a')");
    s.execute('SAVEPOINT sp1');
    s.execute("INSERT INTO t VALUES (3, 'b')");
    s.execute('RELEASE SAVEPOINT sp1');
    
    // Full rollback
    s.rollback();
    s.close();

    const r = rows(db.execute('SELECT COUNT(*) AS c FROM t'));
    assert.equal(r[0].c, 1, 'Full rollback should undo everything');
  });
});
