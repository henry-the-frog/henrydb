// multi-statement-depth.test.js — Multi-statement script tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-multi-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('Multi-Statement', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('sequential statements work correctly', () => {
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    db.execute("INSERT INTO t VALUES (2, 'b')");
    db.execute("INSERT INTO t VALUES (3, 'c')");
    db.execute('DELETE FROM t WHERE id = 2');
    db.execute("UPDATE t SET val = 'x' WHERE id = 1");

    const r = rows(db.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].val, 'x');
    assert.equal(r[1].val, 'c');
  });

  it('DDL then DML', () => {
    db.execute('CREATE TABLE t1 (id INT)');
    db.execute('CREATE TABLE t2 (id INT, t1_id INT)');
    db.execute('INSERT INTO t1 VALUES (1)');
    db.execute('INSERT INTO t2 VALUES (1, 1)');

    const r = rows(db.execute(
      'SELECT t1.id, t2.id AS t2_id FROM t1 INNER JOIN t2 ON t1.id = t2.t1_id'
    ));
    assert.equal(r.length, 1);
  });

  it('DML interleaved with reads', () => {
    db.execute('CREATE TABLE t (id INT, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    let r = rows(db.execute('SELECT SUM(val) AS s FROM t'));
    assert.equal(r[0].s, 10);
    
    db.execute('INSERT INTO t VALUES (2, 20)');
    r = rows(db.execute('SELECT SUM(val) AS s FROM t'));
    assert.equal(r[0].s, 30);
    
    db.execute('UPDATE t SET val = val * 2');
    r = rows(db.execute('SELECT SUM(val) AS s FROM t'));
    assert.equal(r[0].s, 60);
  });
});
