// fk-mvcc.test.js — FK constraint validation under MVCC concurrent transactions
import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-fk-mvcc-'));
  db = TransactionalDatabase.open(dbDir);
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('FK MVCC Visibility', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('rejects INSERT referencing a row being deleted by another transaction', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute('INSERT INTO parents VALUES (1)');

    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();

    // s1 deletes parent
    s1.execute('DELETE FROM parents WHERE id = 1');

    // s2 tries to insert child referencing deleted parent
    assert.throws(
      () => s2.execute('INSERT INTO children VALUES (1, 1)'),
      /being deleted by another transaction|foreign/i
    );

    s1.execute('COMMIT');
    s2.execute('ROLLBACK');
  });

  it('allows INSERT referencing a row NOT being deleted', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id))');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO parents VALUES (2)');

    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();

    // s1 deletes parent 1
    s1.execute('DELETE FROM parents WHERE id = 1');

    // s2 inserts child referencing parent 2 (not being deleted) — should work
    s2.execute('INSERT INTO children VALUES (1, 2)');

    s1.execute('COMMIT');
    s2.execute('COMMIT');

    const r = rows(db.execute('SELECT * FROM children'));
    assert.equal(r.length, 1);
    assert.equal(r[0].p_id, 2);
  });

  it('CASCADE delete is visible within the same transaction', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO children VALUES (1, 1)');
    db.execute('INSERT INTO children VALUES (2, 1)');

    const s = db.session();
    s.begin();
    s.execute('DELETE FROM parents WHERE id = 1');

    const r = rows(s.execute('SELECT * FROM children'));
    assert.equal(r.length, 0, 'CASCADE should delete all children in same tx');
    s.execute('COMMIT');
  });

  it('CASCADE delete is invisible to concurrent transaction snapshot', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO children VALUES (1, 1)');
    db.execute('INSERT INTO children VALUES (2, 1)');

    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();

    // s1 cascade deletes
    s1.execute('DELETE FROM parents WHERE id = 1');

    // s2 still sees both children (snapshot isolation)
    const r = rows(s2.execute('SELECT * FROM children'));
    assert.equal(r.length, 2, 'Concurrent tx should still see children via snapshot');

    s1.execute('COMMIT');
    s2.execute('COMMIT');

    // After both commit, children should be gone
    const rFinal = rows(db.execute('SELECT * FROM children'));
    assert.equal(rFinal.length, 0);
  });

  it('CASCADE delete + rollback restores children', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO children VALUES (1, 1)');
    db.execute('INSERT INTO children VALUES (2, 1)');

    const s = db.session();
    s.begin();
    s.execute('DELETE FROM parents WHERE id = 1');

    assert.equal(rows(s.execute('SELECT * FROM children')).length, 0);
    s.execute('ROLLBACK');

    // After rollback, both children should be back
    const r = rows(db.execute('SELECT * FROM children'));
    assert.equal(r.length, 2);
  });

  it('multi-level CASCADE is MVCC-visible correctly', () => {
    db.execute('CREATE TABLE gp (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY, gp_id INT REFERENCES gp(id) ON DELETE CASCADE)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute('INSERT INTO gp VALUES (1)');
    db.execute('INSERT INTO parents VALUES (10, 1)');
    db.execute('INSERT INTO parents VALUES (20, 1)');
    db.execute('INSERT INTO children VALUES (100, 10)');
    db.execute('INSERT INTO children VALUES (200, 20)');

    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();

    // s1 deletes grandparent — cascades through parents to children
    s1.execute('DELETE FROM gp WHERE id = 1');

    // s2 should still see everything
    assert.equal(rows(s2.execute('SELECT * FROM gp')).length, 1);
    assert.equal(rows(s2.execute('SELECT * FROM parents')).length, 2);
    assert.equal(rows(s2.execute('SELECT * FROM children')).length, 2);

    s1.execute('COMMIT');
    s2.execute('COMMIT');

    // After both commit, all should be gone
    assert.equal(rows(db.execute('SELECT * FROM gp')).length, 0);
    assert.equal(rows(db.execute('SELECT * FROM parents')).length, 0);
    assert.equal(rows(db.execute('SELECT * FROM children')).length, 0);
  });

  it('SET NULL cascade is MVCC-aware', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON DELETE SET NULL)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO children VALUES (1, 1)');

    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();

    s1.execute('DELETE FROM parents WHERE id = 1');

    // s2 should still see child with p_id = 1
    const r = rows(s2.execute('SELECT * FROM children'));
    assert.equal(r.length, 1);
    assert.equal(r[0].p_id, 1);

    s1.execute('COMMIT');
    s2.execute('COMMIT');

    // After commit, child should have p_id = null
    const rFinal = rows(db.execute('SELECT * FROM children'));
    assert.equal(rFinal.length, 1);
    assert.equal(rFinal[0].p_id, null);
  });

  it('UPDATE CASCADE on PK is MVCC-aware', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON UPDATE CASCADE)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO children VALUES (1, 1)');

    const s1 = db.session();
    const s2 = db.session();
    s1.begin();
    s2.begin();

    s1.execute('UPDATE parents SET id = 10 WHERE id = 1');

    // s2 should still see child with p_id = 1
    const r = rows(s2.execute('SELECT * FROM children'));
    assert.equal(r.length, 1);
    assert.equal(r[0].p_id, 1);

    s1.execute('COMMIT');
    s2.execute('COMMIT');

    // After both commit, child should have p_id = 10
    const rFinal = rows(db.execute('SELECT * FROM children'));
    assert.equal(rFinal.length, 1);
    assert.equal(rFinal[0].p_id, 10);
  });

  it('WAL recovery preserves CASCADE delete effects', () => {
    db.execute('CREATE TABLE parents (id INT PRIMARY KEY)');
    db.execute('CREATE TABLE children (id INT, p_id INT REFERENCES parents(id) ON DELETE CASCADE)');
    db.execute('INSERT INTO parents VALUES (1)');
    db.execute('INSERT INTO children VALUES (1, 1)');
    db.execute('INSERT INTO children VALUES (2, 1)');

    db.execute('DELETE FROM parents WHERE id = 1');
    assert.equal(rows(db.execute('SELECT * FROM children')).length, 0);

    // Crash and recover
    db.close();
    const db2 = TransactionalDatabase.open(dbDir);
    assert.equal(rows(db2.execute('SELECT * FROM children')).length, 0);
    assert.equal(rows(db2.execute('SELECT * FROM parents')).length, 0);
    db2.close();
  });
});
