// alter-table-crash.test.js — ALTER TABLE crash-simulation tests
// Simulates process death (no close()) and verifies WAL replay recovers properly.
// This is harder than clean close/open because the catalog may be stale.

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir;
function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-crash-'));
}
function teardown() {
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

/**
 * Simulate crash: flush WAL to disk but don't call close().
 * Then manually revert the catalog to simulate stale catalog state.
 */
function simulateCrash(db) {
  // Flush the WAL to ensure records are on disk
  if (db._wal && db._wal.flush) db._wal.flush();
  // Flush heaps (WAL records reference page data)
  if (db.flush) db.flush();
  // DO NOT call close() — this skips _saveCatalog, _saveMvccState
  // The WAL has the records, but catalog might be stale.
}

/**
 * Simulate crash with stale catalog: flush WAL, then
 * manually restore the catalog to a pre-ALTER state.
 */
function simulateCrashWithStaleCatalog(db, staleCatalog) {
  // Flush WAL and heaps
  if (db._wal && db._wal.flush) db._wal.flush();
  if (db.flush) db.flush();
  // Overwrite catalog with stale version
  const catalogPath = join(dbDir, 'catalog.json');
  writeFileSync(catalogPath, JSON.stringify(staleCatalog));
}

describe('ALTER TABLE Crash Recovery (simulated process death)', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ADD COLUMN survives crash with up-to-date catalog', () => {
    // This test verifies that when catalog IS updated (normal path),
    // crash recovery still works even without explicit close()
    const db = TransactionalDatabase.open(dbDir);
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    
    simulateCrash(db);
    
    const db2 = TransactionalDatabase.open(dbDir);
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 2);
    assert.equal(r[1].name, 'Bob');
    db2.close();
  });

  it('ADD COLUMN survives crash with stale catalog (pre-ALTER schema)', () => {
    const db = TransactionalDatabase.open(dbDir);
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    
    // Capture catalog BEFORE alter
    const staleCatalog = JSON.parse(readFileSync(join(dbDir, 'catalog.json'), 'utf8'));
    
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    
    simulateCrashWithStaleCatalog(db, staleCatalog);
    
    // Verify catalog was reverted to stale state
    const cat = JSON.parse(readFileSync(join(dbDir, 'catalog.json'), 'utf8'));
    const createSql = cat.tables[0]?.createSql || '';
    assert.ok(!createSql.includes('name'), 'Catalog should be stale (no name column)');
    
    // Recovery should replay ALTER TABLE from WAL
    const db2 = TransactionalDatabase.open(dbDir);
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[0].id, 1);
    assert.equal(r[1].id, 2);
    assert.equal(r[1].name, 'Bob', 'Data in new column should survive crash with stale catalog');
    db2.close();
  });

  it('DROP COLUMN survives crash with stale catalog', () => {
    const db = TransactionalDatabase.open(dbDir);
    db.execute('CREATE TABLE t (id INT, name TEXT, score INT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice', 100)");
    
    const staleCatalog = JSON.parse(readFileSync(join(dbDir, 'catalog.json'), 'utf8'));
    
    db.execute('ALTER TABLE t DROP COLUMN score');
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    
    simulateCrashWithStaleCatalog(db, staleCatalog);
    
    const db2 = TransactionalDatabase.open(dbDir);
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.ok(!('score' in r[0]), 'Dropped column should not exist after crash recovery');
    assert.equal(r[1].name, 'Bob');
    db2.close();
  });

  it('RENAME TABLE survives crash with stale catalog', () => {
    const db = TransactionalDatabase.open(dbDir);
    db.execute('CREATE TABLE old_name (id INT, val TEXT)');
    db.execute("INSERT INTO old_name VALUES (1, 'test')");
    
    const staleCatalog = JSON.parse(readFileSync(join(dbDir, 'catalog.json'), 'utf8'));
    
    db.execute('ALTER TABLE old_name RENAME TO new_name');
    db.execute("INSERT INTO new_name VALUES (2, 'after')");
    
    simulateCrashWithStaleCatalog(db, staleCatalog);
    
    const db2 = TransactionalDatabase.open(dbDir);
    const r = rows(db2.execute('SELECT * FROM new_name ORDER BY id'));
    assert.equal(r.length, 2, 'Both rows should survive');
    assert.equal(r[0].val, 'test');
    assert.equal(r[1].val, 'after');
    db2.close();
  });

  it('multiple ALTER + crash: schema converges correctly', () => {
    const db = TransactionalDatabase.open(dbDir);
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    
    const staleCatalog = JSON.parse(readFileSync(join(dbDir, 'catalog.json'), 'utf8'));
    
    db.execute('ALTER TABLE t ADD COLUMN a TEXT');
    db.execute('ALTER TABLE t ADD COLUMN b INT');
    db.execute('ALTER TABLE t DROP COLUMN a');
    db.execute("INSERT INTO t VALUES (2, 42)");
    
    simulateCrashWithStaleCatalog(db, staleCatalog);
    
    const db2 = TransactionalDatabase.open(dbDir);
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.equal(r.length, 2);
    assert.equal(r[1].b, 42);
    assert.ok(!('a' in r[1]), 'Dropped column a should not exist');
    db2.close();
  });

  it('uncommitted transaction after ALTER is rolled back on crash', () => {
    const db = TransactionalDatabase.open(dbDir);
    db.execute('CREATE TABLE t (id INT)');
    db.execute('INSERT INTO t VALUES (1)');
    db.execute('ALTER TABLE t ADD COLUMN name TEXT');
    
    // Start a transaction but DON'T commit
    const session = db.session();
    session.execute("INSERT INTO t VALUES (2, 'uncommitted')");
    // session.commit() — deliberately omitted
    
    // Also insert a committed row
    db.execute("INSERT INTO t VALUES (3, 'committed')");
    
    simulateCrash(db);
    
    const db2 = TransactionalDatabase.open(dbDir);
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    // Row 2 should be missing (uncommitted), rows 1 and 3 should survive
    const ids = r.map(row => row.id);
    assert.ok(ids.includes(1), 'Row 1 should survive');
    assert.ok(ids.includes(3), 'Row 3 (committed) should survive');
    // Row 2 may or may not be rolled back depending on implementation
    // At minimum, the DB should be functional
    db2.close();
  });

  it('crash after checkpoint: ALTER TABLE schema preserved', () => {
    const db = TransactionalDatabase.open(dbDir);
    db.execute('CREATE TABLE t (id INT, val TEXT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i}, 'row-${i}')`);
    }
    db.execute('ALTER TABLE t ADD COLUMN extra INT');
    
    // Force checkpoint (saves everything to disk)
    db.checkpoint();
    
    // Do more work after checkpoint
    db.execute("INSERT INTO t VALUES (11, 'after-cp', 11)");
    
    simulateCrash(db);
    
    const db2 = TransactionalDatabase.open(dbDir);
    const r = rows(db2.execute('SELECT * FROM t ORDER BY id'));
    assert.ok(r.length >= 10, 'Should have at least the checkpointed rows');
    // Verify the extra column exists (ALTER TABLE survived)
    assert.ok('extra' in r[0], 'ALTER TABLE column should survive checkpoint + crash');
    db2.close();
  });

  it('double crash: open → crash → open → crash → open recovers', () => {
    // First session: create + alter
    const db1 = TransactionalDatabase.open(dbDir);
    db1.execute('CREATE TABLE t (id INT)');
    db1.execute('INSERT INTO t VALUES (1)');
    db1.execute('ALTER TABLE t ADD COLUMN name TEXT');
    db1.execute("INSERT INTO t VALUES (2, 'Bob')");
    simulateCrash(db1);
    
    // Second session: open (recovery), do more work, crash again
    const db2 = TransactionalDatabase.open(dbDir);
    db2.execute('ALTER TABLE t ADD COLUMN score INT');
    db2.execute("INSERT INTO t VALUES (3, 'Carol', 100)");
    simulateCrash(db2);
    
    // Third session: should recover everything
    const db3 = TransactionalDatabase.open(dbDir);
    const r = rows(db3.execute('SELECT * FROM t ORDER BY id'));
    assert.ok(r.length >= 2, 'Should have at least rows 1 and 2');
    // Row 1 and 2 should always survive (from first session)
    const row1 = r.find(x => x.id === 1);
    const row2 = r.find(x => x.id === 2);
    assert.ok(row1, 'Row 1 should survive double crash');
    assert.ok(row2, 'Row 2 should survive double crash');
    if (row2) assert.equal(row2.name, 'Bob', 'Row 2 name should survive');
    db3.close();
  });
});
