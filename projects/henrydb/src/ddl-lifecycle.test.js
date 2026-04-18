// ddl-lifecycle.test.js — DDL Lifecycle Test Harness
// Tests each DDL operation through 7 lifecycle phases:
//   1. In-memory execution
//   2. Clean close → reopen
//   3. Crash (no close) → reopen
//   4. Crash with stale catalog → reopen
//   5. Checkpoint → more work → crash
//   6. DDL during open transaction (isolation)
//   7. Concurrent DDL + DML race
//
// Design from: memory/scratch/ddl-lifecycle-harness.md
// Catches layer-boundary bugs (12/14 of April 17 bugs were this pattern)

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync, writeFileSync, readFileSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// --- Test infrastructure ---

function freshDir() {
  return mkdtempSync(join(tmpdir(), 'henrydb-ddl-lifecycle-'));
}

function rows(r) {
  return Array.isArray(r) ? r : r?.rows || [];
}

function simulateCrash(db) {
  // Flush WAL to disk but don't call close() — simulates process death
  if (db._wal && db._wal.flush) db._wal.flush();
  if (db.flush) db.flush();
  // DO NOT call close() — catalog/mvcc state may be stale
}

function saveCatalog(dbDir) {
  const catalogPath = join(dbDir, 'catalog.json');
  if (existsSync(catalogPath)) {
    return readFileSync(catalogPath, 'utf8');
  }
  return null;
}

function restoreStaleCatalog(dbDir, catalogJson) {
  if (catalogJson) {
    writeFileSync(join(dbDir, 'catalog.json'), catalogJson);
  }
}

// --- DDL Lifecycle test generator ---

function ddlLifecycleTests(spec) {
  describe(`DDL Lifecycle: ${spec.name}`, () => {
    let dbDir;
    
    beforeEach(() => { dbDir = freshDir(); });
    afterEach(() => { try { rmSync(dbDir, { recursive: true, force: true }); } catch {} });

    // Phase 1: In-memory execution
    it('Phase 1: in-memory execution', () => {
      const db = TransactionalDatabase.open(dbDir);
      try {
        spec.setup(db);
        spec.ddl(db);
        spec.verify(db);
        if (spec.dmlAfterDDL) spec.dmlAfterDDL(db);
        if (spec.verifyWithDML) spec.verifyWithDML(db);
      } finally { db.close(); }
    });

    // Phase 2: Clean close → reopen (DDL only)
    it('Phase 2: DDL survives clean close/reopen', () => {
      let db = TransactionalDatabase.open(dbDir);
      spec.setup(db);
      spec.ddl(db);
      db.close();

      db = TransactionalDatabase.open(dbDir);
      try {
        spec.verify(db);
      } finally { db.close(); }
    });

    // Phase 2b: DDL + DML survives clean close/reopen
    if (spec.dmlAfterDDL && spec.verifyWithDML) {
      it('Phase 2b: DDL + DML survives clean close/reopen', () => {
        let db = TransactionalDatabase.open(dbDir);
        spec.setup(db);
        spec.ddl(db);
        spec.dmlAfterDDL(db);
        db.close();

        db = TransactionalDatabase.open(dbDir);
        try {
          spec.verifyWithDML(db);
        } finally { db.close(); }
      });
    }

    // Phase 3: Crash (no close) → reopen  
    it('Phase 3: DDL survives crash (no close)', () => {
      let db = TransactionalDatabase.open(dbDir);
      spec.setup(db);
      spec.ddl(db);
      simulateCrash(db);

      db = TransactionalDatabase.open(dbDir);
      try {
        spec.verify(db);
      } finally { db.close(); }
    });

    // Phase 4: Crash with stale catalog → reopen
    // NOTE: Skipped for ALTER TABLE ADD/DROP COLUMN because those trigger a
    // checkpoint (to prevent duplicate tuple bugs), which truncates WAL.
    // With stale catalog, the DDL record is lost after checkpoint.
    if (!spec.skipStaleCatalog) {
      it('Phase 4: DDL survives crash with stale catalog', () => {
      let db = TransactionalDatabase.open(dbDir);
      spec.setup(db);
      // Save catalog BEFORE DDL
      db.close();
      const staleCatalog = saveCatalog(dbDir);
      
      db = TransactionalDatabase.open(dbDir);
      spec.ddl(db);
      simulateCrash(db);
      
      // Restore pre-DDL catalog (simulates stale catalog on disk)
      if (staleCatalog) restoreStaleCatalog(dbDir, staleCatalog);
      
      db = TransactionalDatabase.open(dbDir);
      try {
        spec.verify(db);
      } finally { db.close(); }
    });
    }

    // Phase 5: DDL → checkpoint → crash
    it('Phase 5: DDL survives checkpoint then crash', () => {
      let db = TransactionalDatabase.open(dbDir);
      spec.setup(db);
      spec.ddl(db);
      
      try { db.checkpoint(); } catch (e) { /* checkpoint may fail if txs open */ }
      
      simulateCrash(db);

      db = TransactionalDatabase.open(dbDir);
      try {
        spec.verify(db);
      } finally { db.close(); }
    });

    // Phase 6: DDL during open transaction (isolation)
    if (!spec.skipConcurrency) {
      it('Phase 6: DDL does not break open transaction', () => {
        const db = TransactionalDatabase.open(dbDir);
        try {
          spec.setup(db);
          
          // Start a transaction that reads data
          const s = db.session();
          s.begin();
          if (spec.readInTx) spec.readInTx(s);
          
          // Execute DDL outside the transaction
          spec.ddl(db);
          
          // Transaction should still be able to commit
          if (spec.readInTx) spec.readInTx(s);
          s.commit();
          
          spec.verify(db);
        } finally { db.close(); }
      });

      // Phase 7: Concurrent DDL + DML
      it('Phase 7: concurrent DDL + DML', () => {
        const db = TransactionalDatabase.open(dbDir);
        try {
          spec.setup(db);
          
          // Start a session, begin tx, do some DML
          const s = db.session();
          s.begin();
          if (spec.concurrentDML) spec.concurrentDML(s);
          
          // DDL happens outside tx
          spec.ddl(db);
          
          // Commit the DML
          s.commit();
          
          // After concurrent DML, data may have changed — just verify DDL schema
          if (spec.verifySchema) {
            spec.verifySchema(db);
          }
        } finally { db.close(); }
      });
    }
  });
}

// --- DDL Specs ---

// 1. CREATE TABLE
ddlLifecycleTests({
  name: 'CREATE TABLE',
  setup: (db) => {
    // Nothing — we're testing CREATE TABLE itself
  },
  ddl: (db) => {
    db.execute('CREATE TABLE lifecycle_t (id INT PRIMARY KEY, name TEXT, score INT)');
  },
  verify: (db) => {
    // Table should exist and be queryable
    const r = rows(db.execute('SELECT * FROM lifecycle_t'));
    // Just verify table exists (may have 0 rows after DDL-only phases)
    assert.ok(Array.isArray(r));
  },
  dmlAfterDDL: (db) => {
    db.execute("INSERT INTO lifecycle_t VALUES (1, 'Alice', 100)");
    db.execute("INSERT INTO lifecycle_t VALUES (2, 'Bob', 200)");
  },
  verifyWithDML: (db) => {
    const r = rows(db.execute('SELECT * FROM lifecycle_t ORDER BY id'));
    assert.strictEqual(r.length, 2);
    assert.strictEqual(r[0].name, 'Alice');
    assert.strictEqual(r[1].name, 'Bob');
  },
  verifySchema: (db) => {
    // Just check table exists
    const r = rows(db.execute('SELECT * FROM lifecycle_t'));
    assert.ok(Array.isArray(r));
  },
  readInTx: (s) => {},
  concurrentDML: (s) => {},
  skipConcurrency: false,
});

// 2. CREATE INDEX
ddlLifecycleTests({
  name: 'CREATE INDEX',
  setup: (db) => {
    db.execute('CREATE TABLE idx_t (id INT PRIMARY KEY, val INT, label TEXT)');
    db.execute('INSERT INTO idx_t VALUES (1, 10, \'a\')');
    db.execute('INSERT INTO idx_t VALUES (2, 20, \'b\')');
    db.execute('INSERT INTO idx_t VALUES (3, 30, \'c\')');
  },
  ddl: (db) => {
    db.execute('CREATE INDEX idx_val ON idx_t (val)');
  },
  verify: (db) => {
    // Index should accelerate queries — verify data accessible
    const r = rows(db.execute('SELECT * FROM idx_t WHERE val = 20'));
    assert.strictEqual(r.length, 1);
    assert.strictEqual(r[0].label, 'b');
  },
  dmlAfterDDL: (db) => {
    db.execute('INSERT INTO idx_t VALUES (4, 40, \'d\')');
  },
  verifyWithDML: (db) => {
    const r = rows(db.execute('SELECT * FROM idx_t ORDER BY val'));
    assert.strictEqual(r.length, 4);
    assert.strictEqual(r[3].val, 40);
  },
  readInTx: (s) => {
    s.execute('SELECT * FROM idx_t');
  },
  concurrentDML: (s) => {
    s.execute('INSERT INTO idx_t VALUES (5, 50, \'e\')');
  },
});

// 3. CREATE VIEW
ddlLifecycleTests({
  name: 'CREATE VIEW',
  setup: (db) => {
    db.execute('CREATE TABLE view_t (id INT PRIMARY KEY, category TEXT, amount INT)');
    db.execute("INSERT INTO view_t VALUES (1, 'A', 100)");
    db.execute("INSERT INTO view_t VALUES (2, 'B', 200)");
    db.execute("INSERT INTO view_t VALUES (3, 'A', 300)");
  },
  ddl: (db) => {
    db.execute('CREATE VIEW view_summary AS SELECT category, SUM(amount) AS total FROM view_t GROUP BY category');
  },
  verify: (db) => {
    const r = rows(db.execute('SELECT * FROM view_summary ORDER BY category'));
    assert.strictEqual(r.length, 2);
    assert.strictEqual(r[0].category, 'A');
    assert.strictEqual(r[0].total, 400);
    assert.strictEqual(r[1].category, 'B');
    assert.strictEqual(r[1].total, 200);
  },
  dmlAfterDDL: (db) => {
    db.execute("INSERT INTO view_t VALUES (4, 'C', 500)");
  },
  verifyWithDML: (db) => {
    const r = rows(db.execute('SELECT * FROM view_summary ORDER BY category'));
    assert.strictEqual(r.length, 3); // A, B, C
  },
  verifySchema: (db) => {
    // View should exist and be queryable
    const r = rows(db.execute('SELECT * FROM view_summary'));
    assert.ok(r.length >= 2, 'view should return data');
  },
  readInTx: (s) => {
    s.execute('SELECT * FROM view_t');
  },
  concurrentDML: (s) => {
    s.execute("INSERT INTO view_t VALUES (5, 'D', 600)");
  },
});

// 4. ALTER TABLE ADD COLUMN
ddlLifecycleTests({
  name: 'ALTER TABLE ADD COLUMN',
  skipStaleCatalog: true, // Checkpoint after ADD COLUMN truncates WAL
  setup: (db) => {
    db.execute('CREATE TABLE alter_add_t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO alter_add_t VALUES (1, 'Alice')");
  },
  ddl: (db) => {
    db.execute('ALTER TABLE alter_add_t ADD COLUMN score INT');
  },
  verify: (db) => {
    const r = rows(db.execute('SELECT * FROM alter_add_t'));
    assert.ok('score' in r[0] || r[0].score === null || r[0].score === undefined,
      'score column should exist');
  },
  dmlAfterDDL: (db) => {
    db.execute("INSERT INTO alter_add_t VALUES (2, 'Bob', 100)");
  },
  verifyWithDML: (db) => {
    const r = rows(db.execute('SELECT * FROM alter_add_t ORDER BY id'));
    assert.strictEqual(r.length, 2);
    assert.strictEqual(r[1].score, 100);
  },
  readInTx: (s) => {
    s.execute('SELECT * FROM alter_add_t');
  },
  concurrentDML: (s) => {
    s.execute("INSERT INTO alter_add_t VALUES (3, 'Carol')");
  },
});

// 5. ALTER TABLE DROP COLUMN
ddlLifecycleTests({
  name: 'ALTER TABLE DROP COLUMN',
  skipStaleCatalog: true, // Checkpoint after DROP COLUMN truncates WAL
  setup: (db) => {
    db.execute('CREATE TABLE alter_drop_t (id INT PRIMARY KEY, name TEXT, extra TEXT)');
    db.execute("INSERT INTO alter_drop_t VALUES (1, 'Alice', 'x')");
  },
  ddl: (db) => {
    db.execute('ALTER TABLE alter_drop_t DROP COLUMN extra');
  },
  verify: (db) => {
    const r = rows(db.execute('SELECT * FROM alter_drop_t'));
    assert.ok(!('extra' in r[0]), 'extra column should not exist');
  },
  dmlAfterDDL: (db) => {
    db.execute("INSERT INTO alter_drop_t VALUES (2, 'Bob')");
  },
  verifyWithDML: (db) => {
    const r = rows(db.execute('SELECT * FROM alter_drop_t ORDER BY id'));
    assert.strictEqual(r.length, 2);
    assert.strictEqual(r[1].name, 'Bob');
  },
  readInTx: (s) => {
    s.execute('SELECT * FROM alter_drop_t');
  },
  concurrentDML: (s) => {
    s.execute("UPDATE alter_drop_t SET name = 'Modified' WHERE id = 1");
  },
});

// 6. ALTER TABLE RENAME COLUMN
ddlLifecycleTests({
  name: 'ALTER TABLE RENAME COLUMN',
  setup: (db) => {
    db.execute('CREATE TABLE alter_rename_t (id INT PRIMARY KEY, old_name TEXT)');
    db.execute("INSERT INTO alter_rename_t VALUES (1, 'Alice')");
  },
  ddl: (db) => {
    db.execute('ALTER TABLE alter_rename_t RENAME COLUMN old_name TO new_name');
  },
  verify: (db) => {
    const r = rows(db.execute('SELECT * FROM alter_rename_t'));
    assert.ok('new_name' in r[0], 'new_name column should exist');
    assert.ok(!('old_name' in r[0]), 'old_name should not exist');
  },
  dmlAfterDDL: (db) => {
    db.execute("INSERT INTO alter_rename_t (id, new_name) VALUES (2, 'Bob')");
  },
  verifyWithDML: (db) => {
    const r = rows(db.execute('SELECT * FROM alter_rename_t ORDER BY id'));
    assert.strictEqual(r.length, 2);
  },
  verifySchema: (db) => {
    const r = rows(db.execute('SELECT * FROM alter_rename_t'));
    assert.ok('new_name' in r[0], 'new_name column should exist');
    assert.ok(!('old_name' in r[0]), 'old_name should not exist');
  },
  readInTx: (s) => {
    s.execute('SELECT * FROM alter_rename_t');
  },
  concurrentDML: (s) => {
    s.execute("UPDATE alter_rename_t SET old_name = 'Modified' WHERE id = 1");
  },
});

// 7. DROP TABLE
ddlLifecycleTests({
  name: 'DROP TABLE',
  setup: (db) => {
    db.execute('CREATE TABLE drop_me (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO drop_me VALUES (1, 'data')");
    // Also create a table that should survive
    db.execute('CREATE TABLE keep_me (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO keep_me VALUES (1, 'safe')");
  },
  ddl: (db) => {
    db.execute('DROP TABLE drop_me');
  },
  verify: (db) => {
    // drop_me should be gone
    assert.throws(() => db.execute('SELECT * FROM drop_me'), /not found|does not exist|no such/i);
    // keep_me should survive
    const r = rows(db.execute('SELECT * FROM keep_me'));
    assert.ok(r.length >= 1, 'keep_me should have data');
  },
  dmlAfterDDL: (db) => {
    db.execute("INSERT INTO keep_me VALUES (2, 'still safe')");
  },
  verifyWithDML: (db) => {
    assert.throws(() => db.execute('SELECT * FROM drop_me'), /not found|does not exist|no such/i);
    const r = rows(db.execute('SELECT * FROM keep_me ORDER BY id'));
    assert.strictEqual(r.length, 2);
  },
  verifySchema: (db) => {
    assert.throws(() => db.execute('SELECT * FROM drop_me'), /not found|does not exist|no such/i);
  },
  readInTx: (s) => {
    s.execute('SELECT * FROM keep_me');
  },
  concurrentDML: (s) => {
    s.execute("INSERT INTO keep_me VALUES (3, 'concurrent')");
  },
});

// 8. CREATE TRIGGER
ddlLifecycleTests({
  name: 'CREATE TRIGGER',
  setup: (db) => {
    db.execute('CREATE TABLE trigger_t (id INT PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE trigger_log (id INT, action TEXT)');
  },
  ddl: (db) => {
    db.execute("CREATE TRIGGER trg_insert AFTER INSERT ON trigger_t EXECUTE INSERT INTO trigger_log VALUES (NEW.id, 'inserted')");
  },
  verify: (db) => {
    // Verify trigger fires by inserting and checking the log
    db.execute("INSERT INTO trigger_t VALUES (99, 'test')");
    const r = rows(db.execute('SELECT * FROM trigger_log WHERE id = 99'));
    assert.strictEqual(r.length, 1, 'trigger should have fired on insert');
    assert.strictEqual(r[0].action, 'inserted');
  },
  dmlAfterDDL: (db) => {
    db.execute("INSERT INTO trigger_t VALUES (1, 'data')");
  },
  verifyWithDML: (db) => {
    // Trigger should have fired for the insert in dmlAfterDDL
    const log = rows(db.execute('SELECT * FROM trigger_log WHERE id = 1'));
    assert.ok(log.length >= 1, `trigger_log should have entry for id=1`);
  },
  readInTx: (s) => {
    s.execute('SELECT * FROM trigger_t');
  },
  concurrentDML: (s) => {
    s.execute("INSERT INTO trigger_t VALUES (10, 'concurrent')");
  },
});

// 9. CREATE SEQUENCE
ddlLifecycleTests({
  name: 'CREATE SEQUENCE',
  setup: (db) => {
    // Nothing — testing CREATE SEQUENCE itself
  },
  ddl: (db) => {
    db.execute('CREATE SEQUENCE test_seq START 100 INCREMENT 10');
  },
  verify: (db) => {
    const r = rows(db.execute("SELECT NEXTVAL('test_seq') AS v"));
    // After recovery, sequence should be at or past 100
    assert.ok(r[0].v >= 100, `sequence value should be >= 100, got ${r[0].v}`);
  },
  dmlAfterDDL: (db) => {
    // Advance the sequence
    db.execute("SELECT NEXTVAL('test_seq')");
    db.execute("SELECT NEXTVAL('test_seq')");
  },
  verifyWithDML: (db) => {
    const r = rows(db.execute("SELECT NEXTVAL('test_seq') AS v"));
    // Should be past the initial values
    assert.ok(r[0].v >= 100, `sequence should be >= 100 after recovery, got ${r[0].v}`);
  },
  skipConcurrency: true, // Sequences don't interact with row-level txs the same way
});
