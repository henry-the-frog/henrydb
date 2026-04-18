# DDL Lifecycle Test Harness Design

created: 2026-04-17
tags: henrydb, testing, architecture

## Problem
12 of 14 bugs found on 2026-04-17 would be caught by testing DDL operations through their full lifecycle. Currently, DDL tests only cover Phase 1 (in-memory execution). The persistence, crash recovery, and concurrency phases are untested for most DDL types.

## Design

### Test Generator
```javascript
function ddlLifecycleTests(spec) {
  // spec = { name, setup, ddl, verify, dmlAfterDDL, verifyWithDML }
  
  // Phase 1: In-memory execution
  it(`${spec.name}: in-memory execution`, () => {
    setup(); spec.setup(db); spec.ddl(db); spec.verify(db); spec.dmlAfterDDL(db); spec.verifyWithDML(db);
  });
  
  // Phase 2: Clean close → reopen
  it(`${spec.name}: survives clean close/reopen`, () => {
    spec.setup(db); spec.ddl(db); spec.dmlAfterDDL(db);
    db.close(); db = TransactionalDatabase.open(dbDir);
    spec.verifyWithDML(db);
  });
  
  // Phase 3: Crash (no close) → reopen
  it(`${spec.name}: survives crash`, () => {
    spec.setup(db); spec.ddl(db); spec.dmlAfterDDL(db);
    simulateCrash(db); db = TransactionalDatabase.open(dbDir);
    spec.verifyWithDML(db);
  });
  
  // Phase 4: Crash with stale catalog → reopen
  it(`${spec.name}: survives crash with stale catalog`, () => {
    spec.setup(db);
    const staleCatalog = saveCatalog();
    spec.ddl(db); spec.dmlAfterDDL(db);
    simulateCrashWithStaleCatalog(db, staleCatalog);
    db = TransactionalDatabase.open(dbDir);
    spec.verifyWithDML(db);
  });
  
  // Phase 5: DDL → checkpoint → more work → crash
  it(`${spec.name}: survives checkpoint + crash`, () => {
    spec.setup(db); spec.ddl(db);
    db.checkpoint();
    spec.dmlAfterDDL(db);
    simulateCrash(db); db = TransactionalDatabase.open(dbDir);
    spec.verify(db); // At least schema should survive
  });
  
  // Phase 6: DDL during open transaction
  it(`${spec.name}: doesn't break open transaction`, () => {
    spec.setup(db);
    const s = db.session(); s.begin();
    s.execute('SELECT * FROM [table]'); // Read snapshot
    spec.ddl(db); // DDL outside tx
    s.execute('SELECT * FROM [table]'); // Should still work
    s.commit();
  });
  
  // Phase 7: Concurrent DDL + DML
  it(`${spec.name}: concurrent with DML`, () => {
    spec.setup(db);
    const s = db.session(); s.begin();
    [write some rows in s]
    spec.ddl(db);
    s.commit();
    spec.verify(db);
  });
}
```

### DDL Specs
```javascript
const specs = [
  {
    name: 'ALTER TABLE ADD COLUMN',
    setup: (db) => {
      db.execute('CREATE TABLE t (id INT, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
    },
    ddl: (db) => db.execute('ALTER TABLE t ADD COLUMN score INT'),
    verify: (db) => {
      const r = db.execute('SELECT * FROM t');
      assert('score' in r.rows[0]);
    },
    dmlAfterDDL: (db) => db.execute("INSERT INTO t VALUES (2, 'Bob', 100)"),
    verifyWithDML: (db) => {
      const r = db.execute('SELECT * FROM t ORDER BY id');
      assert.equal(r.rows.length, 2);
      assert.equal(r.rows[1].score, 100);
    },
  },
  // ... similar specs for each DDL operation
];
```

### Coverage: 9 DDL types × 7 phases = 63 tests from ~200 lines of specs

### Priority
This should be the FIRST build task of the next depth session. It's high-ROI infrastructure that catches entire bug categories.
