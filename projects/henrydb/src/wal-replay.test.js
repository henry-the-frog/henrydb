// wal-replay.test.js — Tests for WAL replay engine
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { WALManager } from './wal.js';
import { WALReplayEngine } from './wal-replay.js';
import { Database } from './db.js';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-wal-replay-'));
}

function cleanup(dir) {
  fs.rmSync(dir, { recursive: true, force: true });
}

describe('WAL Replay Engine', () => {
  let dir;

  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('replays CREATE TABLE + INSERT from WAL', () => {
    // Phase 1: Write WAL records
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();
    wal.logBegin(1);
    wal.logCreateTable('users', ['id', 'name', 'age']);
    wal.logInsert('users', { id: 1, name: 'Alice', age: 30 }, 1);
    wal.logInsert('users', { id: 2, name: 'Bob', age: 25 }, 1);
    wal.logCommit(1);
    wal.close();

    // Phase 2: Replay into a fresh database
    const db = new Database();
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    const engine = new WALReplayEngine(db);
    const stats = engine.replay(wal2.recover());

    // Verify
    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[1].name, 'Bob');

    assert.strictEqual(stats.tablesCreated, 1);
    assert.strictEqual(stats.rowsInserted, 2);
    assert.strictEqual(stats.transactionsReplayed, 1);

    wal2.close();
  });

  it('skips uncommitted transactions', () => {
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();

    // Committed transaction
    wal.logBegin(1);
    wal.logCreateTable('items', ['id', 'name']);
    wal.logInsert('items', { id: 1, name: 'committed' }, 1);
    wal.logCommit(1);

    // Uncommitted transaction (crash before commit)
    wal.logBegin(2);
    wal.logInsert('items', { id: 2, name: 'uncommitted' }, 2);
    // No commit!

    wal.close();

    // Replay
    const db = new Database();
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    const engine = new WALReplayEngine(db);
    const stats = engine.replay(wal2.recover());

    const result = db.execute('SELECT * FROM items');
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].name, 'committed');

    assert.strictEqual(stats.transactionsReplayed, 1);
    assert.strictEqual(stats.transactionsRolledBack, 1); // tx 2 implicitly rolled back
    assert.strictEqual(stats.recordsSkipped, 2); // BEGIN(2) + INSERT for tx 2

    wal2.close();
  });

  it('replays UPDATE operations', () => {
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();
    wal.logBegin(1);
    wal.logCreateTable('accounts', ['id', 'balance']);
    wal.logInsert('accounts', { id: 1, balance: 1000 }, 1);
    wal.logInsert('accounts', { id: 2, balance: 500 }, 1);
    wal.logCommit(1);

    wal.logBegin(2);
    wal.logUpdate('accounts', { id: 1, balance: 1000 }, { id: 1, balance: 800 }, 2);
    wal.logUpdate('accounts', { id: 2, balance: 500 }, { id: 2, balance: 700 }, 2);
    wal.logCommit(2);
    wal.close();

    const db = new Database();
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    new WALReplayEngine(db).replay(wal2.recover());

    const result = db.execute('SELECT * FROM accounts ORDER BY id');
    assert.strictEqual(result.rows[0].balance, 800);
    assert.strictEqual(result.rows[1].balance, 700);

    wal2.close();
  });

  it('replays DELETE operations', () => {
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();
    wal.logBegin(1);
    wal.logCreateTable('temp', ['id', 'val']);
    wal.logInsert('temp', { id: 1, val: 'keep' }, 1);
    wal.logInsert('temp', { id: 2, val: 'delete_me' }, 1);
    wal.logInsert('temp', { id: 3, val: 'keep_too' }, 1);
    wal.logCommit(1);

    wal.logBegin(2);
    wal.logDelete('temp', { id: 2, val: 'delete_me' }, 2);
    wal.logCommit(2);
    wal.close();

    const db = new Database();
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    new WALReplayEngine(db).replay(wal2.recover());

    const result = db.execute('SELECT * FROM temp ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].id, 1);
    assert.strictEqual(result.rows[1].id, 3);

    wal2.close();
  });

  it('replays from checkpoint (only recent records)', () => {
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();

    // Pre-checkpoint data
    wal.logBegin(1);
    wal.logCreateTable('data', ['id', 'val']);
    wal.logInsert('data', { id: 1, val: 'old' }, 1);
    wal.logCommit(1);

    // Checkpoint — data up to here is "safe" in the data files
    wal.checkpoint({ tables: ['data'] });

    // Post-checkpoint data (needs replay)
    wal.logBegin(2);
    wal.logInsert('data', { id: 2, val: 'new' }, 2);
    wal.logCommit(2);
    wal.close();

    // Simulate recovery: pre-checkpoint data is already in DB
    const db = new Database();
    db.execute('CREATE TABLE data (id INTEGER, val TEXT)');
    db.execute("INSERT INTO data VALUES (1, 'old')");

    // Replay only post-checkpoint records
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    new WALReplayEngine(db).replay(wal2.recover());

    const result = db.execute('SELECT * FROM data ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[1].val, 'new');

    wal2.close();
  });

  it('handles explicit ROLLBACK transactions', () => {
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();
    wal.logBegin(1);
    wal.logCreateTable('rb_test', ['id']);
    wal.logCommit(1);

    wal.logBegin(2);
    wal.logInsert('rb_test', { id: 1 }, 2);
    wal.logRollback(2); // Explicitly rolled back

    wal.logBegin(3);
    wal.logInsert('rb_test', { id: 2 }, 3);
    wal.logCommit(3);
    wal.close();

    const db = new Database();
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    const stats = new WALReplayEngine(db).replay(wal2.recover());

    const result = db.execute('SELECT * FROM rb_test');
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].id, 2);

    assert.strictEqual(stats.transactionsReplayed, 2); // tx 1 (CREATE TABLE) + tx 3
    assert.strictEqual(stats.transactionsRolledBack, 1); // tx 2

    wal2.close();
  });

  it('handles interleaved transactions', () => {
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();
    wal.logBegin(1);
    wal.logCreateTable('interleaved', ['id', 'tx']);
    wal.logCommit(1);

    // Interleaved transactions
    wal.logBegin(2);
    wal.logInsert('interleaved', { id: 1, tx: 2 }, 2);
    wal.logBegin(3);
    wal.logInsert('interleaved', { id: 2, tx: 3 }, 3);
    wal.logInsert('interleaved', { id: 3, tx: 2 }, 2);
    wal.logCommit(2); // tx 2 commits
    wal.logInsert('interleaved', { id: 4, tx: 3 }, 3);
    // tx 3 never commits — crash

    wal.close();

    const db = new Database();
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    new WALReplayEngine(db).replay(wal2.recover());

    const result = db.execute('SELECT * FROM interleaved ORDER BY id');
    assert.strictEqual(result.rows.length, 2); // Only tx 2's inserts
    assert.strictEqual(result.rows[0].id, 1);
    assert.strictEqual(result.rows[1].id, 3);

    wal2.close();
  });

  it('full end-to-end crash recovery simulation', () => {
    // === Normal operation: write data through WAL ===
    const wal1 = new WALManager(dir, { syncMode: 'immediate', autoCheckpoint: false });
    wal1.open();

    // Create schema
    wal1.logBegin(1);
    wal1.logCreateTable('employees', ['id', 'name', 'salary']);
    wal1.logCommit(1);

    // Insert employees
    wal1.logBegin(2);
    wal1.logInsert('employees', { id: 1, name: 'Alice', salary: 100000 }, 2);
    wal1.logInsert('employees', { id: 2, name: 'Bob', salary: 85000 }, 2);
    wal1.logInsert('employees', { id: 3, name: 'Charlie', salary: 95000 }, 2);
    wal1.logCommit(2);

    // Checkpoint
    wal1.checkpoint({ employeeCount: 3 });

    // Give Alice a raise (committed)
    wal1.logBegin(3);
    wal1.logUpdate('employees', { id: 1, salary: 100000 }, { id: 1, salary: 110000 }, 3);
    wal1.logCommit(3);

    // Hire someone (in progress — crash before commit)
    wal1.logBegin(4);
    wal1.logInsert('employees', { id: 4, name: 'Diana', salary: 90000 }, 4);

    // CRASH!
    wal1.close();

    // === Recovery ===
    // Start with checkpoint state (3 employees, Alice at 100K)
    const db = new Database();
    db.execute('CREATE TABLE employees (id INTEGER, name TEXT, salary INTEGER)');
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 100000)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 85000)");
    db.execute("INSERT INTO employees VALUES (3, 'Charlie', 95000)");

    // Replay WAL from checkpoint
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    const stats = new WALReplayEngine(db).replay(wal2.recover());
    wal2.close();

    // Verify
    const result = db.execute('SELECT * FROM employees ORDER BY id');
    assert.strictEqual(result.rows.length, 3); // Diana should NOT be here (uncommitted)
    assert.strictEqual(result.rows[0].salary, 110000); // Alice's raise should be applied
    assert.strictEqual(result.rows[1].salary, 85000);  // Bob unchanged
    assert.strictEqual(result.rows[2].salary, 95000);  // Charlie unchanged

    assert.strictEqual(stats.transactionsReplayed, 1); // Only tx 3 (raise)
    assert.strictEqual(stats.transactionsRolledBack, 1); // tx 4 (Diana hire)
  });

  it('handles errors gracefully during replay', () => {
    const wal = new WALManager(dir, { syncMode: 'none', autoCheckpoint: false });
    wal.open();
    wal.logBegin(1);
    wal.logCreateTable('safe', ['id']);
    wal.logInsert('safe', { id: 1 }, 1);
    // This will cause an error during replay (table doesn't exist)
    wal.writeRecord('INSERT', { table: 'nonexistent', row: { id: 1 }, txId: 1 });
    wal.logInsert('safe', { id: 2 }, 1);
    wal.logCommit(1);
    wal.close();

    const db = new Database();
    const wal2 = new WALManager(dir, { syncMode: 'none' });
    wal2.open();
    const errors = [];
    const engine = new WALReplayEngine(db);
    engine.onError((record, err) => errors.push({ record, err }));
    engine.replay(wal2.recover());

    // Should have recovered what it could
    const result = db.execute('SELECT * FROM safe ORDER BY id');
    assert.ok(result.rows.length > 0, 'Should have some data');
    assert.ok(errors.length > 0, 'Should have caught errors');

    wal2.close();
  });
});
