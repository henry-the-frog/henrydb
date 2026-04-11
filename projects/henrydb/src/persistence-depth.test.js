// persistence-depth.test.js — Deep persistence tests for HenryDB
// Tests the gaps identified in failures.md:
// - BufferPool eviction under pressure with real file I/O
// - WAL crash recovery without clean shutdown
// - Multi-cycle close/reopen stress
// - Large dataset exceeding buffer pool capacity

import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentDatabase } from './persistent-db.js';
import { DiskManager, PAGE_SIZE } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { FileWAL, recoverFromFileWAL } from './file-wal.js';
import { rmSync, existsSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testDir = () => join(tmpdir(), `henrydb-depth-${Date.now()}-${Math.random().toString(36).slice(2)}`);

// ============================================================
// Suite 1: BufferPool Eviction Stress with Real File I/O
// ============================================================
describe('BufferPool Eviction Stress (file-backed)', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('tiny pool (4 pages) evicts dirty pages to disk correctly', () => {
    // With poolSize=4, inserting enough rows to span 5+ pages forces eviction
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 4 });
    db.execute('CREATE TABLE stress (id INT PRIMARY KEY, val TEXT)');
    
    // Insert enough rows to fill many pages (each row ~20-40 bytes, page is 4096)
    const rowCount = 200;
    for (let i = 0; i < rowCount; i++) {
      db.execute(`INSERT INTO stress VALUES (${i}, 'value_${i}_${'x'.repeat(50)}')`);
    }
    
    // Verify all data readable before close
    const before = db.execute('SELECT COUNT(*) as cnt FROM stress');
    assert.strictEqual(before.rows[0].cnt, rowCount);
    
    db.close();
    
    // Reopen with same tiny pool and verify
    const db2 = PersistentDatabase.open(d, { poolSize: 4 });
    const after = db2.execute('SELECT COUNT(*) as cnt FROM stress');
    assert.strictEqual(after.rows[0].cnt, rowCount);
    
    // Spot-check specific rows
    const first = db2.execute('SELECT * FROM stress WHERE id = 0');
    assert.strictEqual(first.rows.length, 1);
    assert.ok(first.rows[0].val.startsWith('value_0_'));
    
    const last = db2.execute(`SELECT * FROM stress WHERE id = ${rowCount - 1}`);
    assert.strictEqual(last.rows.length, 1);
    assert.ok(last.rows[0].val.startsWith(`value_${rowCount - 1}_`));
    
    // Verify every single row
    const all = db2.execute('SELECT * FROM stress ORDER BY id');
    assert.strictEqual(all.rows.length, rowCount);
    for (let i = 0; i < rowCount; i++) {
      assert.strictEqual(all.rows[i].id, i, `Row ${i} id mismatch`);
      assert.ok(all.rows[i].val.startsWith(`value_${i}_`), `Row ${i} val mismatch`);
    }
    
    db2.close();
  });

  it('eviction with updates: dirty pages written back correctly', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 4 });
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    
    // Create accounts
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO accounts VALUES (${i}, 1000)`);
    }
    
    // Update half of them — this dirties pages that may have been evicted
    for (let i = 0; i < 50; i += 2) {
      db.execute(`UPDATE accounts SET balance = 2000 WHERE id = ${i}`);
    }
    
    db.close();
    
    // Reopen and verify updates persisted
    const db2 = PersistentDatabase.open(d, { poolSize: 4 });
    for (let i = 0; i < 50; i++) {
      const r = db2.execute(`SELECT balance FROM accounts WHERE id = ${i}`);
      const expected = i % 2 === 0 ? 2000 : 1000;
      assert.strictEqual(r.rows[0].balance, expected, `Account ${i} balance wrong`);
    }
    db2.close();
  });

  it('eviction with deletes: deleted rows stay deleted after reopen', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 4 });
    db.execute('CREATE TABLE logs (id INT PRIMARY KEY, msg TEXT)');
    
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO logs VALUES (${i}, 'log_entry_${i}')`);
    }
    
    // Delete odd-numbered rows
    for (let i = 1; i < 100; i += 2) {
      db.execute(`DELETE FROM logs WHERE id = ${i}`);
    }
    
    const beforeClose = db.execute('SELECT COUNT(*) as cnt FROM logs');
    assert.strictEqual(beforeClose.rows[0].cnt, 50);
    
    db.close();
    
    const db2 = PersistentDatabase.open(d, { poolSize: 4 });
    const afterReopen = db2.execute('SELECT COUNT(*) as cnt FROM logs');
    assert.strictEqual(afterReopen.rows[0].cnt, 50);
    
    // Verify only even IDs remain
    const rows = db2.execute('SELECT id FROM logs ORDER BY id');
    for (let i = 0; i < 50; i++) {
      assert.strictEqual(rows.rows[i].id, i * 2, `Expected id ${i * 2}, got ${rows.rows[i].id}`);
    }
    
    db2.close();
  });

  it('direct FileBackedHeap: tiny buffer pool forces eviction cascades', () => {
    // Test the low-level heap directly, bypassing PersistentDatabase
    const d = testDir();
    dirs.push(d);
    if (!existsSync(d)) mkdirSync(d, { recursive: true });
    
    const dbPath = join(d, 'heap_test.db');
    const walPath = join(d, 'heap_test.wal');
    const dm = new DiskManager(dbPath);
    const bp = new BufferPool(2); // Only 2 frames!
    const wal = new FileWAL(walPath);
    const heap = new FileBackedHeap('test_table', dm, bp, wal);
    
    // Insert rows — with only 2 buffer frames, pages MUST be evicted to disk
    const inserted = [];
    const txId = wal.allocateTxId();
    wal.beginTransaction(txId);
    heap._currentTxId = txId;
    for (let i = 0; i < 100; i++) {
      const loc = heap.insert([i, `row_${i}`]);
      inserted.push({ ...loc, id: i });
    }
    wal.appendCommit(txId);
    
    // Verify all rows readable even though they've been evicted from the pool
    for (const { pageId, slotIdx, id } of inserted) {
      const values = heap.get(pageId, slotIdx);
      assert.ok(values, `Row ${id} not found at page ${pageId} slot ${slotIdx}`);
      assert.strictEqual(values[0], id);
      assert.strictEqual(values[1], `row_${id}`);
    }
    
    // Flush and close
    heap.flush();
    wal.close();
    dm.close();
    
    // Reopen and verify
    const dm2 = new DiskManager(dbPath);
    const bp2 = new BufferPool(2);
    const wal2 = new FileWAL(walPath);
    const heap2 = new FileBackedHeap('test_table', dm2, bp2, wal2);
    
    // Recovery
    recoverFromFileWAL(heap2, wal2);
    
    let count = 0;
    for (const { values } of heap2.scan()) {
      assert.ok(values[0] >= 0 && values[0] < 100, `Unexpected id: ${values[0]}`);
      count++;
    }
    assert.strictEqual(count, 100, `Expected 100 rows after reopen, got ${count}`);
    
    wal2.close();
    dm2.close();
  });
});

// ============================================================
// Suite 2: WAL Crash Recovery (no clean shutdown)
// ============================================================
describe('WAL Crash Recovery Depth', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('simulated crash: data recoverable without close()', () => {
    const d = testDir();
    dirs.push(d);
    
    // Open, insert data, flush WAL but DON'T call close()
    const db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE crash_test (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO crash_test VALUES (1, 'survived')");
    db.execute("INSERT INTO crash_test VALUES (2, 'also_survived')");
    
    // Force WAL flush (simulating WAL durability without clean shutdown)
    db._wal.flush();
    // Save catalog so reopen knows about the table
    db._saveCatalog();
    // Don't call db.close() — simulating a crash
    
    // Reopen — recovery should replay WAL
    const db2 = PersistentDatabase.open(d);
    const r = db2.execute('SELECT * FROM crash_test ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].val, 'survived');
    assert.strictEqual(r.rows[1].val, 'also_survived');
    db2.close();
  });

  it('crash with mixed committed and uncommitted: only committed survives', () => {
    const d = testDir();
    dirs.push(d);
    
    // The PersistentDatabase wraps each DML in its own WAL transaction,
    // so every INSERT is auto-committed. Test that this works correctly.
    const db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE mixed (id INT PRIMARY KEY, status TEXT)');
    db.execute("INSERT INTO mixed VALUES (1, 'committed')");
    db.execute("INSERT INTO mixed VALUES (2, 'committed')");
    
    // These are committed (each INSERT gets its own WAL txn)
    db._wal.flush();
    db._saveCatalog();
    
    // Now write a WAL record without committing — simulate in-flight txn
    const txId = db._wal.allocateTxId();
    db._wal.beginTransaction(txId);
    db._wal.appendInsert(txId, 'mixed', 0, 99, [3, 'uncommitted']);
    // No commit for this txn!
    db._wal.flush();
    
    // Reopen — should only see the 2 committed rows
    const db2 = PersistentDatabase.open(d);
    const r = db2.execute('SELECT * FROM mixed ORDER BY id');
    assert.strictEqual(r.rows.length, 2, `Expected 2 committed rows, got ${r.rows.length}`);
    assert.strictEqual(r.rows[0].status, 'committed');
    assert.strictEqual(r.rows[1].status, 'committed');
    db2.close();
  });

  it('WAL recovery is idempotent: multiple reopens produce same result', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE idem (id INT PRIMARY KEY, val INT)');
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO idem VALUES (${i}, ${i * 100})`);
    }
    db.close();
    
    // Reopen 5 times — each should produce the same result
    for (let cycle = 0; cycle < 5; cycle++) {
      const db2 = PersistentDatabase.open(d);
      const r = db2.execute('SELECT COUNT(*) as cnt, SUM(val) as total FROM idem');
      assert.strictEqual(r.rows[0].cnt, 20, `Cycle ${cycle}: count mismatch`);
      assert.strictEqual(r.rows[0].total, 19000, `Cycle ${cycle}: sum mismatch`);
      db2.close();
    }
  });
});

// ============================================================
// Suite 3: Multi-Cycle Close/Reopen Stress
// ============================================================
describe('Multi-Cycle Persistence Stress', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('10 close/reopen cycles with interleaved operations', () => {
    const d = testDir();
    dirs.push(d);
    
    // Cycle 0: create table
    let db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE cycles (id INT PRIMARY KEY, cycle_inserted INT, status TEXT)');
    db.close();
    
    let expectedCount = 0;
    const activeIds = new Set();
    
    for (let cycle = 1; cycle <= 10; cycle++) {
      db = PersistentDatabase.open(d);
      
      // Insert new rows
      const base = cycle * 100;
      for (let i = 0; i < 10; i++) {
        const id = base + i;
        db.execute(`INSERT INTO cycles VALUES (${id}, ${cycle}, 'active')`);
        activeIds.add(id);
        expectedCount++;
      }
      
      // Update some rows from previous cycles
      if (cycle > 1) {
        const prevBase = (cycle - 1) * 100;
        db.execute(`UPDATE cycles SET status = 'updated_by_cycle_${cycle}' WHERE id = ${prevBase}`);
      }
      
      // Delete one row from 2 cycles ago
      if (cycle > 2) {
        const oldBase = (cycle - 2) * 100 + 5;
        if (activeIds.has(oldBase)) {
          db.execute(`DELETE FROM cycles WHERE id = ${oldBase}`);
          activeIds.delete(oldBase);
          expectedCount--;
        }
      }
      
      // Verify count
      const r = db.execute('SELECT COUNT(*) as cnt FROM cycles');
      assert.strictEqual(r.rows[0].cnt, expectedCount, `Cycle ${cycle}: expected ${expectedCount} rows, got ${r.rows[0].cnt}`);
      
      db.close();
    }
    
    // Final verification
    db = PersistentDatabase.open(d);
    const final = db.execute('SELECT COUNT(*) as cnt FROM cycles');
    assert.strictEqual(final.rows[0].cnt, expectedCount);
    
    // Verify specific updates
    const updated = db.execute("SELECT * FROM cycles WHERE id = 200");
    assert.strictEqual(updated.rows.length, 1);
    assert.strictEqual(updated.rows[0].status, 'updated_by_cycle_3');
    
    db.close();
  });

  it('rapid close/reopen with single-row operations', () => {
    const d = testDir();
    dirs.push(d);
    
    let db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE counter (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO counter VALUES (1, 0)');
    db.close();
    
    // 20 rapid cycles, each incrementing the counter
    for (let i = 1; i <= 20; i++) {
      db = PersistentDatabase.open(d);
      db.execute(`UPDATE counter SET val = ${i} WHERE id = 1`);
      db.close();
    }
    
    // Final check
    db = PersistentDatabase.open(d);
    const r = db.execute('SELECT val FROM counter WHERE id = 1');
    assert.strictEqual(r.rows[0].val, 20, 'Counter should be 20 after 20 updates');
    db.close();
  });
});

// ============================================================
// Suite 4: Large Dataset Exceeding Buffer Pool
// ============================================================
describe('Large Dataset Persistence', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('500 rows with small buffer pool: full scan correct after reopen', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 8 });
    db.execute('CREATE TABLE big_table (id INT PRIMARY KEY, name TEXT, score INT, data TEXT)');
    
    const N = 500;
    for (let i = 0; i < N; i++) {
      db.execute(`INSERT INTO big_table VALUES (${i}, 'user_${i}', ${i % 100}, '${'data_'.repeat(5)}${i}')`);
    }
    
    db.close();
    
    const db2 = PersistentDatabase.open(d, { poolSize: 8 });
    
    // Full count
    const count = db2.execute('SELECT COUNT(*) as cnt FROM big_table');
    assert.strictEqual(count.rows[0].cnt, N);
    
    // Aggregation
    const agg = db2.execute('SELECT SUM(score) as total FROM big_table');
    let expectedSum = 0;
    for (let i = 0; i < N; i++) expectedSum += i % 100;
    assert.strictEqual(agg.rows[0].total, expectedSum);
    
    // Spot checks
    const first = db2.execute('SELECT * FROM big_table WHERE id = 0');
    assert.strictEqual(first.rows[0].name, 'user_0');
    
    const mid = db2.execute('SELECT * FROM big_table WHERE id = 250');
    assert.strictEqual(mid.rows[0].name, 'user_250');
    
    const last = db2.execute(`SELECT * FROM big_table WHERE id = ${N - 1}`);
    assert.strictEqual(last.rows[0].name, `user_${N - 1}`);
    
    db2.close();
  });

  it('1000 rows: insert, close, reopen, insert more, verify all', () => {
    const d = testDir();
    dirs.push(d);
    
    // Phase 1: Insert first 500
    let db = PersistentDatabase.open(d, { poolSize: 8 });
    db.execute('CREATE TABLE growing (id INT PRIMARY KEY, batch INT)');
    for (let i = 0; i < 500; i++) {
      db.execute(`INSERT INTO growing VALUES (${i}, 1)`);
    }
    db.close();
    
    // Phase 2: Insert next 500
    db = PersistentDatabase.open(d, { poolSize: 8 });
    for (let i = 500; i < 1000; i++) {
      db.execute(`INSERT INTO growing VALUES (${i}, 2)`);
    }
    db.close();
    
    // Phase 3: Verify all 1000
    db = PersistentDatabase.open(d, { poolSize: 8 });
    const total = db.execute('SELECT COUNT(*) as cnt FROM growing');
    assert.strictEqual(total.rows[0].cnt, 1000);
    
    const batch1 = db.execute('SELECT COUNT(*) as cnt FROM growing WHERE batch = 1');
    assert.strictEqual(batch1.rows[0].cnt, 500);
    
    const batch2 = db.execute('SELECT COUNT(*) as cnt FROM growing WHERE batch = 2');
    assert.strictEqual(batch2.rows[0].cnt, 500);
    
    db.close();
  });

  it('bank transfer invariant: total balance preserved across close/reopen', () => {
    const d = testDir();
    dirs.push(d);
    
    const numAccounts = 50;
    const initialBalance = 1000;
    const totalExpected = numAccounts * initialBalance;
    
    // Create accounts
    let db = PersistentDatabase.open(d, { poolSize: 8 });
    db.execute('CREATE TABLE bank (id INT PRIMARY KEY, balance INT)');
    for (let i = 0; i < numAccounts; i++) {
      db.execute(`INSERT INTO bank VALUES (${i}, ${initialBalance})`);
    }
    db.close();
    
    // Perform transfers across reopen cycles
    for (let cycle = 0; cycle < 5; cycle++) {
      db = PersistentDatabase.open(d, { poolSize: 8 });
      
      // Random transfers
      for (let t = 0; t < 20; t++) {
        const from = (cycle * 20 + t) % numAccounts;
        const to = (from + 7) % numAccounts;
        const amount = 10;
        
        const fromBal = db.execute(`SELECT balance FROM bank WHERE id = ${from}`);
        if (fromBal.rows[0].balance >= amount) {
          db.execute(`UPDATE bank SET balance = balance - ${amount} WHERE id = ${from}`);
          db.execute(`UPDATE bank SET balance = balance + ${amount} WHERE id = ${to}`);
        }
      }
      
      // Verify invariant
      const sum = db.execute('SELECT SUM(balance) as total FROM bank');
      assert.strictEqual(sum.rows[0].total, totalExpected, `Cycle ${cycle}: total balance should be ${totalExpected}`);
      
      db.close();
    }
    
    // Final verification
    db = PersistentDatabase.open(d, { poolSize: 8 });
    const finalSum = db.execute('SELECT SUM(balance) as total FROM bank');
    assert.strictEqual(finalSum.rows[0].total, totalExpected);
    db.close();
  });
});
