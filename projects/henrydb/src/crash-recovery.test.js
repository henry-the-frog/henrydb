// crash-recovery.test.js — Tests for WAL-based crash recovery
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentDatabase } from './persistent-db.js';
import { FileWAL, recoverFromFileWAL } from './file-wal.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { rmSync, existsSync, truncateSync, readdirSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testDir = () => join(tmpdir(), `henrydb-crash-${Date.now()}-${Math.random().toString(36).slice(2)}`);

describe('Crash Recovery', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('recovers all data when page files are lost (WAL intact)', () => {
    const dir = testDir();
    dirs.push(dir);
    
    // Write data
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Carol', 35)");
    
    // "Crash": save catalog + WAL, but destroy page files
    db._saveCatalog();
    db._wal.flush();
    db._wal.close();
    for (const dm of db._diskManagers.values()) dm.close();
    
    // Truncate all page files (simulates dirty pages lost in crash)
    for (const f of readdirSync(dir)) {
      if (f.endsWith('.db')) truncateSync(join(dir, f), 0);
    }
    
    // Recovery
    const db2 = PersistentDatabase.open(dir);
    const r = db2.execute('SELECT * FROM users ORDER BY id');
    assert.strictEqual(r.rows.length, 3);
    assert.strictEqual(r.rows[0].name, 'Alice');
    assert.strictEqual(r.rows[1].name, 'Bob');
    assert.strictEqual(r.rows[2].name, 'Carol');
    db2.close();
  });

  it('recovers large dataset after crash', () => {
    const dir = testDir();
    dirs.push(dir);
    const N = 200;
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE big (id INT PRIMARY KEY, data TEXT)');
    for (let i = 0; i < N; i++) {
      db.execute(`INSERT INTO big VALUES (${i}, 'row_${i}')`);
    }
    
    db._saveCatalog();
    db._wal.flush();
    db._wal.close();
    for (const dm of db._diskManagers.values()) dm.close();
    for (const f of readdirSync(dir)) {
      if (f.endsWith('.db')) truncateSync(join(dir, f), 0);
    }
    
    const db2 = PersistentDatabase.open(dir);
    const count = db2.execute('SELECT COUNT(*) as cnt FROM big');
    assert.strictEqual(count.rows[0].cnt, N);
    
    // Spot check
    const r = db2.execute('SELECT data FROM big WHERE id = 100');
    assert.strictEqual(r.rows[0].data, 'row_100');
    db2.close();
  });

  it('recovers after UPDATE + crash', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE accounts (id INT PRIMARY KEY, balance INT)');
    db.execute('INSERT INTO accounts VALUES (1, 1000)');
    db.execute('INSERT INTO accounts VALUES (2, 2000)');
    db.execute('UPDATE accounts SET balance = 1500 WHERE id = 1');
    
    db._saveCatalog();
    db._wal.flush();
    db._wal.close();
    for (const dm of db._diskManagers.values()) dm.close();
    for (const f of readdirSync(dir)) {
      if (f.endsWith('.db')) truncateSync(join(dir, f), 0);
    }
    
    const db2 = PersistentDatabase.open(dir);
    const r = db2.execute('SELECT * FROM accounts ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].balance, 1500); // Updated value
    assert.strictEqual(r.rows[1].balance, 2000); // Unchanged
    db2.close();
  });

  it('recovers after DELETE + crash', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'item${i}')`);
    }
    db.execute('DELETE FROM items WHERE id > 7');
    
    db._saveCatalog();
    db._wal.flush();
    db._wal.close();
    for (const dm of db._diskManagers.values()) dm.close();
    for (const f of readdirSync(dir)) {
      if (f.endsWith('.db')) truncateSync(join(dir, f), 0);
    }
    
    const db2 = PersistentDatabase.open(dir);
    const count = db2.execute('SELECT COUNT(*) as cnt FROM items');
    assert.strictEqual(count.rows[0].cnt, 7);
    db2.close();
  });

  it('recovers multi-table data after crash', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE authors (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE books (id INT PRIMARY KEY, title TEXT, author_id INT)');
    db.execute("INSERT INTO authors VALUES (1, 'Tolkien')");
    db.execute("INSERT INTO authors VALUES (2, 'Asimov')");
    db.execute("INSERT INTO books VALUES (1, 'The Hobbit', 1)");
    db.execute("INSERT INTO books VALUES (2, 'Foundation', 2)");
    
    db._saveCatalog();
    db._wal.flush();
    db._wal.close();
    for (const dm of db._diskManagers.values()) dm.close();
    for (const f of readdirSync(dir)) {
      if (f.endsWith('.db')) truncateSync(join(dir, f), 0);
    }
    
    const db2 = PersistentDatabase.open(dir);
    const authors = db2.execute('SELECT COUNT(*) as cnt FROM authors');
    assert.strictEqual(authors.rows[0].cnt, 2);
    const books = db2.execute('SELECT COUNT(*) as cnt FROM books');
    assert.strictEqual(books.rows[0].cnt, 2);
    
    // Join should work after recovery
    const joinResult = db2.execute('SELECT a.name, b.title FROM authors a JOIN books b ON a.id = b.author_id ORDER BY b.title');
    assert.strictEqual(joinResult.rows.length, 2);
    assert.strictEqual(joinResult.rows[0].title, 'Foundation');
    assert.strictEqual(joinResult.rows[1].title, 'The Hobbit');
    db2.close();
  });

  it('indexes are rebuilt after crash recovery', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE indexed (id INT PRIMARY KEY, category TEXT, value INT)');
    db.execute('CREATE INDEX idx_cat ON indexed (category)');
    for (let i = 1; i <= 50; i++) {
      const cat = i % 3 === 0 ? 'A' : i % 3 === 1 ? 'B' : 'C';
      db.execute(`INSERT INTO indexed VALUES (${i}, '${cat}', ${i * 10})`);
    }
    
    db._saveCatalog();
    db._wal.flush();
    db._wal.close();
    for (const dm of db._diskManagers.values()) dm.close();
    for (const f of readdirSync(dir)) {
      if (f.endsWith('.db')) truncateSync(join(dir, f), 0);
    }
    
    const db2 = PersistentDatabase.open(dir);
    // PK lookup should work (index rebuilt)
    const r = db2.execute('SELECT value FROM indexed WHERE id = 25');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].value, 250);
    
    // Total count
    const cnt = db2.execute('SELECT COUNT(*) as c FROM indexed');
    assert.strictEqual(cnt.rows[0].c, 50);
    db2.close();
  });

  it('low-level WAL recovery: only committed txns replayed', () => {
    const dir = testDir();
    dirs.push(dir);
    mkdirSync(dir, { recursive: true });
    
    const walPath = join(dir, 'test.wal');
    const dbPath = join(dir, 'test.db');
    
    // Create WAL with mixed committed and uncommitted txns
    const wal = new FileWAL(walPath);
    const tx1 = wal.allocateTxId(); // Committed
    const tx2 = wal.allocateTxId(); // Uncommitted
    
    wal.appendInsert(tx1, 'test', 0, 0, [1, 'committed']);
    wal.appendInsert(tx2, 'test', 0, 1, [2, 'uncommitted']);
    wal.appendInsert(tx1, 'test', 0, 2, [3, 'also_committed']);
    wal.appendCommit(tx1);
    // tx2 never committed!
    wal.flush();
    
    // Create empty heap and recover
    const dm = new DiskManager(dbPath);
    const bp = new BufferPool(16);
    const heap = new FileBackedHeap('test', dm, bp);
    
    const result = recoverFromFileWAL(heap, wal);
    assert.ok(result.redone >= 2, `Should redo at least 2 records, got ${result.redone}`);
    
    // Only committed data should be present
    const rows = [...heap.scan()].map(r => r.values);
    const committedRows = rows.filter(r => r[1] !== 'uncommitted');
    assert.strictEqual(committedRows.length, 2);
    assert.ok(committedRows.some(r => r[0] === 1 && r[1] === 'committed'));
    assert.ok(committedRows.some(r => r[0] === 3 && r[1] === 'also_committed'));
    
    // Uncommitted row should NOT be present
    const uncommitted = rows.filter(r => r[1] === 'uncommitted');
    assert.strictEqual(uncommitted.length, 0, 'Uncommitted data should not survive');
    
    wal.close();
    dm.close();
  });

  it('partial batch: only complete transactions survive crash', () => {
    const dir = testDir();
    dirs.push(dir);
    mkdirSync(dir, { recursive: true });
    
    const walPath = join(dir, 'batch.wal');
    const dbPath = join(dir, 'batch.db');
    
    const wal = new FileWAL(walPath);
    
    // Batch 1: 10 inserts, committed
    const tx1 = wal.allocateTxId();
    for (let i = 0; i < 10; i++) {
      wal.appendInsert(tx1, 'batch', 0, i, [i, `batch1_${i}`]);
    }
    wal.appendCommit(tx1);
    
    // Batch 2: 10 inserts, committed
    const tx2 = wal.allocateTxId();
    for (let i = 10; i < 20; i++) {
      wal.appendInsert(tx2, 'batch', 0, i, [i, `batch2_${i}`]);
    }
    wal.appendCommit(tx2);
    
    // Batch 3: 5 inserts, NOT committed (crash mid-batch)
    const tx3 = wal.allocateTxId();
    for (let i = 20; i < 25; i++) {
      wal.appendInsert(tx3, 'batch', 0, i, [i, `batch3_${i}`]);
    }
    // No commit for tx3!
    
    wal.flush();
    
    // Recover
    const dm = new DiskManager(dbPath);
    const bp = new BufferPool(16);
    const heap = new FileBackedHeap('batch', dm, bp);
    
    recoverFromFileWAL(heap, wal);
    
    const rows = [...heap.scan()].map(r => r.values);
    
    // Should have 20 rows (batch 1 + batch 2), NOT 25
    assert.strictEqual(rows.length, 20, `Expected 20 committed rows, got ${rows.length}`);
    
    // Verify batch 3 data is absent
    const batch3Rows = rows.filter(r => r[0] >= 20);
    assert.strictEqual(batch3Rows.length, 0, 'Batch 3 (uncommitted) should be absent');
    
    wal.close();
    dm.close();
  });

  it('interleaved transactions: committed ones recovered, uncommitted discarded', () => {
    const dir = testDir();
    dirs.push(dir);
    mkdirSync(dir, { recursive: true });
    
    const walPath = join(dir, 'interleave.wal');
    const dbPath = join(dir, 'interleave.db');
    
    const wal = new FileWAL(walPath);
    const txA = wal.allocateTxId(); // Will commit
    const txB = wal.allocateTxId(); // Will abort
    const txC = wal.allocateTxId(); // Will commit
    
    // Interleaved operations
    wal.appendInsert(txA, 'data', 0, 0, [1, 'A1']);
    wal.appendInsert(txB, 'data', 0, 1, [2, 'B1']);
    wal.appendInsert(txC, 'data', 0, 2, [3, 'C1']);
    wal.appendInsert(txA, 'data', 0, 3, [4, 'A2']);
    wal.appendInsert(txB, 'data', 0, 4, [5, 'B2']);
    wal.appendCommit(txA);
    wal.appendInsert(txC, 'data', 0, 5, [6, 'C2']);
    // txB crashes (no commit or abort)
    wal.appendCommit(txC);
    
    wal.flush();
    
    const dm = new DiskManager(dbPath);
    const bp = new BufferPool(16);
    const heap = new FileBackedHeap('data', dm, bp);
    
    recoverFromFileWAL(heap, wal);
    
    const rows = [...heap.scan()].map(r => r.values);
    
    // Should have: A1, A2 (txA committed), C1, C2 (txC committed) = 4 rows
    assert.strictEqual(rows.length, 4, `Expected 4 committed rows, got ${rows.length}`);
    
    // B1, B2 should be absent
    const bRows = rows.filter(r => String(r[1]).startsWith('B'));
    assert.strictEqual(bRows.length, 0, 'txB (uncommitted) should not survive');
    
    wal.close();
    dm.close();
  });
});
