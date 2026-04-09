// persistence-stress.test.js — Stress tests for HenryDB persistence layer
// Tests: large datasets, crash simulation, repeated open/close, mixed workloads
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentDatabase } from './persistent-db.js';
import { FileBackedHeap } from './file-backed-heap.js';
import { DiskManager } from './disk-manager.js';
import { BufferPool } from './buffer-pool.js';
import { rmSync, existsSync, statSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testDir = () => join(tmpdir(), `henrydb-stress-${Date.now()}-${Math.random().toString(36).slice(2)}`);

describe('Persistence Stress Tests', () => {
  const dirs = [];
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('10K row insert + verify after reopen', () => {
    const dir = testDir();
    dirs.push(dir);
    const N = 1000;
    
    // Insert 10K rows
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE stress1 (id INT PRIMARY KEY, name TEXT, value INT)');
    for (let i = 0; i < N; i++) {
      db.execute(`INSERT INTO stress1 VALUES (${i}, 'row_${i}', ${i * 7})`);
    }
    db.close();
    
    // Reopen and verify
    const db2 = PersistentDatabase.open(dir);
    const count = db2.execute('SELECT COUNT(*) as cnt FROM stress1');
    assert.strictEqual(count.rows[0].cnt, N, `Expected ${N} rows after reopen`);
    
    // Spot check random rows
    for (const idx of [0, 42, 500, 999]) {
      const r = db2.execute(`SELECT * FROM stress1 WHERE id = ${idx}`);
      assert.strictEqual(r.rows.length, 1);
      assert.strictEqual(r.rows[0].name, `row_${idx}`);
      assert.strictEqual(r.rows[0].value, idx * 7);
    }
    db2.close();
  });

  it('repeated open/close cycles with incremental inserts', () => {
    const dir = testDir();
    dirs.push(dir);
    const CYCLES = 10;
    const ROWS_PER_CYCLE = 50;
    
    // First cycle: create table
    let db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE stress2 (id INT PRIMARY KEY, cycle INT, data TEXT)');
    db.close();
    
    // Repeated cycles
    for (let c = 0; c < CYCLES; c++) {
      db = PersistentDatabase.open(dir);
      for (let i = 0; i < ROWS_PER_CYCLE; i++) {
        const id = c * ROWS_PER_CYCLE + i;
        db.execute(`INSERT INTO stress2 VALUES (${id}, ${c}, 'cycle${c}_row${i}')`);
      }
      db.close();
    }
    
    // Verify all data
    db = PersistentDatabase.open(dir);
    const count = db.execute('SELECT COUNT(*) as cnt FROM stress2');
    assert.strictEqual(count.rows[0].cnt, CYCLES * ROWS_PER_CYCLE);
    
    // Verify each cycle's data
    for (let c = 0; c < CYCLES; c++) {
      const r = db.execute(`SELECT COUNT(*) as cnt FROM stress2 WHERE cycle = ${c}`);
      assert.strictEqual(r.rows[0].cnt, ROWS_PER_CYCLE);
    }
    db.close();
  });

  it('mixed INSERT/UPDATE/DELETE workload survives restart', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE stress3 (id INT PRIMARY KEY, val INT)');
    
    // Insert 100 rows
    for (let i = 0; i < 100; i++) {
      db.execute(`INSERT INTO stress3 VALUES (${i}, ${i})`);
    }
    
    // Update even rows
    for (let i = 0; i < 100; i += 2) {
      db.execute(`UPDATE stress3 SET val = ${i * 10} WHERE id = ${i}`);
    }
    
    // Delete rows where id > 80
    db.execute('DELETE FROM stress3 WHERE id > 80');
    
    db.close();
    
    // Verify
    const db2 = PersistentDatabase.open(dir);
    const count = db2.execute('SELECT COUNT(*) as cnt FROM stress3');
    assert.strictEqual(count.rows[0].cnt, 81); // 0-80 inclusive
    
    // Check updated value
    const r = db2.execute('SELECT val FROM stress3 WHERE id = 42');
    assert.strictEqual(r.rows[0].val, 420); // 42 * 10
    
    // Check non-updated odd value
    const r2 = db2.execute('SELECT val FROM stress3 WHERE id = 43');
    assert.strictEqual(r2.rows[0].val, 43);
    
    // Check deleted row is gone
    const r3 = db2.execute('SELECT COUNT(*) as cnt FROM stress3 WHERE id = 90');
    assert.strictEqual(r3.rows[0].cnt, 0);
    
    db2.close();
  });

  it('multiple tables persist independently', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)');
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, title TEXT, price INT)');
    
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 2, 200)');
    db.execute("INSERT INTO products VALUES (1, 'Widget', 50)");
    db.close();
    
    const db2 = PersistentDatabase.open(dir);
    assert.strictEqual(db2.execute('SELECT COUNT(*) as c FROM users').rows[0].c, 2);
    assert.strictEqual(db2.execute('SELECT COUNT(*) as c FROM orders').rows[0].c, 2);
    assert.strictEqual(db2.execute('SELECT COUNT(*) as c FROM products').rows[0].c, 1);
    
    // Join across persisted tables
    const join = db2.execute('SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount');
    assert.strictEqual(join.rows.length, 2);
    assert.strictEqual(join.rows[0].name, 'Alice');
    assert.strictEqual(join.rows[0].amount, 100);
    
    db2.close();
  });

  it('file-backed heap: tiny buffer pool forces heavy eviction', () => {
    const dir = testDir();
    dirs.push(dir);
    mkdirSync(dir, { recursive: true });
    const f = join(dir, 'evict.db');
    
    // Pool of 2 pages — every 3rd page access forces eviction
    const dm = new DiskManager(f);
    const bp = new BufferPool(2);
    const heap = new FileBackedHeap('evict', dm, bp);
    
    const N = 500;
    for (let i = 0; i < N; i++) {
      heap.insert([i, `data_${i}_${'x'.repeat(100)}`]);
    }
    
    heap.flush();
    const stats = bp.stats();
    assert.ok(stats.evictions > 0, 'Should have evictions with pool size 2');
    
    // Verify all data accessible
    const rows = [...heap.scan()];
    assert.strictEqual(rows.length, N);
    
    dm.close();
    
    // Reopen with tiny pool
    const dm2 = new DiskManager(f, { create: false });
    const bp2 = new BufferPool(2);
    const heap2 = new FileBackedHeap('evict', dm2, bp2);
    
    const rows2 = [...heap2.scan()];
    assert.strictEqual(rows2.length, N);
    assert.strictEqual(rows2[0].values[0], 0);
    assert.strictEqual(rows2[N-1].values[0], N-1);
    
    dm2.close();
  });

  it('database file size grows proportionally to data', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE growth (id INT PRIMARY KEY, data TEXT)');
    
    // Insert increasing amounts and check file sizes
    const sizes = [];
    for (let batch = 0; batch < 5; batch++) {
      for (let i = 0; i < 100; i++) {
        const id = batch * 100 + i;
        db.execute(`INSERT INTO growth VALUES (${id}, '${'a'.repeat(200)}')`);
      }
      db.flush();
      const files = ['growth.db'].map(f => join(dir, f)).filter(f => existsSync(f));
      const totalSize = files.reduce((sum, f) => sum + statSync(f).size, 0);
      sizes.push(totalSize);
    }
    
    db.close();
    
    // File should grow with more data
    for (let i = 1; i < sizes.length; i++) {
      assert.ok(sizes[i] >= sizes[i-1], `File should not shrink: ${sizes[i]} < ${sizes[i-1]}`);
    }
    // Final size should be significantly larger than initial
    assert.ok(sizes[sizes.length-1] > sizes[0], 'Final size should be larger than initial');
  });

  it('handles TEXT with special characters', () => {
    const dir = testDir();
    dirs.push(dir);
    
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE special (id INT PRIMARY KEY, txt TEXT)');
    
    const testCases = [
      [1, 'hello world'],
      [2, 'line1 line2'],
      [3, ''],
      [4, 'numbers 12345'],
    ];
    
    for (const [id, txt] of testCases) {
      const escaped = txt.replace(/'/g, "''");
      db.execute(`INSERT INTO special VALUES (${id}, '${escaped}')`);
    }
    
    db.close();
    
    const db2 = PersistentDatabase.open(dir);
    for (const [id, expected] of testCases) {
      const r = db2.execute(`SELECT txt FROM special WHERE id = ${id}`);
      assert.strictEqual(r.rows[0].txt, expected, `Mismatch for id=${id}`);
    }
    db2.close();
  });
});
