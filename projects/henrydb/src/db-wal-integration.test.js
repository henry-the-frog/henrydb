// db-wal-integration.test.js — Tests for Database + WAL crash recovery
import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Database } from './db.js';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-dbwal-'));
}

function cleanup(dir) {
  fs.rmSync(dir, { recursive: true, force: true });
}

describe('Database + WAL Integration', () => {
  let dir;

  beforeEach(() => { dir = tmpDir(); });
  afterEach(() => { cleanup(dir); });

  it('creates a Database with persistent WAL', () => {
    const db = new Database({ dataDir: dir });
    assert.ok(fs.existsSync(path.join(dir, 'wal')));
    db.close();
  });

  it('records operations to WAL', () => {
    const db = new Database({ dataDir: dir, walSync: 'immediate' });
    db.execute('CREATE TABLE users (id INTEGER, name TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    
    const records = db.getWALRecords();
    assert.ok(records.length > 0, 'WAL should have records');
    
    db.close();
  });

  it('recovers data after simulated crash', () => {
    // Phase 1: Write data
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE accounts (id INTEGER, name TEXT, balance INTEGER)');
    db1.execute("INSERT INTO accounts VALUES (1, 'Alice', 1000)");
    db1.execute("INSERT INTO accounts VALUES (2, 'Bob', 500)");
    db1.execute("INSERT INTO accounts VALUES (3, 'Charlie', 750)");
    db1.close(); // "Normal shutdown" — but imagine a crash

    // Phase 2: Recover
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM accounts ORDER BY id');
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[0].balance, 1000);
    assert.strictEqual(result.rows[1].name, 'Bob');
    assert.strictEqual(result.rows[2].name, 'Charlie');
    db2.close();
  });

  it('recovers after checkpoint + additional writes', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE items (id INTEGER, name TEXT)');
    db1.execute("INSERT INTO items VALUES (1, 'Widget')");
    db1.execute("INSERT INTO items VALUES (2, 'Gadget')");
    
    // Checkpoint
    db1.checkpoint();
    
    // More writes after checkpoint
    db1.execute("INSERT INTO items VALUES (3, 'Doohickey')");
    db1.execute("UPDATE items SET name = 'Super Widget' WHERE id = 1");
    db1.close();

    // Recover — should have all 3 items with the update
    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM items ORDER BY id');
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].name, 'Super Widget'); // Updated
    assert.strictEqual(result.rows[2].name, 'Doohickey');
    db2.close();
  });

  it('query after recovery works normally', () => {
    // Phase 1: Write initial data
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE products (id INTEGER, name TEXT, price REAL)');
    db1.execute("INSERT INTO products VALUES (1, 'Phone', 599)");
    db1.execute("INSERT INTO products VALUES (2, 'Laptop', 999)");
    db1.execute("INSERT INTO products VALUES (3, 'Tablet', 399)");
    db1.close();

    // Phase 2: Recover and continue operating
    const db2 = Database.recover(dir);
    
    // Can query
    const r1 = db2.execute('SELECT COUNT(*) AS cnt FROM products');
    assert.strictEqual(r1.rows[0].cnt, 3);
    
    // Can aggregate
    const r2 = db2.execute('SELECT SUM(price) AS total FROM products');
    assert.strictEqual(r2.rows[0].total, 1997);
    
    // Can insert new data
    db2.execute("INSERT INTO products VALUES (4, 'Watch', 299)");
    const r3 = db2.execute('SELECT COUNT(*) AS cnt FROM products');
    assert.strictEqual(r3.rows[0].cnt, 4);
    
    db2.close();
  });

  it('multiple recovery cycles', () => {
    // Cycle 1: Create and write
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE activity_log (id INTEGER, msg TEXT)');
    db1.execute("INSERT INTO activity_log VALUES (1, 'first')");
    db1.close();

    // Cycle 2: Recover and add more
    const db2 = Database.recover(dir);
    db2.execute("INSERT INTO activity_log VALUES (2, 'second')");
    db2.close();

    // Cycle 3: Recover again — should have both
    const db3 = Database.recover(dir);
    const result = db3.execute('SELECT * FROM activity_log ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].msg, 'first');
    assert.strictEqual(result.rows[1].msg, 'second');
    db3.close();
  });

  it('DELETE is recovered correctly', () => {
    const db1 = new Database({ dataDir: dir, walSync: 'immediate' });
    db1.execute('CREATE TABLE temp (id INTEGER, val TEXT)');
    db1.execute("INSERT INTO temp VALUES (1, 'keep')");
    db1.execute("INSERT INTO temp VALUES (2, 'delete')");
    db1.execute("INSERT INTO temp VALUES (3, 'keep_too')");
    db1.execute("DELETE FROM temp WHERE id = 2");
    db1.close();

    const db2 = Database.recover(dir);
    const result = db2.execute('SELECT * FROM temp ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].val, 'keep');
    assert.strictEqual(result.rows[1].val, 'keep_too');
    db2.close();
  });

  it('WAL stats are accessible', () => {
    const db = new Database({ dataDir: dir, walSync: 'none' });
    db.execute('CREATE TABLE stats_test (id INTEGER)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO stats_test VALUES (${i})`);
    }
    
    const stats = db.wal.getStats();
    assert.ok(stats.recordsWritten > 0, 'Should have WAL records');
    assert.ok(stats.bytesWritten > 0, 'Should have WAL bytes');
    
    db.close();
  });
});
