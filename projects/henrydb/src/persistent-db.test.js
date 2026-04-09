// persistent-db.test.js — PersistentDatabase integration tests
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { PersistentDatabase } from './persistent-db.js';
import { rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const testDir = () => join(tmpdir(), `henrydb-persistent-${Date.now()}-${Math.random().toString(36).slice(2)}`);

describe('PersistentDatabase', () => {
  const dirs = [];
  
  afterEach(() => {
    for (const d of dirs) {
      try { if (existsSync(d)) rmSync(d, { recursive: true }); } catch {}
    }
    dirs.length = 0;
  });

  it('basic SQL operations', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    
    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].name, 'Alice');
    assert.strictEqual(result.rows[1].name, 'Bob');
    
    db.close();
  });

  it('data persists across close/reopen', () => {
    const d = testDir();
    dirs.push(d);
    
    // Create and populate
    {
      const db = PersistentDatabase.open(d);
      db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT)');
      db.execute("INSERT INTO items VALUES (1, 'Widget', 10)");
      db.execute("INSERT INTO items VALUES (2, 'Gadget', 20)");
      db.execute("INSERT INTO items VALUES (3, 'Doohickey', 30)");
      db.close();
    }
    
    // Reopen and verify
    {
      const db = PersistentDatabase.open(d);
      const result = db.execute('SELECT * FROM items ORDER BY id');
      assert.strictEqual(result.rows.length, 3);
      assert.strictEqual(result.rows[0].name, 'Widget');
      assert.strictEqual(result.rows[2].name, 'Doohickey');
      
      // Add more data
      db.execute("INSERT INTO items VALUES (4, 'Thingamajig', 40)");
      db.close();
    }
    
    // Verify again
    {
      const db = PersistentDatabase.open(d);
      const result = db.execute('SELECT COUNT(*) as cnt FROM items');
      assert.strictEqual(result.rows[0].cnt, 4);
      db.close();
    }
  });

  it('multiple tables persist', () => {
    const d = testDir();
    dirs.push(d);
    
    {
      const db = PersistentDatabase.open(d);
      db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
      db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total INT)');
      db.execute("INSERT INTO users VALUES (1, 'Alice')");
      db.execute("INSERT INTO orders VALUES (100, 1, 50)");
      db.close();
    }
    
    {
      const db = PersistentDatabase.open(d);
      const users = db.execute('SELECT * FROM users');
      assert.strictEqual(users.rows.length, 1);
      assert.strictEqual(users.rows[0].name, 'Alice');
      
      const orders = db.execute('SELECT * FROM orders');
      assert.strictEqual(orders.rows.length, 1);
      assert.strictEqual(orders.rows[0].total, 50);
      
      db.close();
    }
  });

  it('aggregates work with persistent storage', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE data (id INT PRIMARY KEY, val INT, grp TEXT)');
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO data VALUES (${i}, ${i * 10}, 'group_${i % 5}')`);
    }
    
    const result = db.execute(`
      SELECT grp, COUNT(*) as cnt, SUM(val) as total, AVG(val) as avg_val
      FROM data
      GROUP BY grp
      ORDER BY grp
    `);
    
    assert.strictEqual(result.rows.length, 5);
    assert.strictEqual(result.rows[0].cnt, 10);
    
    db.close();
  });

  it('joins work with persistent storage', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d);
    db.execute('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT)');
    
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
    db.execute("INSERT INTO departments VALUES (2, 'Sales')");
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1)");
    db.execute("INSERT INTO employees VALUES (3, 'Carol', 2)");
    
    const result = db.execute(`
      SELECT e.name as emp, d.name as dept
      FROM employees e
      JOIN departments d ON d.id = e.dept_id
      ORDER BY e.name
    `);
    
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].emp, 'Alice');
    assert.strictEqual(result.rows[0].dept, 'Engineering');
    
    db.close();
  });

  it('buffer pool stats are available', () => {
    const d = testDir();
    dirs.push(d);
    
    const db = PersistentDatabase.open(d, { poolSize: 8 });
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO t VALUES (${i})`);
    }
    
    const stats = db.stats();
    assert.ok(stats.poolSize === 8);
    assert.ok(stats.used >= 0);
    
    db.close();
  });

  it('index rebuild: PK lookups work after reopen', () => {
    const dir = testDir();
    dirs.push(dir);
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Carol', 35)");
    db.close();

    // Reopen — indexes should be rebuilt from heap data
    const db2 = PersistentDatabase.open(dir);
    const r = db2.execute('SELECT * FROM users WHERE id = 2');
    assert.strictEqual(r.rows.length, 1);
    assert.strictEqual(r.rows[0].name, 'Bob');
    
    // COUNT should work
    const count = db2.execute('SELECT COUNT(*) as cnt FROM users');
    assert.strictEqual(count.rows[0].cnt, 3);
    
    // INSERT after reopen should work (no PK conflict)
    db2.execute("INSERT INTO users VALUES (4, 'Dave', 28)");
    const all = db2.execute('SELECT * FROM users');
    assert.strictEqual(all.rows.length, 4);
    
    db2.close();
  });

  it('data survives multiple close/reopen cycles', () => {
    const dir = testDir();
    dirs.push(dir);
    // Cycle 1: create and insert
    let db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE counters (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO counters VALUES (1, 100)');
    db.close();

    // Cycle 2: update
    db = PersistentDatabase.open(dir);
    db.execute('UPDATE counters SET val = 200 WHERE id = 1');
    db.execute('INSERT INTO counters VALUES (2, 300)');
    db.close();

    // Cycle 3: verify
    db = PersistentDatabase.open(dir);
    const r = db.execute('SELECT * FROM counters ORDER BY id');
    assert.strictEqual(r.rows.length, 2);
    assert.strictEqual(r.rows[0].val, 200);
    assert.strictEqual(r.rows[1].val, 300);
    db.close();
  });

  it('DELETE persists across restarts', () => {
    const dir = testDir();
    dirs.push(dir);
    const db = PersistentDatabase.open(dir);
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    for (let i = 1; i <= 10; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'item${i}')`);
    }
    db.execute('DELETE FROM items WHERE id > 5');
    db.close();

    const db2 = PersistentDatabase.open(dir);
    const r = db2.execute('SELECT COUNT(*) as cnt FROM items');
    assert.strictEqual(r.rows[0].cnt, 5);
    db2.close();
  });
});
