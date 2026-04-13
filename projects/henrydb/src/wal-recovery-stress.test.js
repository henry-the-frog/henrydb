// wal-recovery-stress.test.js — Complex interleaved DDL+DML recovery scenarios
// Stress tests the WAL recovery path with realistic multi-table workflows.

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

function tmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'henrydb-wal-stress-'));
}

function cleanup(dir) {
  try { fs.rmSync(dir, { recursive: true }); } catch {}
}

describe('WAL Recovery Stress: Interleaved DDL + DML', () => {
  it('full lifecycle: create → populate → modify → truncate → repopulate → drop some → recover', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });

      // Phase 1: Create tables
      db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)');
      db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)');
      db.execute('CREATE TABLE logs (id INT PRIMARY KEY, msg TEXT)');

      // Phase 2: Populate
      db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')");
      db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@test.com')");
      db.execute("INSERT INTO users VALUES (3, 'Carol', 'carol@test.com')");
      db.execute('INSERT INTO orders VALUES (1, 1, 100)');
      db.execute('INSERT INTO orders VALUES (2, 1, 200)');
      db.execute('INSERT INTO orders VALUES (3, 2, 150)');
      db.execute("INSERT INTO logs VALUES (1, 'system started')");
      db.execute("INSERT INTO logs VALUES (2, 'user created')");

      // Phase 3: Modify
      db.execute("UPDATE users SET email = 'alice_new@test.com' WHERE id = 1");
      db.execute('DELETE FROM orders WHERE amount < 150');

      // Phase 4: Truncate logs
      db.execute('TRUNCATE TABLE logs');
      db.execute("INSERT INTO logs VALUES (10, 'after truncate')");

      // Phase 5: Drop orders table
      db.execute('DROP TABLE orders');

      // Phase 6: More modifications to surviving tables
      db.execute("INSERT INTO users VALUES (4, 'Dave', 'dave@test.com')");
      db.execute('DELETE FROM users WHERE id = 2');

      db.close();

      // Recover and verify
      const db2 = Database.recover(dir);

      // Users: Alice(updated), Carol, Dave (Bob deleted)
      const users = db2.execute('SELECT * FROM users ORDER BY id').rows;
      assert.strictEqual(users.length, 3);
      assert.strictEqual(users[0].name, 'Alice');
      assert.strictEqual(users[0].email, 'alice_new@test.com');
      assert.strictEqual(users[1].name, 'Carol');
      assert.strictEqual(users[2].name, 'Dave');

      // Orders: should be dropped
      assert.throws(() => db2.execute('SELECT * FROM orders'), /not found/i);

      // Logs: should only have post-truncate entry
      const logs = db2.execute('SELECT * FROM logs').rows;
      assert.strictEqual(logs.length, 1);
      assert.strictEqual(logs[0].msg, 'after truncate');

      db2.close();
    } finally { cleanup(dir); }
  });

  it('repeated create-drop cycles for same table name', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });

      // Cycle 1
      db.execute('CREATE TABLE temp (id INT PRIMARY KEY, ver INT)');
      db.execute('INSERT INTO temp VALUES (1, 1)');
      db.execute('INSERT INTO temp VALUES (2, 1)');
      db.execute('DROP TABLE temp');

      // Cycle 2
      db.execute('CREATE TABLE temp (id INT PRIMARY KEY, ver INT, extra TEXT)');
      db.execute("INSERT INTO temp VALUES (10, 2, 'cycle2')");
      db.execute('DROP TABLE temp');

      // Cycle 3 (final — survives)
      db.execute('CREATE TABLE temp (id INT PRIMARY KEY, ver INT)');
      db.execute('INSERT INTO temp VALUES (100, 3)');
      db.execute('INSERT INTO temp VALUES (101, 3)');

      db.close();

      const db2 = Database.recover(dir);
      const rows = db2.execute('SELECT * FROM temp ORDER BY id').rows;
      assert.strictEqual(rows.length, 2);
      assert.strictEqual(rows[0].id, 100);
      assert.strictEqual(rows[0].ver, 3);
      assert.strictEqual(rows[1].id, 101);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('bulk inserts → truncate → bulk inserts → recover', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE big (id INT PRIMARY KEY, val INT)');

      // Insert 100 rows
      for (let i = 0; i < 100; i++) {
        db.execute(`INSERT INTO big VALUES (${i}, ${i * 10})`);
      }
      assert.strictEqual(db.execute('SELECT COUNT(*) as cnt FROM big').rows[0].cnt, 100);

      // Truncate
      db.execute('TRUNCATE TABLE big');

      // Insert 50 new rows
      for (let i = 200; i < 250; i++) {
        db.execute(`INSERT INTO big VALUES (${i}, ${i * 5})`);
      }

      db.close();

      const db2 = Database.recover(dir);
      const cnt = db2.execute('SELECT COUNT(*) as cnt FROM big').rows[0].cnt;
      assert.strictEqual(cnt, 50, 'Should have only post-truncate rows');

      const min = db2.execute('SELECT MIN(id) as m FROM big').rows[0].m;
      const max = db2.execute('SELECT MAX(id) as m FROM big').rows[0].m;
      assert.strictEqual(min, 200);
      assert.strictEqual(max, 249);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('interleaved operations on multiple tables', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE a (id INT PRIMARY KEY, val TEXT)');
      db.execute('CREATE TABLE b (id INT PRIMARY KEY, val TEXT)');
      db.execute('CREATE TABLE c (id INT PRIMARY KEY, val TEXT)');

      // Interleave operations
      db.execute("INSERT INTO a VALUES (1, 'a1')");
      db.execute("INSERT INTO b VALUES (1, 'b1')");
      db.execute("INSERT INTO c VALUES (1, 'c1')");
      db.execute("INSERT INTO a VALUES (2, 'a2')");
      db.execute("UPDATE b SET val = 'b1_updated' WHERE id = 1");
      db.execute("INSERT INTO c VALUES (2, 'c2')");
      db.execute('TRUNCATE TABLE a');
      db.execute("INSERT INTO b VALUES (2, 'b2')");
      db.execute("INSERT INTO a VALUES (3, 'a3_after_truncate')");
      db.execute('DROP TABLE c');
      db.execute("INSERT INTO a VALUES (4, 'a4')");

      db.close();

      const db2 = Database.recover(dir);

      // Table a: truncated then 2 new rows
      const aRows = db2.execute('SELECT * FROM a ORDER BY id').rows;
      assert.strictEqual(aRows.length, 2);
      assert.strictEqual(aRows[0].val, 'a3_after_truncate');
      assert.strictEqual(aRows[1].val, 'a4');

      // Table b: original + update + insert
      const bRows = db2.execute('SELECT * FROM b ORDER BY id').rows;
      assert.strictEqual(bRows.length, 2);
      assert.strictEqual(bRows[0].val, 'b1_updated');
      assert.strictEqual(bRows[1].val, 'b2');

      // Table c: dropped
      assert.throws(() => db2.execute('SELECT * FROM c'), /not found/i);

      db2.close();
    } finally { cleanup(dir); }
  });

  it('empty tables and tables with only NULLs recover correctly', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE empty_t (id INT PRIMARY KEY, val TEXT)');
      db.execute('CREATE TABLE null_t (id INT PRIMARY KEY, val TEXT)');
      db.execute('INSERT INTO null_t VALUES (1, NULL)');
      db.execute('INSERT INTO null_t VALUES (2, NULL)');
      db.close();

      const db2 = Database.recover(dir);
      const empty = db2.execute('SELECT COUNT(*) as cnt FROM empty_t').rows[0].cnt;
      assert.strictEqual(empty, 0);
      const nulls = db2.execute('SELECT * FROM null_t ORDER BY id').rows;
      assert.strictEqual(nulls.length, 2);
      assert.strictEqual(nulls[0].val, null);
      assert.strictEqual(nulls[1].val, null);
      db2.close();
    } finally { cleanup(dir); }
  });

  it('many small tables created and some dropped', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      const keepTables = [];
      const dropTables = [];

      for (let i = 0; i < 20; i++) {
        const name = `t${i}`;
        db.execute(`CREATE TABLE ${name} (id INT PRIMARY KEY, val INT)`);
        db.execute(`INSERT INTO ${name} VALUES (1, ${i})`);
        if (i % 3 === 0) {
          db.execute(`DROP TABLE ${name}`);
          dropTables.push(name);
        } else {
          keepTables.push(name);
        }
      }
      db.close();

      const db2 = Database.recover(dir);
      for (const name of keepTables) {
        const rows = db2.execute(`SELECT * FROM ${name}`).rows;
        assert.strictEqual(rows.length, 1, `${name} should have 1 row`);
      }
      for (const name of dropTables) {
        assert.throws(() => db2.execute(`SELECT * FROM ${name}`), /not found/i,
          `${name} should be dropped`);
      }
      db2.close();
    } finally { cleanup(dir); }
  });

  it('double recovery produces same result', () => {
    const dir = tmpDir();
    try {
      const db = new Database({ dataDir: dir });
      db.execute('CREATE TABLE t (id INT PRIMARY KEY, val TEXT)');
      db.execute("INSERT INTO t VALUES (1, 'one')");
      db.execute("INSERT INTO t VALUES (2, 'two')");
      db.execute('TRUNCATE TABLE t');
      db.execute("INSERT INTO t VALUES (3, 'three')");
      db.close();

      // First recovery
      const db2 = Database.recover(dir);
      const rows1 = db2.execute('SELECT * FROM t ORDER BY id').rows;
      db2.close();

      // Second recovery from same WAL
      const db3 = Database.recover(dir);
      const rows2 = db3.execute('SELECT * FROM t ORDER BY id').rows;
      db3.close();

      assert.deepStrictEqual(rows1, rows2, 'Double recovery should produce identical results');
      assert.strictEqual(rows1.length, 1);
      assert.strictEqual(rows1[0].val, 'three');
    } finally { cleanup(dir); }
  });
});
