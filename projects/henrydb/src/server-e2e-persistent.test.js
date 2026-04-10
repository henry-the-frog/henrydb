// server-e2e-persistent.test.js — End-to-end test using real pg client
// Tests the full lifecycle: connect → create → insert → stop → restart → reconnect → verify
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync, existsSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 16000 + Math.floor(Math.random() * 10000);
}

async function connect(port) {
  const client = new Client({
    host: '127.0.0.1',
    port,
    user: 'test',
    database: 'testdb',
    // HenryDB doesn't require password by default
  });
  await client.connect();
  return client;
}

describe('E2E Persistent Server (pg client)', () => {
  let dataDir;
  let port;

  before(() => {
    dataDir = mkdtempSync(join(tmpdir(), 'henrydb-e2e-'));
    port = getPort();
  });

  after(() => {
    if (dataDir && existsSync(dataDir)) rmSync(dataDir, { recursive: true });
  });

  it('full lifecycle: create, insert, restart, reconnect, verify', async () => {
    // Session 1: Create schema and populate
    const server1 = new HenryDBServer({ port, dataDir });
    await server1.start();

    const c1 = await connect(port);
    await c1.query('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept TEXT, salary INT)');
    await c1.query("INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 95000)");
    await c1.query("INSERT INTO employees VALUES (2, 'Bob', 'Marketing', 75000)");
    await c1.query("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering', 105000)");
    await c1.query("INSERT INTO employees VALUES (4, 'Diana', 'Sales', 85000)");
    await c1.query("INSERT INTO employees VALUES (5, 'Eve', 'Engineering', 115000)");

    // Verify data before shutdown
    const r1 = await c1.query('SELECT COUNT(*) as cnt FROM employees');
    assert.equal(String(r1.rows[0].cnt), '5');

    const r2 = await c1.query('SELECT name, salary FROM employees WHERE dept = \'Engineering\' ORDER BY salary DESC');
    assert.equal(r2.rows.length, 3);
    // Verify all Engineering employees are present
    const names = r2.rows.map(r => r.name);
    assert.ok(names.includes('Eve'));
    assert.ok(names.includes('Charlie'));
    assert.ok(names.includes('Alice'));

    await c1.end();
    await server1.stop();

    // Session 2: Reopen and verify + modify
    const server2 = new HenryDBServer({ port, dataDir });
    await server2.start();

    const c2 = await connect(port);

    // All data should be there
    const r3 = await c2.query('SELECT * FROM employees ORDER BY id');
    assert.equal(r3.rows.length, 5);
    assert.equal(r3.rows[0].name, 'Alice');
    assert.equal(r3.rows[4].name, 'Eve');

    // Aggregates should work
    const r4 = await c2.query('SELECT dept, COUNT(*) as cnt, SUM(salary) as total FROM employees GROUP BY dept ORDER BY total DESC');
    assert.ok(r4.rows.length >= 3);

    // Add more data in session 2
    await c2.query("INSERT INTO employees VALUES (6, 'Frank', 'Marketing', 80000)");
    await c2.query("INSERT INTO employees VALUES (7, 'Grace', 'Engineering', 125000)");

    const r5 = await c2.query('SELECT COUNT(*) as cnt FROM employees');
    assert.equal(String(r5.rows[0].cnt), '7');

    await c2.end();
    await server2.stop();

    // Session 3: Verify everything persisted through two restarts
    const server3 = new HenryDBServer({ port, dataDir });
    await server3.start();

    const c3 = await connect(port);
    const r6 = await c3.query('SELECT * FROM employees ORDER BY id');
    assert.equal(r6.rows.length, 7);
    assert.equal(r6.rows[5].name, 'Frank');
    assert.equal(r6.rows[6].name, 'Grace');

    // Complex query should work on recovered data
    const r7 = await c3.query('SELECT dept, COUNT(*) as cnt FROM employees GROUP BY dept ORDER BY cnt DESC');
    const eng = r7.rows.find(r => r.dept === 'Engineering');
    assert.equal(String(eng.cnt), '4'); // Alice, Charlie, Eve, Grace

    await c3.end();
    await server3.stop();
  });

  it('UPDATE and DELETE persist across restart', async () => {
    const p = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-upd-'));

    try {
      // Session 1: create, insert, update, delete
      const s1 = new HenryDBServer({ port: p, dataDir: dir });
      await s1.start();
      const c1 = await connect(p);

      await c1.query('CREATE TABLE inventory (id INT PRIMARY KEY, item TEXT, qty INT)');
      await c1.query("INSERT INTO inventory VALUES (1, 'widget', 100)");
      await c1.query("INSERT INTO inventory VALUES (2, 'gadget', 50)");
      await c1.query("INSERT INTO inventory VALUES (3, 'gizmo', 75)");

      // Update
      await c1.query('UPDATE inventory SET qty = 200 WHERE id = 1');
      // Delete
      await c1.query('DELETE FROM inventory WHERE id = 3');

      const r1 = await c1.query('SELECT * FROM inventory ORDER BY id');
      assert.equal(r1.rows.length, 2);
      assert.equal(String(r1.rows[0].qty), '200');

      await c1.end();
      await s1.stop();

      // Session 2: verify updates/deletes persisted
      const s2 = new HenryDBServer({ port: p, dataDir: dir });
      await s2.start();
      const c2 = await connect(p);

      const r2 = await c2.query('SELECT * FROM inventory ORDER BY id');
      assert.equal(r2.rows.length, 2);
      assert.equal(r2.rows[0].item, 'widget');
      assert.equal(String(r2.rows[0].qty), '200'); // Updated value
      assert.equal(r2.rows[1].item, 'gadget');
      assert.equal(String(r2.rows[1].qty), '50');
      // gizmo should be gone

      await c2.end();
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });

  it('multiple tables with JOINs after restart', async () => {
    const p = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-join-'));

    try {
      const s1 = new HenryDBServer({ port: p, dataDir: dir });
      await s1.start();
      const c1 = await connect(p);

      await c1.query('CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)');
      await c1.query('CREATE TABLE staff (id INT PRIMARY KEY, name TEXT, dept_id INT)');
      await c1.query("INSERT INTO departments VALUES (1, 'Engineering')");
      await c1.query("INSERT INTO departments VALUES (2, 'Sales')");
      await c1.query("INSERT INTO staff VALUES (1, 'Alice', 1)");
      await c1.query("INSERT INTO staff VALUES (2, 'Bob', 2)");
      await c1.query("INSERT INTO staff VALUES (3, 'Charlie', 1)");

      await c1.end();
      await s1.stop();

      // Session 2: JOIN across recovered tables
      const s2 = new HenryDBServer({ port: p, dataDir: dir });
      await s2.start();
      const c2 = await connect(p);

      const r = await c2.query(
        'SELECT s.name, d.name as dept FROM staff s JOIN departments d ON s.dept_id = d.id ORDER BY s.name'
      );
      assert.equal(r.rows.length, 3);
      assert.equal(r.rows[0].name, 'Alice');
      assert.equal(r.rows[0].dept, 'Engineering');
      assert.equal(r.rows[1].name, 'Bob');
      assert.equal(r.rows[1].dept, 'Sales');

      await c2.end();
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });

  it('handles large dataset across restart', async () => {
    const p = getPort();
    const dir = mkdtempSync(join(tmpdir(), 'henrydb-large-'));

    try {
      const s1 = new HenryDBServer({ port: p, dataDir: dir });
      await s1.start();
      const c1 = await connect(p);

      await c1.query('CREATE TABLE numbers (id INT PRIMARY KEY, val INT)');

      // Insert 100 rows
      for (let i = 1; i <= 100; i++) {
        await c1.query(`INSERT INTO numbers VALUES (${i}, ${i * i})`);
      }

      const r1 = await c1.query('SELECT COUNT(*) as cnt FROM numbers');
      assert.equal(String(r1.rows[0].cnt), '100');

      await c1.end();
      await s1.stop();

      // Reopen and verify
      const s2 = new HenryDBServer({ port: p, dataDir: dir });
      await s2.start();
      const c2 = await connect(p);

      const r2 = await c2.query('SELECT COUNT(*) as cnt FROM numbers');
      assert.equal(String(r2.rows[0].cnt), '100');

      const r3 = await c2.query('SELECT val FROM numbers WHERE id = 50');
      assert.equal(String(r3.rows[0].val), '2500');

      const r4 = await c2.query('SELECT SUM(val) as total FROM numbers');
      assert.ok(parseInt(r4.rows[0].total) > 0);

      await c2.end();
      await s2.stop();
    } finally {
      rmSync(dir, { recursive: true });
    }
  });
});
