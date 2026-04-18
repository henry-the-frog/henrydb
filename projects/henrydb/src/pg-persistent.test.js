// pg-persistent.test.js — Test PG wire + PersistentDatabase restart cycle
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { createPgServer } from './pg-server.js';
import { PersistentDatabase } from './persistent-db.js';
import { existsSync, rmSync, mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const BASE_PORT = 17500 + Math.floor(Math.random() * 500);
let portCounter = 0;

function nextPort() { return BASE_PORT + portCounter++; }

function clientConfig(port) {
  return { host: 'localhost', port, user: 'henrydb', database: 'henrydb', password: '' };
}

describe('PG Wire + Persistent Storage', () => {
  test('data survives server restart', async () => {
    const dir = join(tmpdir(), 'henrydb-pg-persist-' + Date.now());
    if (existsSync(dir)) rmSync(dir, { recursive: true });

    const port1 = nextPort();

    // Phase 1: Create and populate
    let pdb = PersistentDatabase.open(dir, { poolSize: 16 });
    let server = createPgServer(pdb, port1);
    await new Promise(r => setTimeout(r, 200));

    let client = new pg.Client(clientConfig(port1));
    client.on('error', () => {});
    await client.connect();

    await client.query('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    await client.query("INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Carol', 35)");

    const r1 = await client.query('SELECT COUNT(*) as cnt FROM users');
    assert.equal(r1.rows[0].cnt, '3');

    await client.end();
    await new Promise(r => { server.close(r); });
    pdb.close();

    // Phase 2: Restart
    const port2 = nextPort();
    pdb = PersistentDatabase.open(dir, { poolSize: 16 });
    server = createPgServer(pdb, port2);
    await new Promise(r => setTimeout(r, 200));

    client = new pg.Client(clientConfig(port2));
    client.on('error', () => {});
    await client.connect();

    const r2 = await client.query('SELECT * FROM users ORDER BY id');
    assert.equal(r2.rows.length, 3);
    assert.equal(r2.rows[0].name, 'Alice');
    assert.equal(r2.rows[2].name, 'Carol');

    // Insert more
    await client.query("INSERT INTO users VALUES (4, 'Dave', 28)");
    const r3 = await client.query('SELECT COUNT(*) as cnt FROM users');
    assert.equal(r3.rows[0].cnt, '4');

    await client.end();
    await new Promise(r => { server.close(r); });
    pdb.close();

    // Phase 3: Second restart — verify Dave persisted
    const port3 = nextPort();
    pdb = PersistentDatabase.open(dir, { poolSize: 16 });
    server = createPgServer(pdb, port3);
    await new Promise(r => setTimeout(r, 200));

    client = new pg.Client(clientConfig(port3));
    client.on('error', () => {});
    await client.connect();

    const r4 = await client.query('SELECT * FROM users ORDER BY id');
    assert.equal(r4.rows.length, 4);
    assert.equal(r4.rows[3].name, 'Dave');

    await client.end();
    await new Promise(r => { server.close(r); });
    pdb.close();

    // Cleanup
    rmSync(dir, { recursive: true });
  });

  test('SET and SHOW interceptors work', async () => {
    const dir = join(tmpdir(), 'henrydb-pg-set-' + Date.now());
    const port = nextPort();
    const pdb = PersistentDatabase.open(dir, { poolSize: 16 });
    const server = createPgServer(pdb, port);
    await new Promise(r => setTimeout(r, 200));

    const client = new pg.Client(clientConfig(port));
    await client.connect();

    await client.query('SET client_encoding TO utf8');
    const r1 = await client.query('SHOW server_version');
    assert.ok(r1.rows[0].server_version.includes('HenryDB'));

    const r2 = await client.query('SELECT version()');
    assert.ok(r2.rows[0].version.includes('HenryDB'));

    const r3 = await client.query('SELECT current_database()');
    assert.equal(r3.rows[0].current_database, 'henrydb');

    await client.end();
    await new Promise(r => { server.close(r); });
    pdb.close();
    rmSync(dir, { recursive: true });
  });

  test('parameterized queries work with PersistentDatabase', async () => {
    const dir = join(tmpdir(), 'henrydb-pg-param-' + Date.now());
    const port = nextPort();
    const pdb = PersistentDatabase.open(dir, { poolSize: 16 });
    const server = createPgServer(pdb, port);
    await new Promise(r => setTimeout(r, 200));

    const client = new pg.Client(clientConfig(port));
    await client.connect();

    await client.query('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, qty INT)');
    
    for (let i = 1; i <= 5; i++) {
      await client.query('INSERT INTO items VALUES ($1, $2, $3)', [i, `item_${i}`, i * 10]);
    }

    const r1 = await client.query('SELECT * FROM items WHERE qty > $1 ORDER BY id', [25]);
    assert.equal(r1.rows.length, 3); // qty 30, 40, 50

    const r2 = await client.query('SELECT * FROM items WHERE name = $1', ['item_3']);
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].qty, 30);

    await client.end();
    await new Promise(r => { server.close(r); });
    pdb.close();
    rmSync(dir, { recursive: true });
  });

  test('pool with PersistentDatabase', async () => {
    const dir = join(tmpdir(), 'henrydb-pg-pool-' + Date.now());
    const port = nextPort();
    const pdb = PersistentDatabase.open(dir, { poolSize: 16 });
    const server = createPgServer(pdb, port);
    await new Promise(r => setTimeout(r, 200));

    const pool = new pg.Pool({ ...clientConfig(port), max: 3 });

    await pool.query('CREATE TABLE data (id INT PRIMARY KEY, value TEXT)');
    
    // Concurrent inserts
    await Promise.all(
      Array.from({ length: 10 }, (_, i) =>
        pool.query('INSERT INTO data VALUES ($1, $2)', [i, `val_${i}`])
      )
    );

    const r = await pool.query('SELECT COUNT(*) as cnt FROM data');
    assert.equal(r.rows[0].cnt, '10');

    await pool.end();
    await new Promise(r => { server.close(r); });
    pdb.close();
    rmSync(dir, { recursive: true });
  });
});
