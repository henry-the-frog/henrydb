// prepared-stmt-explore.test.js — Testing parameterized queries through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Client } = pg;

function getPort() {
  return 26000 + Math.floor(Math.random() * 10000);
}

describe('Prepared Statements / Parameterized Queries', () => {
  let server, port, dir;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-prep-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
    
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    await client.query("CREATE TABLE users (id INT, name TEXT, age INT)");
    await client.query("INSERT INTO users VALUES (1, 'Alice', 30)");
    await client.query("INSERT INTO users VALUES (2, 'Bob', 25)");
    await client.query("INSERT INTO users VALUES (3, 'Charlie', 35)");
    await client.query("INSERT INTO users VALUES (4, 'Diana', 28)");
    await client.end();
  });
  
  after(async () => {
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('SELECT with $1 parameter', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const result = await client.query('SELECT * FROM users WHERE id = $1', [2]);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Bob');
    await client.end();
  });

  it('SELECT with multiple parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    const result = await client.query('SELECT * FROM users WHERE age >= $1 AND age <= $2', [25, 30]);
    console.log('Age 25-30:', result.rows.map(r => r.name));
    assert.equal(result.rows.length, 3); // Alice (30), Bob (25), Diana (28)
    await client.end();
  });

  it('INSERT with parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('INSERT INTO users VALUES ($1, $2, $3)', [5, 'Eve', 22]);
    const result = await client.query('SELECT * FROM users WHERE id = $1', [5]);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Eve');
    assert.equal(String(result.rows[0].age), '22');
    await client.end();
  });

  it('UPDATE with parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('BEGIN');
    await client.query('UPDATE users SET age = $1 WHERE name = $2', [99, 'Alice']);
    const result = await client.query('SELECT age FROM users WHERE name = $1', ['Alice']);
    assert.equal(String(result.rows[0].age), '99');
    await client.query('ROLLBACK');
    
    // Should be back to 30
    const after = await client.query('SELECT age FROM users WHERE name = $1', ['Alice']);
    assert.equal(String(after.rows[0].age), '30');
    await client.end();
  });

  it('DELETE with parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('BEGIN');
    await client.query('DELETE FROM users WHERE id = $1', [5]);
    const count = await client.query('SELECT COUNT(*) as n FROM users');
    console.log('After delete:', count.rows[0].n);
    await client.query('COMMIT');
    await client.end();
  });

  it('string parameter with special characters', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query("INSERT INTO users VALUES ($1, $2, $3)", [6, "O'Malley", 40]);
    const result = await client.query("SELECT name FROM users WHERE id = $1", [6]);
    assert.equal(result.rows[0].name, "O'Malley");
    await client.end();
  });

  it('NULL parameter', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query("INSERT INTO users VALUES ($1, $2, $3)", [7, null, 33]);
    const result = await client.query("SELECT * FROM users WHERE id = $1", [7]);
    assert.equal(result.rows.length, 1);
    console.log('NULL name:', result.rows[0].name);
    await client.end();
  });

  it('repeated same query with different parameters', async () => {
    const client = new Client({ host: '127.0.0.1', port, user: 'test', database: 'test' });
    await client.connect();
    
    for (let id = 1; id <= 4; id++) {
      const result = await client.query('SELECT name FROM users WHERE id = $1', [id]);
      console.log(`id=${id}: ${result.rows[0]?.name}`);
      assert.equal(result.rows.length, 1);
    }
    await client.end();
  });
});
