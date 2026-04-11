// real-app-explore.test.js — Test HenryDB as a real app backend
// Creates a todo list API and exercises it with realistic CRUD operations
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HenryDBServer } from './server.js';

const { Pool, Client } = pg;

function getPort() {
  return 36000 + Math.floor(Math.random() * 10000);
}

describe('Real App: Todo List API', () => {
  let server, port, dir, pool;
  
  before(async () => {
    port = getPort();
    dir = mkdtempSync(join(tmpdir(), 'henrydb-app-'));
    server = new HenryDBServer({ port, dataDir: dir, transactional: true });
    await server.start();
    
    pool = new Pool({ host: '127.0.0.1', port, user: 'test', database: 'test', max: 5 });
    
    // Schema setup
    const client = await pool.connect();
    await client.query(`
      CREATE TABLE todos (
        id INT PRIMARY KEY,
        title TEXT,
        description TEXT,
        completed INT DEFAULT 0,
        created_at TEXT DEFAULT 'now',
        priority INT DEFAULT 0
      )
    `);
    await client.query(`
      CREATE TABLE tags (
        id INT PRIMARY KEY,
        name TEXT
      )
    `);
    await client.query(`
      CREATE TABLE todo_tags (
        todo_id INT,
        tag_id INT
      )
    `);
    client.release();
  });
  
  after(async () => {
    if (pool) await pool.end();
    if (server) await server.stop();
    if (dir) rmSync(dir, { recursive: true });
  });

  it('CREATE todo', async () => {
    const client = await pool.connect();
    await client.query('INSERT INTO todos VALUES ($1, $2, $3, $4, $5, $6)', 
      [1, 'Buy groceries', 'Milk, eggs, bread', 0, '2026-04-10', 1]);
    await client.query('INSERT INTO todos VALUES ($1, $2, $3, $4, $5, $6)',
      [2, 'Fix database bug', 'Query cache rollback issue', 0, '2026-04-10', 3]);
    await client.query('INSERT INTO todos VALUES ($1, $2, $3, $4, $5, $6)',
      [3, 'Write blog post', 'About TPC-B benchmarks', 0, '2026-04-10', 2]);
    
    const count = await client.query('SELECT COUNT(*) as n FROM todos');
    assert.equal(String(count.rows[0].n), '3');
    client.release();
  });

  it('READ all todos', async () => {
    const client = await pool.connect();
    const result = await client.query('SELECT * FROM todos ORDER BY priority DESC');
    assert.equal(result.rows.length, 3);
    assert.equal(result.rows[0].title, 'Fix database bug'); // highest priority
    client.release();
  });

  it('READ single todo by ID', async () => {
    const client = await pool.connect();
    const result = await client.query('SELECT * FROM todos WHERE id = $1', [2]);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].title, 'Fix database bug');
    client.release();
  });

  it('UPDATE todo (mark complete)', async () => {
    const client = await pool.connect();
    await client.query('BEGIN');
    await client.query('UPDATE todos SET completed = 1 WHERE id = $1', [2]);
    await client.query('COMMIT');
    
    const result = await client.query('SELECT completed FROM todos WHERE id = $1', [2]);
    assert.equal(String(result.rows[0].completed), '1');
    client.release();
  });

  it('DELETE todo', async () => {
    const client = await pool.connect();
    await client.query('DELETE FROM todos WHERE id = $1', [3]);
    
    const count = await client.query('SELECT COUNT(*) as n FROM todos');
    assert.equal(String(count.rows[0].n), '2');
    client.release();
  });

  it('filter by status', async () => {
    const client = await pool.connect();
    const done = await client.query('SELECT * FROM todos WHERE completed = 1');
    assert.equal(done.rows.length, 1);
    assert.equal(done.rows[0].title, 'Fix database bug');
    
    const pending = await client.query('SELECT * FROM todos WHERE completed = 0');
    assert.equal(pending.rows.length, 1);
    assert.equal(pending.rows[0].title, 'Buy groceries');
    client.release();
  });

  it('search by title (LIKE)', async () => {
    const client = await pool.connect();
    const result = await client.query("SELECT * FROM todos WHERE title LIKE '%groc%'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].title, 'Buy groceries');
    client.release();
  });

  it('tag a todo (many-to-many)', async () => {
    const client = await pool.connect();
    
    // Create tags
    await client.query('INSERT INTO tags VALUES (1, $1)', ['work']);
    await client.query('INSERT INTO tags VALUES (2, $1)', ['shopping']);
    await client.query('INSERT INTO tags VALUES (3, $1)', ['writing']);
    
    // Tag todos
    await client.query('INSERT INTO todo_tags VALUES ($1, $2)', [1, 2]); // groceries → shopping
    await client.query('INSERT INTO todo_tags VALUES ($1, $2)', [2, 1]); // bug → work
    
    // Query with JOIN
    const result = await client.query(`
      SELECT t.title, g.name as tag
      FROM todos t
      JOIN todo_tags tt ON t.id = tt.todo_id
      JOIN tags g ON tt.tag_id = g.id
      ORDER BY t.title
    `);
    
    console.log('Tagged todos:', result.rows);
    assert.equal(result.rows.length, 2);
    
    client.release();
  });

  it('aggregate stats', async () => {
    const client = await pool.connect();
    
    const stats = await client.query(`
      SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN completed = 1 THEN 1 ELSE 0 END) as done,
        SUM(CASE WHEN completed = 0 THEN 1 ELSE 0 END) as pending,
        AVG(priority) as avg_priority
      FROM todos
    `);
    
    console.log('Stats:', stats.rows[0]);
    assert.equal(String(stats.rows[0].total), '2');
    assert.equal(String(stats.rows[0].done), '1');
    assert.equal(String(stats.rows[0].pending), '1');
    
    client.release();
  });

  it('connection pool (concurrent requests)', async () => {
    // Simulate 10 concurrent reads
    const promises = [];
    for (let i = 0; i < 10; i++) {
      promises.push(pool.query('SELECT * FROM todos'));
    }
    
    const results = await Promise.all(promises);
    for (const r of results) {
      assert.equal(r.rows.length, 2);
    }
    console.log('10 concurrent reads: ✅');
  });

  it('transaction rollback (error recovery)', async () => {
    const client = await pool.connect();
    
    // Save current state
    const before = await client.query('SELECT COUNT(*) as n FROM todos');
    
    // Start transaction, insert, then rollback
    await client.query('BEGIN');
    await client.query('INSERT INTO todos VALUES (99, $1, $2, 0, $3, 0)', ['temp', 'will be rolled back', 'now']);
    
    const during = await client.query('SELECT COUNT(*) as n FROM todos');
    assert.equal(parseInt(String(during.rows[0].n)), parseInt(String(before.rows[0].n)) + 1);
    
    await client.query('ROLLBACK');
    
    const after = await client.query('SELECT COUNT(*) as n FROM todos');
    assert.equal(String(after.rows[0].n), String(before.rows[0].n));
    
    client.release();
  });
});
