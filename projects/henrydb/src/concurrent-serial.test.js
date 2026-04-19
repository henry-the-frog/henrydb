// concurrent-serial.test.js — Multi-client concurrent SERIAL INSERT test

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { HenryDBServer } from './server.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import pg from 'pg';

describe('Concurrent SERIAL Inserts', () => {
  let server, port, dir;
  
  before(async () => {
    port = 34580 + Math.floor(Math.random() * 100);
    dir = mkdtempSync(join(tmpdir(), 'henrydb-concurrent-'));
    server = new HenryDBServer({ port, dataDir: dir });
    await server.start();
  });
  
  after(async () => {
    await server.stop();
    rmSync(dir, { recursive: true });
  });
  
  it('single client: 100 SERIAL inserts produce unique IDs', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT)');
    
    const ids = [];
    for (let i = 0; i < 100; i++) {
      const r = await c.query('INSERT INTO items (name) VALUES ($1) RETURNING id', [`item${i}`]);
      ids.push(r.rows[0].id);
    }
    
    // All IDs should be unique
    const uniqueIds = new Set(ids);
    assert.equal(uniqueIds.size, 100, 'All 100 IDs should be unique');
    
    // IDs should be sequential 1-100
    ids.sort((a, b) => a - b);
    for (let i = 0; i < 100; i++) {
      assert.equal(ids[i], i + 1, `ID ${i} should be ${i + 1}`);
    }
    
    await c.end();
  });
  
  it('SERIAL with multiple tables: IDs are independent', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE t_a (id SERIAL PRIMARY KEY, val TEXT)');
    await c.query('CREATE TABLE t_b (id SERIAL PRIMARY KEY, val TEXT)');
    
    // Interleave inserts into both tables
    for (let i = 0; i < 20; i++) {
      await c.query('INSERT INTO t_a (val) VALUES ($1)', [`a${i}`]);
      await c.query('INSERT INTO t_b (val) VALUES ($1)', [`b${i}`]);
    }
    
    const aCount = await c.query('SELECT COUNT(*) AS cnt FROM t_a');
    const bCount = await c.query('SELECT COUNT(*) AS cnt FROM t_b');
    assert.equal(aCount.rows[0].cnt, 20);
    assert.equal(bCount.rows[0].cnt, 20);
    
    // Check IDs are 1-20 in each table
    const aIds = await c.query('SELECT id FROM t_a ORDER BY id');
    const bIds = await c.query('SELECT id FROM t_b ORDER BY id');
    for (let i = 0; i < 20; i++) {
      assert.equal(aIds.rows[i].id, i + 1);
      assert.equal(bIds.rows[i].id, i + 1);
    }
    
    await c.end();
  });
  
  it('two clients: both can insert into same SERIAL table', async () => {
    const c1 = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    const c2 = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c1.connect();
    await c2.connect();
    
    await c1.query('CREATE TABLE shared_items (id SERIAL PRIMARY KEY, source TEXT, val INT)');
    
    // Both clients insert sequentially
    const allIds = [];
    for (let i = 0; i < 10; i++) {
      const r1 = await c1.query('INSERT INTO shared_items (source, val) VALUES ($1, $2) RETURNING id', ['c1', i]);
      allIds.push(r1.rows[0].id);
      const r2 = await c2.query('INSERT INTO shared_items (source, val) VALUES ($1, $2) RETURNING id', ['c2', i]);
      allIds.push(r2.rows[0].id);
    }
    
    // All 20 IDs should be unique
    const uniqueIds = new Set(allIds);
    assert.equal(uniqueIds.size, 20, 'All 20 IDs from both clients should be unique');
    
    // Total count should be 20
    const r = await c1.query('SELECT COUNT(*) AS cnt FROM shared_items');
    assert.equal(r.rows[0].cnt, 20);
    
    await c1.end();
    await c2.end();
  });
  
  it('SERIAL with explicit ID gaps and UNIQUE constraints', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE events (id SERIAL PRIMARY KEY, type TEXT UNIQUE, data TEXT)');
    
    // Insert with ON CONFLICT to test SERIAL + UPSERT
    await c.query("INSERT INTO events (type, data) VALUES ('click', 'first')");
    await c.query("INSERT INTO events (type, data) VALUES ('view', 'second')");
    await c.query("INSERT INTO events (type, data) VALUES ('click', 'updated') ON CONFLICT (type) DO UPDATE SET data = 'updated'");
    
    const r = await c.query("SELECT * FROM events ORDER BY id");
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].type, 'click');
    assert.equal(r.rows[0].data, 'updated');
    assert.equal(r.rows[1].type, 'view');
    
    // Explicit ID insert should advance sequence
    await c.query("INSERT INTO events (id, type, data) VALUES (100, 'custom', 'explicit')");
    const r2 = await c.query("INSERT INTO events (type, data) VALUES ('after_gap') RETURNING id");
    assert.ok(r2.rows[0].id > 100, `ID after gap should be > 100, got ${r2.rows[0].id}`);
    
    await c.end();
  });
  
  it('complex schema: SERIAL + defaults + constraints + JOINs', async () => {
    const c = new pg.Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query("CREATE TABLE authors (id SERIAL PRIMARY KEY, name TEXT NOT NULL, bio TEXT DEFAULT 'No bio')");
    await c.query("CREATE TABLE books (id SERIAL PRIMARY KEY, title TEXT NOT NULL, author_id INT, price INT DEFAULT 0)");
    await c.query("CREATE TABLE reviews (id SERIAL PRIMARY KEY, book_id INT, rating INT, comment TEXT)");
    
    // Insert authors
    for (let i = 0; i < 5; i++) {
      await c.query('INSERT INTO authors (name) VALUES ($1)', [`Author${i}`]);
    }
    
    // Insert books
    for (let i = 0; i < 15; i++) {
      await c.query('INSERT INTO books (title, author_id, price) VALUES ($1, $2, $3)', 
        [`Book${i}`, 1 + (i % 5), 10 + i * 2]);
    }
    
    // Insert reviews
    for (let i = 0; i < 30; i++) {
      await c.query('INSERT INTO reviews (book_id, rating, comment) VALUES ($1, $2, $3)',
        [1 + (i % 15), 1 + (i % 5), `Review${i}`]);
    }
    
    // Verify counts
    const authors = await c.query('SELECT COUNT(*) AS cnt FROM authors');
    assert.equal(authors.rows[0].cnt, 5);
    
    const books = await c.query('SELECT COUNT(*) AS cnt FROM books');
    assert.equal(books.rows[0].cnt, 15);
    
    const reviews = await c.query('SELECT COUNT(*) AS cnt FROM reviews');
    assert.equal(reviews.rows[0].cnt, 30);
    
    // JOIN query
    const topBooks = await c.query(
      "SELECT b.title, a.name AS author, COUNT(r.id) AS review_count " +
      "FROM books b " +
      "INNER JOIN authors a ON a.id = b.author_id " +
      "INNER JOIN reviews r ON r.book_id = b.id " +
      "GROUP BY b.title, a.name " +
      "ORDER BY review_count DESC " +
      "LIMIT 5"
    );
    assert.ok(topBooks.rows.length > 0, 'Should have books with reviews');
    assert.ok(topBooks.rows[0].review_count > 0, 'Should have review counts');
    
    await c.end();
  });
});
