// server-fulltext.test.js — Full-text search through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15513;

describe('Full-Text Search', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE articles (id INTEGER, title TEXT, body TEXT, category TEXT)');
    await client.query("INSERT INTO articles VALUES (1, 'Introduction to Databases', 'Databases store data efficiently using tables and indexes', 'tech')");
    await client.query("INSERT INTO articles VALUES (2, 'PostgreSQL vs MySQL', 'PostgreSQL offers advanced features like JSON support and full-text search', 'tech')");
    await client.query("INSERT INTO articles VALUES (3, 'Cooking with Fire', 'Learn to cook amazing dishes using fire and traditional methods', 'food')");
    await client.query("INSERT INTO articles VALUES (4, 'Database Performance Tuning', 'Optimize your database queries with indexes and caching strategies', 'tech')");
    await client.query("INSERT INTO articles VALUES (5, 'Garden Design', 'Beautiful garden designs using flowers and stones', 'lifestyle')");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('LIKE search on title', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT * FROM articles WHERE title LIKE '%Database%'");
    assert.ok(result.rows.length >= 2); // Articles 1 and 4

    await client.end();
  });

  it('LIKE search on body', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT * FROM articles WHERE body LIKE '%index%'");
    assert.ok(result.rows.length >= 1);

    await client.end();
  });

  it('case-insensitive search with LOWER', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT * FROM articles WHERE LOWER(title) LIKE '%database%'");
    assert.ok(result.rows.length >= 2);

    await client.end();
  });

  it('multi-column search', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT * FROM articles WHERE title LIKE '%fire%' OR body LIKE '%fire%'"
    );
    assert.ok(result.rows.length >= 1);
    assert.ok(result.rows.some(r => r.title === 'Cooking with Fire'));

    await client.end();
  });

  it('search with category filter', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT * FROM articles WHERE category = 'tech' AND body LIKE '%JSON%'"
    );
    assert.strictEqual(result.rows.length, 1);
    assert.ok(result.rows[0].title.includes('PostgreSQL'));

    await client.end();
  });

  it('search result ranking by relevance (manual)', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    // Count keyword occurrences as a simple relevance score
    const result = await client.query(`
      SELECT title, 
             (LENGTH(body) - LENGTH(REPLACE(LOWER(body), 'database', ''))) / 8 AS relevance
      FROM articles 
      WHERE LOWER(body) LIKE '%database%'
      ORDER BY relevance DESC
    `);
    assert.ok(result.rows.length >= 1);

    await client.end();
  });
});
