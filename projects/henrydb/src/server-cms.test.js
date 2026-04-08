// server-cms.test.js — Content Management System pattern
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15534;

describe('Content Management System', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE authors (id INTEGER, name TEXT, bio TEXT)');
    await client.query('CREATE TABLE categories (id INTEGER, name TEXT, parent_id INTEGER)');
    await client.query('CREATE TABLE cms_posts (id INTEGER, author_id INTEGER, category_id INTEGER, title TEXT, slug TEXT, body TEXT, status TEXT, published_at TEXT)');
    await client.query('CREATE TABLE tags (id INTEGER, name TEXT)');
    await client.query('CREATE TABLE post_tags (post_id INTEGER, tag_id INTEGER)');
    
    // Authors
    await client.query("INSERT INTO authors VALUES (1, 'Alice Author', 'Tech writer')");
    await client.query("INSERT INTO authors VALUES (2, 'Bob Blogger', 'Lifestyle blogger')");
    
    // Categories (hierarchical)
    await client.query("INSERT INTO categories VALUES (1, 'Technology', NULL)");
    await client.query("INSERT INTO categories VALUES (2, 'Programming', 1)");
    await client.query("INSERT INTO categories VALUES (3, 'Databases', 2)");
    await client.query("INSERT INTO categories VALUES (4, 'Lifestyle', NULL)");
    
    // Posts
    await client.query("INSERT INTO cms_posts VALUES (1, 1, 3, 'Building a Database', 'building-database', 'How to build a database engine...', 'published', '2026-04-01')");
    await client.query("INSERT INTO cms_posts VALUES (2, 1, 2, 'JavaScript Tips', 'js-tips', 'Top 10 JS tips...', 'published', '2026-04-02')");
    await client.query("INSERT INTO cms_posts VALUES (3, 2, 4, 'Morning Routine', 'morning-routine', 'My productive morning...', 'published', '2026-04-03')");
    await client.query("INSERT INTO cms_posts VALUES (4, 1, 3, 'SQL Optimization', 'sql-optimization', 'How to optimize queries...', 'draft', NULL)");
    
    // Tags
    await client.query("INSERT INTO tags VALUES (1, 'tutorial')");
    await client.query("INSERT INTO tags VALUES (2, 'beginner')");
    await client.query("INSERT INTO tags VALUES (3, 'advanced')");
    
    // Post-tags
    await client.query('INSERT INTO post_tags VALUES (1, 1)');
    await client.query('INSERT INTO post_tags VALUES (1, 3)');
    await client.query('INSERT INTO post_tags VALUES (2, 1)');
    await client.query('INSERT INTO post_tags VALUES (2, 2)');
    await client.query('INSERT INTO post_tags VALUES (3, 2)');
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('list published posts with author', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      "SELECT p.title, a.name AS author, p.published_at FROM cms_posts p JOIN authors a ON p.author_id = a.id WHERE p.status = 'published' ORDER BY p.published_at DESC"
    );
    assert.strictEqual(result.rows.length, 3);
    assert.strictEqual(result.rows[0].title, 'Morning Routine'); // Most recent

    await client.end();
  });

  it('posts by category with hierarchy', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.title, c.name AS category FROM cms_posts p JOIN categories c ON p.category_id = c.id ORDER BY c.name'
    );
    assert.ok(result.rows.length >= 3);

    await client.end();
  });

  it('posts with tags', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT p.title, t.name AS tag FROM cms_posts p JOIN post_tags pt ON p.id = pt.post_id JOIN tags t ON pt.tag_id = t.id ORDER BY p.title, t.name'
    );
    assert.ok(result.rows.length >= 5);

    await client.end();
  });

  it('author statistics', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT a.name, COUNT(p.id) AS post_count FROM authors a JOIN cms_posts p ON a.id = p.author_id GROUP BY a.name ORDER BY post_count DESC'
    );
    assert.ok(result.rows.length >= 2);
    assert.strictEqual(result.rows[0].name, 'Alice Author'); // Most posts

    await client.end();
  });

  it('find by slug', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT title, body FROM cms_posts WHERE slug = 'building-database'");
    assert.strictEqual(result.rows.length, 1);
    assert.strictEqual(result.rows[0].title, 'Building a Database');

    await client.end();
  });

  it('popular tags', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT t.name, COUNT(pt.post_id) AS usage FROM tags t JOIN post_tags pt ON t.id = pt.tag_id GROUP BY t.name ORDER BY usage DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });
});
