// fulltext-integration.test.js — Full-text search integration with Database
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Full-text Search Integration', () => {
  function createArticlesDB() {
    const db = new Database();
    db.execute('CREATE TABLE articles (id INT PRIMARY KEY, title TEXT, body TEXT)');
    db.execute("INSERT INTO articles VALUES (1, 'SQL Basics', 'Learn SQL query language for database management')");
    db.execute("INSERT INTO articles VALUES (2, 'Web Dev', 'Modern web development with javascript frameworks')");
    db.execute("INSERT INTO articles VALUES (3, 'DB Design', 'Database schema design and normalization techniques')");
    db.execute("INSERT INTO articles VALUES (4, 'Advanced SQL', 'Advanced SQL techniques including window functions and CTEs')");
    db.execute("INSERT INTO articles VALUES (5, 'Python Guide', 'Python programming language guide for beginners')");
    db.execute('CREATE FULLTEXT INDEX idx_body ON articles(body)');
    return db;
  }

  it('MATCH AGAINST filters rows by text content', () => {
    const db = createArticlesDB();
    const r = db.execute("SELECT id, title FROM articles WHERE MATCH(body) AGAINST('sql')");
    assert.ok(r.rows.length >= 2);
    assert.ok(r.rows.some(row => row.id === 1));
    assert.ok(r.rows.some(row => row.id === 4));
  });

  it('MATCH AGAINST with multiple search terms (AND)', () => {
    const db = createArticlesDB();
    const r = db.execute("SELECT id FROM articles WHERE MATCH(body) AGAINST('sql techniques')");
    // Both "sql" and "techniques" must appear
    assert.ok(r.rows.some(row => row.id === 4)); // "Advanced SQL techniques..."
  });

  it('MATCH AGAINST returns no results for non-matching text', () => {
    const db = createArticlesDB();
    const r = db.execute("SELECT * FROM articles WHERE MATCH(body) AGAINST('blockchain')");
    assert.equal(r.rows.length, 0);
  });

  it('MATCH AGAINST works with ORDER BY', () => {
    const db = createArticlesDB();
    const r = db.execute("SELECT id, title FROM articles WHERE MATCH(body) AGAINST('database') ORDER BY id");
    assert.ok(r.rows.length >= 1);
    // Results should be ordered by id
    for (let i = 1; i < r.rows.length; i++) {
      assert.ok(r.rows[i].id > r.rows[i - 1].id);
    }
  });

  it('MATCH AGAINST with LIMIT', () => {
    const db = createArticlesDB();
    const r = db.execute("SELECT id FROM articles WHERE MATCH(body) AGAINST('sql') LIMIT 1");
    assert.equal(r.rows.length, 1);
  });

  it('fulltext index covers existing data', () => {
    const db = new Database();
    db.execute('CREATE TABLE docs (id INT PRIMARY KEY, content TEXT)');
    db.execute("INSERT INTO docs VALUES (1, 'hello world')");
    db.execute("INSERT INTO docs VALUES (2, 'goodbye world')");
    
    // Create index AFTER inserting data
    db.execute('CREATE FULLTEXT INDEX idx_content ON docs(content)');
    
    const r = db.execute("SELECT * FROM docs WHERE MATCH(content) AGAINST('hello')");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].id, 1);
  });

  it('MATCH AGAINST combined with other WHERE conditions', () => {
    const db = createArticlesDB();
    const r = db.execute("SELECT id FROM articles WHERE MATCH(body) AGAINST('sql') AND id > 2");
    assert.ok(r.rows.length >= 1);
    assert.ok(r.rows.every(row => row.id > 2));
  });
});
