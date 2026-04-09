// fulltext-sql.test.js — Tests for full-text search SQL integration
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Full-text search SQL integration', () => {
  function setupDB() {
    const db = new Database();
    db.execute('CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, body TEXT)');
    db.execute("INSERT INTO articles VALUES (1, 'Database Internals', 'B-trees buffer pools and storage engines in modern databases')");
    db.execute("INSERT INTO articles VALUES (2, 'Web Development Guide', 'Building web applications with JavaScript React and Node.js')");
    db.execute("INSERT INTO articles VALUES (3, 'Machine Learning Basics', 'Neural networks deep learning and AI for computer science')");
    db.execute("INSERT INTO articles VALUES (4, 'Advanced Database Design', 'Query optimization indexes and database performance tuning')");
    db.execute("INSERT INTO articles VALUES (5, 'JavaScript Patterns', 'Design patterns and best practices for JavaScript developers')");
    db.execute('CREATE FULLTEXT INDEX idx_body ON articles(body)');
    return db;
  }

  it('MATCH AGAINST basic search', () => {
    const db = setupDB();
    const result = db.execute("SELECT title FROM articles WHERE MATCH(body) AGAINST('database')");
    assert.ok(result.rows.length >= 1);
    const titles = result.rows.map(r => r.title);
    assert.ok(titles.includes('Database Internals') || titles.includes('Advanced Database Design'));
  });

  it('MATCH AGAINST multi-word search (AND)', () => {
    const db = setupDB();
    const result = db.execute("SELECT title FROM articles WHERE MATCH(body) AGAINST('database optimization')");
    const titles = result.rows.map(r => r.title);
    // Should match doc 4 which has both "database" and "optimization"
    assert.ok(titles.includes('Advanced Database Design'));
  });

  it('MATCH AGAINST returns no results for non-matching query', () => {
    const db = setupDB();
    const result = db.execute("SELECT * FROM articles WHERE MATCH(body) AGAINST('quantum physics')");
    assert.equal(result.rows.length, 0);
  });

  it('MATCH AGAINST with stemming', () => {
    const db = setupDB();
    // "databases" in the body should match "database" in the query (stemming)
    const result = db.execute("SELECT title FROM articles WHERE MATCH(body) AGAINST('database')");
    assert.ok(result.rows.length > 0);
  });

  it('full-text search with other WHERE conditions', () => {
    const db = setupDB();
    const result = db.execute("SELECT * FROM articles WHERE MATCH(body) AGAINST('javascript') AND id > 2");
    assert.ok(result.rows.length >= 1);
    for (const row of result.rows) {
      assert.ok(row.id > 2);
    }
  });

  it('full-text search with SELECT specific columns', () => {
    const db = setupDB();
    const result = db.execute("SELECT id, title FROM articles WHERE MATCH(body) AGAINST('learning')");
    assert.ok(result.rows.length >= 1);
    assert.ok(result.rows[0].id);
    assert.ok(result.rows[0].title);
  });

  it('full-text search with ORDER BY and LIMIT', () => {
    const db = setupDB();
    const result = db.execute("SELECT * FROM articles WHERE MATCH(body) AGAINST('database') ORDER BY id ASC LIMIT 1");
    assert.equal(result.rows.length, 1);
  });

  it('full-text search with COUNT', () => {
    const db = setupDB();
    const result = db.execute("SELECT COUNT(*) as cnt FROM articles WHERE MATCH(body) AGAINST('javascript')");
    assert.ok(result.rows[0].cnt >= 1);
  });

  it('CREATE FULLTEXT INDEX on existing data', () => {
    const db = new Database();
    db.execute('CREATE TABLE docs (id INTEGER PRIMARY KEY, content TEXT)');
    for (let i = 1; i <= 100; i++) {
      const topic = ['programming', 'cooking', 'sports'][i % 3];
      db.execute(`INSERT INTO docs VALUES (${i}, 'Article about ${topic} topic number ${i}')`);
    }
    
    db.execute('CREATE FULLTEXT INDEX idx_content ON docs(content)');
    
    const result = db.execute("SELECT COUNT(*) as cnt FROM docs WHERE MATCH(content) AGAINST('programming')");
    assert.ok(result.rows[0].cnt > 0);
  });

  it('full-text search on BTreeTable', () => {
    const db = new Database();
    db.execute('CREATE TABLE posts (id INTEGER PRIMARY KEY, content TEXT) USING BTREE');
    db.execute("INSERT INTO posts VALUES (1, 'Rust programming language systems')");
    db.execute("INSERT INTO posts VALUES (2, 'Python data science machine learning')");
    db.execute("INSERT INTO posts VALUES (3, 'Rust cargo package manager')");
    
    db.execute('CREATE FULLTEXT INDEX idx_content ON posts(content)');
    
    const result = db.execute("SELECT * FROM posts WHERE MATCH(content) AGAINST('rust')");
    assert.equal(result.rows.length, 2);
  });

  it('error: MATCH on column without fulltext index', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'test')");
    
    assert.throws(() => {
      db.execute("SELECT * FROM t WHERE MATCH(name) AGAINST('test')");
    }, /fulltext index/i);
  });

  it('stress: 1000 documents, search performance', () => {
    const db = new Database();
    db.execute('CREATE TABLE big_docs (id INTEGER PRIMARY KEY, content TEXT)');
    
    const topics = ['database', 'algorithms', 'networking', 'security', 'systems'];
    for (let i = 1; i <= 1000; i++) {
      const t1 = topics[i % 5];
      const t2 = topics[(i + 2) % 5];
      db.execute(`INSERT INTO big_docs VALUES (${i}, 'Document ${i} about ${t1} and ${t2} in computer science')`);
    }
    
    db.execute('CREATE FULLTEXT INDEX idx_content ON big_docs(content)');
    
    const t0 = performance.now();
    for (let i = 0; i < 100; i++) {
      db.execute("SELECT * FROM big_docs WHERE MATCH(content) AGAINST('database algorithms')");
    }
    const elapsed = performance.now() - t0;
    
    console.log(`  100 FTS queries on 1K docs: ${elapsed.toFixed(1)}ms (${(elapsed/100).toFixed(2)}ms avg)`);
    assert.ok(elapsed < 10000);
  });
});
