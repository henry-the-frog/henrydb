// prepared.test.js — Prepared statement tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PreparedStatement, PreparedStatementCache, formatLiteral } from './prepared.js';
import { Database } from './db.js';

describe('PreparedStatement', () => {
  it('counts parameters', () => {
    const stmt = new PreparedStatement('', 'SELECT * FROM t WHERE id = $1');
    assert.strictEqual(stmt.paramCount, 1);
  });

  it('counts multiple parameters', () => {
    const stmt = new PreparedStatement('', 'INSERT INTO t VALUES ($1, $2, $3)');
    assert.strictEqual(stmt.paramCount, 3);
  });

  it('counts no parameters', () => {
    const stmt = new PreparedStatement('', 'SELECT * FROM t');
    assert.strictEqual(stmt.paramCount, 0);
  });

  it('binds integer parameter', () => {
    const stmt = new PreparedStatement('', 'SELECT * FROM t WHERE id = $1');
    const sql = stmt.bind([42]);
    assert.strictEqual(sql, 'SELECT * FROM t WHERE id = 42');
  });

  it('binds string parameter', () => {
    const stmt = new PreparedStatement('', "SELECT * FROM t WHERE name = $1");
    const sql = stmt.bind(['Alice']);
    assert.strictEqual(sql, "SELECT * FROM t WHERE name = 'Alice'");
  });

  it('binds null parameter', () => {
    const stmt = new PreparedStatement('', 'SELECT * FROM t WHERE val = $1');
    const sql = stmt.bind([null]);
    assert.strictEqual(sql, 'SELECT * FROM t WHERE val = NULL');
  });

  it('binds multiple parameters', () => {
    const stmt = new PreparedStatement('', 'INSERT INTO t VALUES ($1, $2, $3)');
    const sql = stmt.bind([1, 'hello', true]);
    assert.strictEqual(sql, "INSERT INTO t VALUES (1, 'hello', TRUE)");
  });

  it('handles parameter reuse', () => {
    const stmt = new PreparedStatement('', 'SELECT * FROM t WHERE a = $1 OR b = $1');
    const sql = stmt.bind([42]);
    assert.strictEqual(sql, 'SELECT * FROM t WHERE a = 42 OR b = 42');
  });

  it('escapes single quotes in strings', () => {
    const stmt = new PreparedStatement('', "SELECT * FROM t WHERE name = $1");
    const sql = stmt.bind(["O'Brien"]);
    assert.strictEqual(sql, "SELECT * FROM t WHERE name = 'O''Brien'");
  });

  it('throws on missing parameters', () => {
    const stmt = new PreparedStatement('', 'SELECT * FROM t WHERE a = $1 AND b = $2');
    assert.throws(() => stmt.bind([1]), /Expected 2 parameters/);
  });

  it('handles high parameter numbers', () => {
    const stmt = new PreparedStatement('', 'SELECT $10, $1');
    assert.strictEqual(stmt.paramCount, 10);
    const sql = stmt.bind([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    assert.strictEqual(sql, 'SELECT 10, 1');
  });
});

describe('PreparedStatementCache', () => {
  it('stores and retrieves statements', () => {
    const cache = new PreparedStatementCache();
    cache.prepare('get_user', 'SELECT * FROM users WHERE id = $1');
    const stmt = cache.get('get_user');
    assert.ok(stmt);
    assert.strictEqual(stmt.paramCount, 1);
  });

  it('binds from cache', () => {
    const cache = new PreparedStatementCache();
    cache.prepare('ins', 'INSERT INTO t VALUES ($1, $2)');
    const sql = cache.bind('ins', [1, 'hello']);
    assert.strictEqual(sql, "INSERT INTO t VALUES (1, 'hello')");
  });

  it('closes statements', () => {
    const cache = new PreparedStatementCache();
    cache.prepare('tmp', 'SELECT 1');
    cache.close('tmp');
    assert.strictEqual(cache.get('tmp'), undefined);
  });

  it('throws on unknown statement', () => {
    const cache = new PreparedStatementCache();
    assert.throws(() => cache.bind('nonexistent', []), /not found/);
  });
});

describe('Integration: prepared statements with Database', () => {
  it('executes prepared SELECT', () => {
    const db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Carol', 35)");
    
    const cache = new PreparedStatementCache();
    cache.prepare('get_by_age', 'SELECT name FROM users WHERE age > $1 ORDER BY name');
    
    const sql1 = cache.bind('get_by_age', [28]);
    const result1 = db.execute(sql1);
    assert.strictEqual(result1.rows.length, 2);
    assert.strictEqual(result1.rows[0].name, 'Alice');
    assert.strictEqual(result1.rows[1].name, 'Carol');
    
    const sql2 = cache.bind('get_by_age', [32]);
    const result2 = db.execute(sql2);
    assert.strictEqual(result2.rows.length, 1);
    assert.strictEqual(result2.rows[0].name, 'Carol');
  });

  it('executes prepared INSERT', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    
    const cache = new PreparedStatementCache();
    cache.prepare('ins', 'INSERT INTO items VALUES ($1, $2)');
    
    for (let i = 0; i < 5; i++) {
      db.execute(cache.bind('ins', [i, `item_${i}`]));
    }
    
    const result = db.execute('SELECT COUNT(*) as cnt FROM items');
    assert.strictEqual(result.rows[0].cnt, 5);
  });
});

describe('formatLiteral', () => {
  it('formats null', () => assert.strictEqual(formatLiteral(null), 'NULL'));
  it('formats number', () => assert.strictEqual(formatLiteral(42), '42'));
  it('formats float', () => assert.strictEqual(formatLiteral(3.14), '3.14'));
  it('formats string', () => assert.strictEqual(formatLiteral('hello'), "'hello'"));
  it('formats boolean', () => assert.strictEqual(formatLiteral(true), 'TRUE'));
  it('escapes quotes', () => assert.strictEqual(formatLiteral("it's"), "'it''s'"));
});
