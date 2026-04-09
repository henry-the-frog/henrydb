// prepared-statements.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { PreparedStatementCache } from './prepared-statements.js';

let db, psc;

describe('PreparedStatementCache', () => {
  beforeEach(() => {
    db = new Database();
    psc = new PreparedStatementCache(db);
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER, active BOOLEAN)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, TRUE)");
    db.execute("INSERT INTO users VALUES (3, 'Carol', 35, FALSE)");
  });

  test('PREPARE and EXECUTE basic query', () => {
    psc.prepare('get_user', 'SELECT * FROM users WHERE id = $1', ['INTEGER']);
    const result = psc.execute('get_user', [1]);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Alice');
  });

  test('EXECUTE with multiple parameters', () => {
    psc.prepare('find_users', 'SELECT * FROM users WHERE age > $1 AND age < $2', ['INTEGER', 'INTEGER']);
    const result = psc.execute('find_users', [24, 31]);
    assert.equal(result.rows.length, 2); // Alice (30), Bob (25)
  });

  test('EXECUTE with string parameter', () => {
    psc.prepare('by_name', "SELECT * FROM users WHERE name = $1", ['TEXT']);
    const result = psc.execute('by_name', ['Bob']);
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].age, 25);
  });

  test('reuse prepared statement multiple times', () => {
    psc.prepare('by_id', 'SELECT * FROM users WHERE id = $1');
    
    const r1 = psc.execute('by_id', [1]);
    assert.equal(r1.rows[0].name, 'Alice');
    
    const r2 = psc.execute('by_id', [2]);
    assert.equal(r2.rows[0].name, 'Bob');
    
    const r3 = psc.execute('by_id', [3]);
    assert.equal(r3.rows[0].name, 'Carol');
    
    const desc = psc.describe('by_id');
    assert.equal(desc.executionCount, 3);
  });

  test('NULL parameter handling', () => {
    psc.prepare('insert_user', "INSERT INTO users VALUES ($1, $2, $3, $4)");
    psc.execute('insert_user', [4, 'Dave', null, true]);
    
    const result = db.execute('SELECT * FROM users WHERE id = 4');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Dave');
  });

  test('DEALLOCATE removes statement', () => {
    psc.prepare('temp', 'SELECT 1');
    assert.ok(psc.has('temp'));
    psc.deallocate('temp');
    assert.ok(!psc.has('temp'));
  });

  test('DEALLOCATE ALL', () => {
    psc.prepare('s1', 'SELECT 1');
    psc.prepare('s2', 'SELECT 2');
    psc.prepare('s3', 'SELECT 3');
    
    const count = psc.deallocate('ALL');
    assert.equal(count, 3);
    assert.equal(psc.list().length, 0);
  });

  test('duplicate PREPARE throws', () => {
    psc.prepare('my_query', 'SELECT 1');
    assert.throws(() => psc.prepare('my_query', 'SELECT 2'), /already exists/);
  });

  test('EXECUTE nonexistent throws', () => {
    assert.throws(() => psc.execute('nonexistent', []), /does not exist/);
  });

  test('insufficient parameters throws', () => {
    psc.prepare('needs_params', 'SELECT * FROM users WHERE id = $1 AND age = $2');
    assert.throws(() => psc.execute('needs_params', [1]), /Expected 2 parameters/);
  });

  test('parameter with special chars', () => {
    psc.prepare('ins', "INSERT INTO users VALUES ($1, $2, $3, $4)");
    psc.execute('ins', [5, "Dave Jr", 40, true]);
    
    const result = db.execute('SELECT * FROM users WHERE id = 5');
    assert.equal(result.rows[0].name, "Dave Jr");
  });

  test('list returns all statements', () => {
    psc.prepare('s1', 'SELECT 1');
    psc.prepare('s2', 'SELECT * FROM users WHERE id = $1');
    
    const list = psc.list();
    assert.equal(list.length, 2);
    assert.ok(list.some(s => s.name === 's1'));
    assert.ok(list.some(s => s.name === 's2'));
  });

  test('describe returns metadata', () => {
    psc.prepare('q', 'SELECT * FROM users WHERE id = $1', ['INTEGER']);
    psc.execute('q', [1]);
    
    const desc = psc.describe('q');
    assert.equal(desc.name, 'q');
    assert.equal(desc.paramCount, 1);
    assert.deepEqual(desc.paramTypes, ['INTEGER']);
    assert.equal(desc.executionCount, 1);
    assert.ok(desc.avgTimeMs >= 0);
  });

  test('LRU eviction at capacity', async () => {
    const small = new PreparedStatementCache(db, { maxStatements: 3 });
    small.prepare('s1', 'SELECT 1');
    await new Promise(r => setTimeout(r, 5));
    small.prepare('s2', 'SELECT 2');
    await new Promise(r => setTimeout(r, 5));
    small.prepare('s3', 'SELECT 3');
    
    // s1 is oldest
    small.prepare('s4', 'SELECT 4'); // should evict s1
    
    assert.ok(!small.has('s1')); // evicted
    assert.ok(small.has('s2'));
    assert.ok(small.has('s3'));
    assert.ok(small.has('s4'));
  });

  test('stats tracking', () => {
    psc.prepare('q', 'SELECT * FROM users WHERE id = $1');
    psc.execute('q', [1]);
    psc.execute('q', [2]);
    psc.deallocate('q');
    
    const stats = psc.getStats();
    assert.equal(stats.prepares, 1);
    assert.equal(stats.executions, 2);
    assert.equal(stats.deallocations, 1);
  });

  test('case-insensitive statement names', () => {
    psc.prepare('MyQuery', 'SELECT 1 as x');
    const result = psc.execute('myquery', []);
    assert.equal(result.rows[0].x, 1);
  });
});
