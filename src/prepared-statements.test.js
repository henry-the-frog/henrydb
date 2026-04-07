// prepared-statements.test.js — Tests for prepared statements
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { PreparedStatementManager } from './prepared-statements.js';

describe('Prepared Statements', () => {
  let db, psm;

  beforeEach(() => {
    db = new Database();
    psm = new PreparedStatementManager();
    db.execute('CREATE TABLE users (id INT, name TEXT, age INT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
  });

  // ===== PREPARE =====

  it('PREPARE creates a named statement', () => {
    const result = psm.prepare('get_user', 'SELECT * FROM users WHERE id = $1');
    assert.equal(result.message, 'PREPARE');
    assert.ok(psm.has('get_user'));
  });

  it('PREPARE rejects duplicate name', () => {
    psm.prepare('q1', 'SELECT 1');
    assert.throws(() => psm.prepare('q1', 'SELECT 2'), /already exists/);
  });

  it('PREPARE counts parameters correctly', () => {
    psm.prepare('q1', 'SELECT * FROM users WHERE age > $1 AND name = $2');
    assert.equal(psm.get('q1').paramCount, 2);
  });

  // ===== EXECUTE =====

  it('EXECUTE substitutes parameters and returns SQL', () => {
    psm.prepare('get_user', 'SELECT * FROM users WHERE id = $1');
    const sql = psm.execute('get_user', [42]);
    assert.equal(sql, 'SELECT * FROM users WHERE id = 42');
  });

  it('EXECUTE with string parameters escapes quotes', () => {
    psm.prepare('find_name', "SELECT * FROM users WHERE name = $1");
    const sql = psm.execute('find_name', ['Alice']);
    assert.equal(sql, "SELECT * FROM users WHERE name = 'Alice'");
  });

  it('EXECUTE with multiple parameters', () => {
    psm.prepare('range', 'SELECT * FROM users WHERE age > $1 AND age < $2');
    const sql = psm.execute('range', [25, 35]);
    assert.equal(sql, 'SELECT * FROM users WHERE age > 25 AND age < 35');
  });

  it('EXECUTE runs correctly through Database', () => {
    psm.prepare('get_user', 'SELECT name FROM users WHERE id = $1');
    const sql = psm.execute('get_user', [2]);
    const result = db.execute(sql);
    assert.equal(result.rows[0].name, 'Bob');
  });

  it('EXECUTE with different parameters each time', () => {
    psm.prepare('get_user', 'SELECT name FROM users WHERE id = $1');
    
    const sql1 = psm.execute('get_user', [1]);
    assert.equal(db.execute(sql1).rows[0].name, 'Alice');
    
    const sql2 = psm.execute('get_user', [3]);
    assert.equal(db.execute(sql2).rows[0].name, 'Charlie');
  });

  it('EXECUTE rejects non-existent statement', () => {
    assert.throws(() => psm.execute('nonexistent', []), /does not exist/);
  });

  it('EXECUTE rejects insufficient parameters', () => {
    psm.prepare('q', 'SELECT * FROM users WHERE id = $1 AND age = $2');
    assert.throws(() => psm.execute('q', [1]), /Expected 2 parameters/);
  });

  // ===== DEALLOCATE =====

  it('DEALLOCATE removes a statement', () => {
    psm.prepare('q1', 'SELECT 1');
    psm.deallocate('q1');
    assert.ok(!psm.has('q1'));
  });

  it('DEALLOCATE ALL removes all statements', () => {
    psm.prepare('q1', 'SELECT 1');
    psm.prepare('q2', 'SELECT 2');
    psm.deallocate('ALL');
    assert.ok(!psm.has('q1'));
    assert.ok(!psm.has('q2'));
  });

  // ===== SQL parsing =====

  it('parseCommand recognizes PREPARE', () => {
    const cmd = PreparedStatementManager.parseCommand("PREPARE get_user (INT) AS SELECT * FROM users WHERE id = $1");
    assert.equal(cmd.type, 'PREPARE');
    assert.equal(cmd.name, 'get_user');
    assert.deepEqual(cmd.paramTypes, ['INT']);
    assert.ok(cmd.sql.includes('SELECT'));
  });

  it('parseCommand recognizes PREPARE without types', () => {
    const cmd = PreparedStatementManager.parseCommand("PREPARE q1 AS SELECT 1");
    assert.equal(cmd.type, 'PREPARE');
    assert.equal(cmd.name, 'q1');
    assert.deepEqual(cmd.paramTypes, []);
  });

  it('parseCommand recognizes EXECUTE', () => {
    const cmd = PreparedStatementManager.parseCommand("EXECUTE get_user(42)");
    assert.equal(cmd.type, 'EXECUTE');
    assert.equal(cmd.name, 'get_user');
    assert.deepEqual(cmd.params, [42]);
  });

  it('parseCommand recognizes EXECUTE with string params', () => {
    const cmd = PreparedStatementManager.parseCommand("EXECUTE find_name('Alice', 25)");
    assert.equal(cmd.type, 'EXECUTE');
    assert.deepEqual(cmd.params, ['Alice', 25]);
  });

  it('parseCommand recognizes DEALLOCATE', () => {
    const cmd = PreparedStatementManager.parseCommand("DEALLOCATE get_user");
    assert.equal(cmd.type, 'DEALLOCATE');
    assert.equal(cmd.name, 'get_user');
  });

  it('parseCommand recognizes DEALLOCATE PREPARE', () => {
    const cmd = PreparedStatementManager.parseCommand("DEALLOCATE PREPARE q1");
    assert.equal(cmd.type, 'DEALLOCATE');
    assert.equal(cmd.name, 'q1');
  });

  it('parseCommand recognizes DEALLOCATE ALL', () => {
    const cmd = PreparedStatementManager.parseCommand("DEALLOCATE ALL");
    assert.equal(cmd.type, 'DEALLOCATE');
    assert.equal(cmd.name, 'ALL');
  });

  it('parseCommand returns null for non-prepared commands', () => {
    assert.equal(PreparedStatementManager.parseCommand("SELECT * FROM users"), null);
    assert.equal(PreparedStatementManager.parseCommand("INSERT INTO t VALUES (1)"), null);
  });

  // ===== End-to-end =====

  it('full PREPARE/EXECUTE/DEALLOCATE cycle', () => {
    psm.prepare('ins', "INSERT INTO users VALUES ($1, $2, $3)");
    
    const sql1 = psm.execute('ins', [4, 'Diana', 28]);
    db.execute(sql1);
    
    const sql2 = psm.execute('ins', [5, 'Eve', 22]);
    db.execute(sql2);
    
    const result = db.execute('SELECT * FROM users ORDER BY id');
    assert.equal(result.rows.length, 5);
    assert.equal(result.rows[3].name, 'Diana');
    assert.equal(result.rows[4].name, 'Eve');
    
    psm.deallocate('ins');
    assert.ok(!psm.has('ins'));
  });

  it('handles SQL injection attempt via parameters', () => {
    psm.prepare('find', "SELECT * FROM users WHERE name = $1");
    const sql = psm.execute('find', ["'; DROP TABLE users; --"]);
    // The parameter should be properly quoted
    assert.ok(sql.includes("''"));
    // Execute should work without dropping the table
    db.execute(sql);
    const result = db.execute('SELECT COUNT(*) as cnt FROM users');
    assert.equal(result.rows[0].cnt, 3); // Table still exists with 3 rows
  });
});
