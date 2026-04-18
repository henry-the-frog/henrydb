import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

function query(db, sql) {
  const r = db.execute(sql);
  return r && r.rows ? r.rows : r;
}

describe('pg_stat_statements', () => {
  
  it('should track query execution counts', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
    db.execute("INSERT INTO t VALUES (3, 'Charlie')");
    
    const stats = query(db, 'SELECT * FROM pg_stat_statements ORDER BY calls DESC');
    assert.ok(stats.length > 0, 'Should have stats entries');
    
    // Find the INSERT entries — they should be normalized to one entry
    const insertStats = stats.filter(s => s.query.includes('INSERT'));
    assert.ok(insertStats.length > 0, 'Should have INSERT stats');
  });

  it('should normalize numeric literals to $?', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER, val INTEGER)');
    db.execute('INSERT INTO nums VALUES (1, 100)');
    db.execute('INSERT INTO nums VALUES (2, 200)');
    db.execute('INSERT INTO nums VALUES (3, 300)');
    
    const stats = query(db, 'SELECT * FROM pg_stat_statements');
    const insertStats = stats.filter(s => s.query.includes('INSERT INTO nums'));
    
    // All 3 INSERTs should be grouped into 1 normalized entry
    assert.equal(insertStats.length, 1, 'Should have exactly one normalized INSERT entry');
    assert.equal(insertStats[0].calls, 3, 'Should show 3 calls');
    assert.equal(insertStats[0].query, 'INSERT INTO nums VALUES ($?, $?)');
  });

  it('should normalize string literals to $?', () => {
    const db = new Database();
    db.execute('CREATE TABLE strings (id INTEGER, name TEXT)');
    db.execute("INSERT INTO strings VALUES (1, 'Alice')");
    db.execute("INSERT INTO strings VALUES (2, 'Bob')");
    
    const stats = query(db, 'SELECT * FROM pg_stat_statements');
    const insertStats = stats.filter(s => s.query.includes('INSERT INTO strings'));
    
    assert.equal(insertStats.length, 1);
    assert.equal(insertStats[0].calls, 2);
    assert.equal(insertStats[0].query, 'INSERT INTO strings VALUES ($?, $?)');
  });

  it('should track execution time', () => {
    const db = new Database();
    db.execute('CREATE TABLE perf (id INTEGER)');
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO perf VALUES (${i})`);
    }
    
    const stats = query(db, 'SELECT * FROM pg_stat_statements');
    const insertStats = stats.filter(s => s.query.includes('INSERT INTO perf'));
    
    assert.equal(insertStats[0].calls, 10);
    assert.ok(insertStats[0].total_exec_time >= 0, 'Should have total exec time');
    assert.ok(insertStats[0].mean_exec_time >= 0, 'Should have mean exec time');
    assert.ok(insertStats[0].min_exec_time >= 0, 'Should have min exec time');
    assert.ok(insertStats[0].max_exec_time >= insertStats[0].min_exec_time, 'Max should be >= min');
  });

  it('should track rows returned/affected', () => {
    const db = new Database();
    db.execute('CREATE TABLE tracked (id INTEGER, val TEXT)');
    db.execute("INSERT INTO tracked VALUES (1, 'a')");
    db.execute("INSERT INTO tracked VALUES (2, 'b')");
    db.execute("INSERT INTO tracked VALUES (3, 'c')");
    
    // SELECT returns rows
    query(db, 'SELECT * FROM tracked');
    query(db, 'SELECT * FROM tracked');
    
    const stats = query(db, 'SELECT * FROM pg_stat_statements');
    const selectStats = stats.filter(s => s.query === 'SELECT * FROM tracked');
    
    assert.ok(selectStats.length > 0, 'Should have SELECT stats');
    assert.equal(selectStats[0].calls, 2);
    // 2 calls * 3 rows each = 6 total rows
    assert.equal(selectStats[0].rows, 6);
  });

  it('should reset stats via pg_stat_statements_reset()', () => {
    const db = new Database();
    db.execute('CREATE TABLE reset_test (id INTEGER)');
    db.execute('INSERT INTO reset_test VALUES (1)');
    db.execute('INSERT INTO reset_test VALUES (2)');
    
    let stats = query(db, 'SELECT * FROM pg_stat_statements');
    assert.ok(stats.length > 0, 'Should have stats before reset');
    
    db.execute('SELECT pg_stat_statements_reset()');
    
    stats = query(db, 'SELECT * FROM pg_stat_statements');
    // After reset, only the SELECT and reset queries should exist
    const insertStats = stats.filter(s => s.query.includes('INSERT INTO reset_test'));
    assert.equal(insertStats.length, 0, 'INSERT stats should be cleared after reset');
  });

  it('should handle SELECT with WHERE normalization', () => {
    const db = new Database();
    db.execute('CREATE TABLE lookup (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO lookup VALUES (1, 'Alice')");
    db.execute("INSERT INTO lookup VALUES (2, 'Bob')");
    
    query(db, 'SELECT * FROM lookup WHERE id = 1');
    query(db, 'SELECT * FROM lookup WHERE id = 2');
    query(db, 'SELECT * FROM lookup WHERE id = 99');
    
    const stats = query(db, 'SELECT * FROM pg_stat_statements');
    const lookupStats = stats.filter(s => s.query.includes('WHERE id'));
    
    // All 3 should normalize to one entry
    assert.equal(lookupStats.length, 1);
    assert.equal(lookupStats[0].calls, 3);
    assert.equal(lookupStats[0].query, 'SELECT * FROM lookup WHERE id = $?');
  });

  it('should differentiate structurally different queries', () => {
    const db = new Database();
    db.execute('CREATE TABLE diff (id INTEGER, name TEXT, age INTEGER)');
    db.execute("INSERT INTO diff VALUES (1, 'Alice', 30)");
    
    query(db, 'SELECT * FROM diff WHERE id = 1');
    query(db, 'SELECT * FROM diff WHERE name = \'Alice\'');
    query(db, 'SELECT * FROM diff WHERE age = 30');
    
    const stats = query(db, 'SELECT * FROM pg_stat_statements');
    const whereStats = stats.filter(s => s.query.includes('WHERE'));
    
    // 3 structurally different queries
    assert.equal(whereStats.length, 3, 'Structurally different queries should have separate entries');
  });
});
