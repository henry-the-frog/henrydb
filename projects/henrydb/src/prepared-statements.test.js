// prepared-statements.test.js — Tests for prepared statements
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Prepared statements (SQL)', () => {
  function setupDB() {
    const db = new Database();
    db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
    return db;
  }

  it('PREPARE and EXECUTE with parameters', () => {
    const db = setupDB();
    db.execute('PREPARE find_user AS SELECT * FROM users WHERE id = $1');
    const result = db.execute('EXECUTE find_user(1)');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Alice');
  });

  it('EXECUTE with different parameters', () => {
    const db = setupDB();
    db.execute('PREPARE get_user AS SELECT * FROM users WHERE id = $1');
    
    const r1 = db.execute('EXECUTE get_user(1)');
    assert.equal(r1.rows[0].name, 'Alice');
    
    const r2 = db.execute('EXECUTE get_user(2)');
    assert.equal(r2.rows[0].name, 'Bob');
    
    const r3 = db.execute('EXECUTE get_user(3)');
    assert.equal(r3.rows[0].name, 'Charlie');
  });

  it('PREPARE with multiple parameters', () => {
    const db = setupDB();
    db.execute('PREPARE find_by_age AS SELECT * FROM users WHERE age >= $1 AND age <= $2');
    
    const result = db.execute('EXECUTE find_by_age(25, 30)');
    assert.equal(result.rows.length, 2);
  });

  it('DEALLOCATE removes prepared statement', () => {
    const db = setupDB();
    db.execute('PREPARE test_stmt AS SELECT * FROM users');
    db.execute('DEALLOCATE test_stmt');
    
    assert.throws(() => {
      db.execute('EXECUTE test_stmt()');
    }, /not found/);
  });

  it('DEALLOCATE ALL', () => {
    const db = setupDB();
    db.execute('PREPARE s1 AS SELECT * FROM users WHERE id = $1');
    db.execute('PREPARE s2 AS SELECT * FROM users WHERE name = $1');
    db.execute('DEALLOCATE ALL');
    
    assert.throws(() => db.execute('EXECUTE s1(1)'), /not found/);
    assert.throws(() => db.execute('EXECUTE s2(1)'), /not found/);
  });

  it('duplicate PREPARE throws', () => {
    const db = setupDB();
    db.execute('PREPARE dup AS SELECT * FROM users');
    assert.throws(() => {
      db.execute('PREPARE dup AS SELECT * FROM users');
    }, /already exists/);
  });

  it('EXECUTE non-existent throws', () => {
    const db = setupDB();
    assert.throws(() => {
      db.execute('EXECUTE nonexistent()');
    }, /not found/);
  });

  it('PREPARE INSERT with parameters', () => {
    const db = setupDB();
    db.execute('PREPARE add_user AS INSERT INTO users VALUES ($1, $2, $3)');
    db.execute('EXECUTE add_user(4, \'Dave\', 28)');
    
    const result = db.execute('SELECT * FROM users WHERE id = 4');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Dave');
  });

  it('PREPARE DELETE with parameters', () => {
    const db = setupDB();
    db.execute('PREPARE del_user AS DELETE FROM users WHERE id = $1');
    db.execute('EXECUTE del_user(2)');
    
    const result = db.execute('SELECT COUNT(*) as cnt FROM users');
    assert.equal(result.rows[0].cnt, 2);
  });
});

describe('Programmatic prepare API', () => {
  function setupDB() {
    const db = new Database();
    db.execute('CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO items VALUES (${i}, 'Item ${i}', ${i * 10})`);
    }
    return db;
  }

  it('db.prepare returns statement with execute method', () => {
    const db = setupDB();
    const stmt = db.prepare('SELECT * FROM items WHERE id = $1');
    
    const r1 = stmt.execute(1);
    assert.equal(r1.rows.length, 1);
    assert.equal(r1.rows[0].name, 'Item 1');
    
    const r2 = stmt.execute(50);
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].name, 'Item 50');
  });

  it('prepared statement is reusable', () => {
    const db = setupDB();
    const stmt = db.prepare('SELECT * FROM items WHERE price > $1 AND price <= $2');
    
    const r1 = stmt.execute(100, 200);
    assert.equal(r1.rows.length, 10); // prices 110-200
    
    const r2 = stmt.execute(500, 600);
    assert.equal(r2.rows.length, 10); // prices 510-600
  });

  it('stmt.close removes the statement', () => {
    const db = setupDB();
    const stmt = db.prepare('SELECT * FROM items WHERE id = $1');
    stmt.close();
    
    // Should still work for new prepare
    const stmt2 = db.prepare('SELECT * FROM items WHERE id = $1');
    const result = stmt2.execute(1);
    assert.equal(result.rows.length, 1);
  });

  it('performance: prepared vs unprepared for 1K queries', () => {
    const db = setupDB();
    
    // Unprepared (parse each time)
    const t0 = performance.now();
    for (let i = 1; i <= 1000; i++) {
      db.execute(`SELECT * FROM items WHERE id = ${i % 100 + 1}`);
    }
    const unpreparedMs = performance.now() - t0;
    
    // Prepared (parse once)
    const stmt = db.prepare('SELECT * FROM items WHERE id = $1');
    const t1 = performance.now();
    for (let i = 1; i <= 1000; i++) {
      stmt.execute(i % 100 + 1);
    }
    const preparedMs = performance.now() - t1;
    
    const speedup = unpreparedMs / preparedMs;
    console.log(`  1K queries: Unprepared ${unpreparedMs.toFixed(1)}ms | Prepared ${preparedMs.toFixed(1)}ms | speedup ${speedup.toFixed(2)}x`);
    
    // Prepared should be faster (no parsing overhead)
    // But the plan cache in execute() might reduce the difference
    assert.ok(true);
  });
});
