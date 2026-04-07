// views.test.js — Tests for CREATE VIEW
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Views', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT, name TEXT, age INT, dept TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 'Engineering')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, 'Sales')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35, 'Engineering')");
    db.execute("INSERT INTO users VALUES (4, 'Diana', 28, 'Marketing')");
    
    db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT)');
    db.execute('INSERT INTO orders VALUES (1, 1, 100)');
    db.execute('INSERT INTO orders VALUES (2, 2, 200)');
    db.execute('INSERT INTO orders VALUES (3, 1, 50)');
  });

  it('CREATE VIEW and SELECT from view', () => {
    db.execute('CREATE VIEW engineers AS SELECT * FROM users WHERE dept = \'Engineering\'');
    const result = db.execute('SELECT * FROM engineers ORDER BY id');
    assert.equal(result.rows.length, 2);
    assert.equal(result.rows[0].name, 'Alice');
    assert.equal(result.rows[1].name, 'Charlie');
  });

  it('view reflects live data (not materialized)', () => {
    db.execute('CREATE VIEW all_users AS SELECT * FROM users');
    
    const before = db.execute('SELECT COUNT(*) as cnt FROM all_users');
    assert.equal(before.rows[0].cnt, 4);
    
    db.execute("INSERT INTO users VALUES (5, 'Eve', 22, 'Sales')");
    
    const after = db.execute('SELECT COUNT(*) as cnt FROM all_users');
    assert.equal(after.rows[0].cnt, 5); // View sees new row
  });

  it('view with specific columns', () => {
    db.execute('CREATE VIEW user_names AS SELECT id, name FROM users');
    const result = db.execute('SELECT * FROM user_names ORDER BY id');
    assert.equal(result.rows.length, 4);
    assert.ok('name' in result.rows[0]);
  });

  it('view with aggregate', () => {
    db.execute('CREATE VIEW dept_counts AS SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept');
    const result = db.execute('SELECT * FROM dept_counts ORDER BY cnt DESC');
    assert.ok(result.rows.length > 0);
    assert.equal(result.rows[0].dept, 'Engineering');
    assert.equal(result.rows[0].cnt, 2);
  });

  it('view with WHERE filter on top', () => {
    db.execute('CREATE VIEW adults AS SELECT * FROM users WHERE age >= 25');
    const result = db.execute('SELECT name FROM adults WHERE dept = \'Engineering\'');
    assert.equal(result.rows.length, 2); // Alice (30) and Charlie (35)
  });

  it('view used in subquery', () => {
    db.execute("CREATE VIEW engineers AS SELECT * FROM users WHERE dept = 'Engineering'");
    const result = db.execute('SELECT name FROM engineers WHERE age > 30');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Charlie');
  });

  it('DROP VIEW', () => {
    db.execute('CREATE VIEW v AS SELECT * FROM users');
    db.execute('SELECT * FROM v'); // Works
    db.execute('DROP VIEW v');
    assert.throws(() => db.execute('SELECT * FROM v'));
  });

  it('views survive across queries', () => {
    db.execute('CREATE VIEW v AS SELECT * FROM users WHERE age > 25');
    const r1 = db.execute('SELECT * FROM v');
    const r2 = db.execute('SELECT COUNT(*) as cnt FROM v');
    assert.equal(r1.rows.length, 3);
    assert.equal(r2.rows[0].cnt, 3);
  });

  it('multiple views on same table', () => {
    db.execute("CREATE VIEW eng AS SELECT * FROM users WHERE dept = 'Engineering'");
    db.execute("CREATE VIEW sales AS SELECT * FROM users WHERE dept = 'Sales'");
    
    const engResult = db.execute('SELECT * FROM eng');
    const salesResult = db.execute('SELECT * FROM sales');
    
    assert.equal(engResult.rows.length, 2);
    assert.equal(salesResult.rows.length, 1);
  });

  it('view with ORDER BY', () => {
    db.execute('CREATE VIEW sorted_users AS SELECT * FROM users ORDER BY age DESC');
    const result = db.execute('SELECT name FROM sorted_users');
    assert.equal(result.rows[0].name, 'Charlie'); // 35
  });
});
