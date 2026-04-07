// transaction-index.test.js — Transaction, index, and ALTER TABLE tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Transactions', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('COMMIT persists changes', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('COMMIT');
    assert.equal(db.execute('SELECT COUNT(*) AS cnt FROM t').rows[0].cnt, 1);
  });

  it('BEGIN/COMMIT block works', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('BEGIN');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    db.execute('COMMIT');
    const result = db.execute('SELECT SUM(val) AS total FROM t');
    assert.equal(result.rows[0].total, 60);
  });
});

describe('Indexes', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, age INT)');
    for (let i = 1; i <= 20; i++) {
      db.execute(`INSERT INTO users VALUES (${i}, 'User${i}', 'user${i}@test.com', ${20 + i})`);
    }
  });

  it('CREATE INDEX', () => {
    db.execute('CREATE INDEX idx_name ON users (name)');
    const result = db.execute("SELECT * FROM users WHERE name = 'User5'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 5);
  });

  it('index speeds up lookups (correctness)', () => {
    db.execute('CREATE INDEX idx_email ON users (email)');
    const result = db.execute("SELECT id FROM users WHERE email = 'user10@test.com'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 10);
  });

  it('index with range query', () => {
    db.execute('CREATE INDEX idx_age ON users (age)');
    const result = db.execute('SELECT id FROM users WHERE age >= 30 AND age <= 35 ORDER BY age');
    assert.equal(result.rows.length, 6); // ages 30-35
  });

  it('query without index still works', () => {
    // No index on age, should still work via full scan
    const result = db.execute('SELECT COUNT(*) AS cnt FROM users WHERE age > 30');
    assert.equal(result.rows[0].cnt, 10); // ages 31-40
  });

  it('index on multiple inserts', () => {
    db.execute('CREATE INDEX idx_name ON users (name)');
    db.execute("INSERT INTO users VALUES (21, 'NewUser', 'new@test.com', 50)");
    const result = db.execute("SELECT * FROM users WHERE name = 'NewUser'");
    assert.equal(result.rows.length, 1);
  });
});

describe('ALTER TABLE', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    db.execute("INSERT INTO t VALUES (2, 'Bob')");
  });

  it('ADD COLUMN', () => {
    db.execute('ALTER TABLE t ADD COLUMN age INT');
    const result = db.execute('SELECT * FROM t ORDER BY id');
    assert.equal(result.rows.length, 2);
    // New column should be NULL
    assert.equal(result.rows[0].age, null);
  });

  it('ADD COLUMN with default via update', () => {
    db.execute('ALTER TABLE t ADD COLUMN active INT');
    db.execute('UPDATE t SET active = 1');
    const result = db.execute('SELECT * FROM t ORDER BY id');
    assert.ok(result.rows.every(r => r.active === 1));
  });

  it('INSERT after ADD COLUMN', () => {
    db.execute('ALTER TABLE t ADD COLUMN age INT');
    db.execute("INSERT INTO t VALUES (3, 'Charlie', 25)");
    const result = db.execute('SELECT * FROM t WHERE id = 3');
    assert.equal(result.rows[0].name, 'Charlie');
    assert.equal(result.rows[0].age, 25);
  });

  it('UPDATE new column', () => {
    db.execute('ALTER TABLE t ADD COLUMN score INT');
    db.execute('UPDATE t SET score = 100 WHERE id = 1');
    const result = db.execute('SELECT score FROM t WHERE id = 1');
    assert.equal(result.rows[0].score, 100);
  });
});

describe('DROP TABLE', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('DROP TABLE removes table', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('DROP TABLE t');
    assert.throws(() => db.execute('SELECT * FROM t'));
  });

  it('can recreate dropped table', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('DROP TABLE t');
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'Alice')");
    assert.equal(db.execute('SELECT * FROM t').rows[0].name, 'Alice');
  });
});

describe('UPDATE advanced', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('UPDATE multiple rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i * 10})`);
    db.execute('UPDATE t SET val = 0 WHERE val > 30');
    const result = db.execute('SELECT * FROM t WHERE val = 0');
    assert.equal(result.rows.length, 2); // id 4 and 5
  });

  it('UPDATE with expression', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('UPDATE t SET val = val + 5 WHERE id = 1');
    assert.equal(db.execute('SELECT val FROM t WHERE id = 1').rows[0].val, 15);
  });

  it('UPDATE all rows (no WHERE)', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('UPDATE t SET val = 99');
    const result = db.execute('SELECT DISTINCT val FROM t');
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].val, 99);
  });
});

describe('DISTINCT advanced', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('DISTINCT with multiple columns', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'x', 'y')");
    db.execute("INSERT INTO t VALUES (2, 'x', 'y')");
    db.execute("INSERT INTO t VALUES (3, 'x', 'z')");
    const result = db.execute('SELECT DISTINCT a, b FROM t ORDER BY a, b');
    assert.equal(result.rows.length, 2);
  });

  it('DISTINCT with ORDER BY', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, val INT)');
    db.execute('INSERT INTO t VALUES (1, 30)');
    db.execute('INSERT INTO t VALUES (2, 10)');
    db.execute('INSERT INTO t VALUES (3, 20)');
    db.execute('INSERT INTO t VALUES (4, 10)');
    const result = db.execute('SELECT DISTINCT val FROM t ORDER BY val');
    assert.equal(result.rows.length, 3);
    assert.equal(result.rows[0].val, 10);
    assert.equal(result.rows[2].val, 30);
  });
});

describe('OFFSET', () => {
  let db;
  beforeEach(() => { db = new Database(); });

  it('OFFSET skips rows', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i})`);
    const result = db.execute('SELECT * FROM t ORDER BY id LIMIT 3 OFFSET 5');
    assert.equal(result.rows.length, 3);
    assert.equal(result.rows[0].id, 6);
    assert.equal(result.rows[2].id, 8);
  });

  it('OFFSET beyond rows returns empty', () => {
    db.execute('CREATE TABLE t (id INT PRIMARY KEY)');
    db.execute('INSERT INTO t VALUES (1)');
    const result = db.execute('SELECT * FROM t LIMIT 10 OFFSET 100');
    assert.equal(result.rows.length, 0);
  });
});
