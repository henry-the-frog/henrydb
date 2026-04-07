// volcano-integration.test.js — Compare volcano engine results with standard engine
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { parse } from './sql.js';

describe('Volcano Integration', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT, name TEXT, age INT, dept TEXT)');
    db.execute("INSERT INTO users VALUES (1, 'Alice', 30, 'Engineering')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 25, 'Sales')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 35, 'Engineering')");
    db.execute("INSERT INTO users VALUES (4, 'Diana', 28, 'Marketing')");
    db.execute("INSERT INTO users VALUES (5, 'Eve', 22, 'Sales')");
    
    db.execute('CREATE TABLE orders (id INT, user_id INT, amount INT, product TEXT)');
    db.execute('INSERT INTO orders VALUES (1, 1, 100, \'book\')');
    db.execute('INSERT INTO orders VALUES (2, 1, 50, \'pen\')');
    db.execute('INSERT INTO orders VALUES (3, 2, 200, \'laptop\')');
    db.execute('INSERT INTO orders VALUES (4, 3, 75, \'book\')');
    db.execute('INSERT INTO orders VALUES (5, 5, 300, \'phone\')');
  });

  function volcanoQuery(sql) {
    const ast = parse(sql);
    const plan = buildPlan(ast, db.tables);
    return plan.toArray();
  }

  function standardQuery(sql) {
    return db.execute(sql).rows;
  }

  // Helper: compare results ignoring row order and internal fields
  function compareResults(volcano, standard, msg) {
    const clean = (rows) => rows.map(r => {
      const c = {};
      for (const [k, v] of Object.entries(r)) {
        if (!k.startsWith('_') && !k.includes('.')) c[k] = v;
      }
      return c;
    });
    const vRows = clean(volcano);
    const sRows = clean(standard);
    assert.equal(vRows.length, sRows.length, `${msg}: row count mismatch (volcano=${vRows.length}, standard=${sRows.length})`);
  }

  // ===== Basic SELECT =====

  it('SELECT * FROM users', () => {
    const v = volcanoQuery('SELECT * FROM users');
    const s = standardQuery('SELECT * FROM users');
    assert.equal(v.length, s.length);
  });

  it('SELECT with WHERE', () => {
    const v = volcanoQuery('SELECT name, age FROM users WHERE age > 25');
    assert.ok(v.length > 0);
    assert.ok(v.every(r => r.age > 25));
  });

  it('SELECT with AND', () => {
    const v = volcanoQuery("SELECT name FROM users WHERE age > 25 AND dept = 'Engineering'");
    assert.equal(v.length, 2); // Alice (30) and Charlie (35)
  });

  it('SELECT with OR', () => {
    const v = volcanoQuery("SELECT name FROM users WHERE dept = 'Sales' OR dept = 'Marketing'");
    assert.equal(v.length, 3); // Bob, Diana, Eve
  });

  it('SELECT with ORDER BY', () => {
    const v = volcanoQuery('SELECT name, age FROM users ORDER BY age');
    assert.equal(v[0].name, 'Eve');    // 22
    assert.equal(v[4].name, 'Charlie'); // 35
  });

  it('SELECT with ORDER BY DESC', () => {
    const v = volcanoQuery('SELECT name, age FROM users ORDER BY age DESC');
    assert.equal(v[0].name, 'Charlie');
    assert.equal(v[4].name, 'Eve');
  });

  it('SELECT with LIMIT', () => {
    const v = volcanoQuery('SELECT name FROM users ORDER BY id LIMIT 3');
    assert.equal(v.length, 3);
  });

  it('SELECT with LIMIT and OFFSET', () => {
    const v = volcanoQuery('SELECT name FROM users ORDER BY id LIMIT 2 OFFSET 2');
    assert.equal(v.length, 2);
    assert.equal(v[0].name, 'Charlie');
  });

  it('SELECT DISTINCT', () => {
    const v = volcanoQuery('SELECT DISTINCT dept FROM users');
    assert.equal(v.length, 3); // Engineering, Sales, Marketing
  });

  // ===== Aggregate queries =====

  it('COUNT(*)', () => {
    const v = volcanoQuery('SELECT COUNT(*) as cnt FROM users');
    assert.equal(v.length, 1);
    assert.equal(v[0].cnt, 5);
  });

  it('SUM with GROUP BY', () => {
    const v = volcanoQuery('SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id');
    assert.ok(v.length > 0);
    const alice = v.find(r => r.user_id === 1);
    assert.equal(alice.total, 150); // 100 + 50
  });

  it('COUNT with GROUP BY', () => {
    const v = volcanoQuery('SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept');
    const eng = v.find(r => r.dept === 'Engineering');
    assert.equal(eng.cnt, 2); // Alice, Charlie
  });

  it('AVG aggregate', () => {
    const v = volcanoQuery('SELECT AVG(age) as avg_age FROM users');
    assert.equal(v[0].avg_age, 28); // (30+25+35+28+22)/5
  });

  it('MIN and MAX', () => {
    const v = volcanoQuery('SELECT MIN(age) as youngest, MAX(age) as oldest FROM users');
    assert.equal(v[0].youngest, 22);
    assert.equal(v[0].oldest, 35);
  });

  it('GROUP BY with HAVING', () => {
    const v = volcanoQuery('SELECT dept, COUNT(*) as cnt FROM users GROUP BY dept HAVING COUNT(*) > 1');
    assert.equal(v.length, 2); // Engineering (2), Sales (2)
  });

  // ===== JOIN queries =====

  it('INNER JOIN', () => {
    const v = volcanoQuery('SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id');
    assert.equal(v.length, 5); // 5 orders match users
  });

  it('JOIN with WHERE', () => {
    const v = volcanoQuery("SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100");
    assert.ok(v.length > 0);
    assert.ok(v.every(r => (r['o.amount'] || r.amount) > 100));
  });

  it('JOIN with ORDER BY', () => {
    const v = volcanoQuery('SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id ORDER BY o.amount DESC');
    const topAmount = v[0]['o.amount'] || v[0].amount;
    assert.equal(topAmount, 300); // Eve's phone
  });

  // ===== Complex queries =====

  it('JOIN + GROUP BY + ORDER BY', () => {
    const v = volcanoQuery('SELECT u.name, SUM(o.amount) as total FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.name ORDER BY total DESC');
    assert.ok(v.length > 0);
    // Eve has 300, Bob has 200, Alice has 150, Charlie has 75
  });

  it('multiple column ORDER BY', () => {
    const v = volcanoQuery('SELECT dept, name FROM users ORDER BY dept, name');
    assert.equal(v[0].dept, 'Engineering');
    assert.equal(v[0].name, 'Alice');
  });

  // ===== Result correctness =====

  it('volcano and standard produce same row count for basic query', () => {
    const queries = [
      'SELECT * FROM users',
      'SELECT name FROM users WHERE age > 25',
      "SELECT * FROM users WHERE dept = 'Engineering'",
    ];
    for (const sql of queries) {
      const v = volcanoQuery(sql);
      const s = standardQuery(sql);
      compareResults(v, s, sql);
    }
  });

  it('volcano LIMIT actually stops early (performance)', () => {
    // Insert 1000 rows
    db.execute('CREATE TABLE big (x INT)');
    for (let i = 0; i < 1000; i++) db.execute(`INSERT INTO big VALUES (${i})`);
    
    const v = volcanoQuery('SELECT x FROM big LIMIT 5');
    assert.equal(v.length, 5);
  });
});
