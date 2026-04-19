// critical-paths-stress.test.js — Targeted stress tests for recently-fixed critical paths
// Tests SERIAL with partial column inserts, correlated subquery WHERE, composite PK, persistence

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('SERIAL with Partial Column Inserts', () => {
  let db;
  
  beforeEach(() => { db = new Database(); });
  
  it('SERIAL auto-increment with multiple tables', () => {
    db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT NOT NULL)");
    db.execute("CREATE TABLE posts (id SERIAL PRIMARY KEY, user_id INT, title TEXT)");
    db.execute("CREATE TABLE comments (id SERIAL PRIMARY KEY, post_id INT, user_id INT, body TEXT)");
    
    // Insert users
    for (let i = 0; i < 10; i++) {
      db.execute(`INSERT INTO users (name) VALUES ('User${i}')`);
    }
    
    // Insert posts with partial columns
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO posts (user_id, title) VALUES (${1 + (i % 10)}, 'Post${i}')`);
    }
    
    // Insert comments with partial columns
    for (let i = 0; i < 50; i++) {
      db.execute(`INSERT INTO comments (post_id, user_id, body) VALUES (${1 + (i % 20)}, ${1 + (i % 10)}, 'Comment${i}')`);
    }
    
    const users = db.execute("SELECT COUNT(*) AS cnt FROM users");
    assert.equal(users.rows[0].cnt, 10);
    
    const posts = db.execute("SELECT COUNT(*) AS cnt FROM posts");
    assert.equal(posts.rows[0].cnt, 20);
    
    const comments = db.execute("SELECT COUNT(*) AS cnt FROM comments");
    assert.equal(comments.rows[0].cnt, 50);
    
    // Verify IDs are sequential
    const postIds = db.execute("SELECT id FROM posts ORDER BY id");
    for (let i = 0; i < 20; i++) {
      assert.equal(postIds.rows[i].id, i + 1);
    }
    
    const commentIds = db.execute("SELECT id FROM comments ORDER BY id");
    for (let i = 0; i < 50; i++) {
      assert.equal(commentIds.rows[i].id, i + 1);
    }
  });
  
  it('SERIAL with explicit ID overrides', () => {
    db.execute("CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT)");
    
    // Auto-generate first 5
    for (let i = 0; i < 5; i++) {
      db.execute(`INSERT INTO items (name) VALUES ('auto${i}')`);
    }
    
    // Explicitly insert id=100
    db.execute("INSERT INTO items (id, name) VALUES (100, 'explicit')");
    
    // Auto-generate should continue after gap
    db.execute("INSERT INTO items (name) VALUES ('after_gap')");
    const last = db.execute("SELECT id FROM items WHERE name = 'after_gap'");
    assert.ok(last.rows[0].id > 100, `Expected id > 100 but got ${last.rows[0].id}`);
    
    const total = db.execute("SELECT COUNT(*) AS cnt FROM items");
    assert.equal(total.rows[0].cnt, 7);
  });
  
  it('SERIAL with RETURNING clause', () => {
    db.execute("CREATE TABLE events (id SERIAL PRIMARY KEY, type TEXT, data TEXT DEFAULT 'empty')");
    
    const ids = [];
    for (let i = 0; i < 10; i++) {
      const r = db.execute(`INSERT INTO events (type) VALUES ('event${i}') RETURNING id`);
      ids.push(r.rows[0].id);
    }
    
    // IDs should be 1-10
    for (let i = 0; i < 10; i++) {
      assert.equal(ids[i], i + 1);
    }
  });
  
  it('SERIAL with UPSERT (ON CONFLICT)', () => {
    db.execute("CREATE TABLE counters (id SERIAL PRIMARY KEY, key TEXT UNIQUE, count INT DEFAULT 0)");
    
    db.execute("INSERT INTO counters (key, count) VALUES ('views', 1) ON CONFLICT (key) DO UPDATE SET count = counters.count + 1");
    db.execute("INSERT INTO counters (key, count) VALUES ('views', 1) ON CONFLICT (key) DO UPDATE SET count = counters.count + 1");
    db.execute("INSERT INTO counters (key, count) VALUES ('clicks', 1) ON CONFLICT (key) DO UPDATE SET count = counters.count + 1");
    
    const views = db.execute("SELECT count FROM counters WHERE key = 'views'");
    assert.equal(views.rows[0].count, 2);
    
    const total = db.execute("SELECT COUNT(*) AS cnt FROM counters");
    assert.equal(total.rows[0].cnt, 2);
  });
});

describe('Correlated Subquery in WHERE', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
    db.execute("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)");
    db.execute("CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT)");
    
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
    db.execute("INSERT INTO departments VALUES (2, 'Sales')");
    db.execute("INSERT INTO departments VALUES (3, 'HR')");
    
    db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 90000)");
    db.execute("INSERT INTO employees VALUES (2, 'Bob', 1, 80000)");
    db.execute("INSERT INTO employees VALUES (3, 'Carol', 2, 70000)");
    db.execute("INSERT INTO employees VALUES (4, 'Dave', 2, 60000)");
    db.execute("INSERT INTO employees VALUES (5, 'Eve', 3, 50000)");
  });
  
  it('WHERE (SELECT COUNT(*)) > N', () => {
    // Departments with more than 1 employee
    const r = db.execute(
      "SELECT d.name FROM departments d WHERE (SELECT COUNT(*) FROM employees e WHERE e.dept_id = d.id) > 1 ORDER BY d.name"
    );
    assert.equal(r.rows.length, 2);
    assert.deepEqual(r.rows.map(x => x.name), ['Engineering', 'Sales']);
  });
  
  it('WHERE (SELECT AVG()) > value', () => {
    // Departments where average salary > 75000
    const r = db.execute(
      "SELECT d.name FROM departments d WHERE (SELECT AVG(salary) FROM employees e WHERE e.dept_id = d.id) > 75000 ORDER BY d.name"
    );
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Engineering');
  });
  
  it('WHERE (SELECT MAX()) = value', () => {
    // Departments where the top earner makes 90000
    const r = db.execute(
      "SELECT d.name FROM departments d WHERE (SELECT MAX(salary) FROM employees e WHERE e.dept_id = d.id) = 90000"
    );
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'Engineering');
  });
  
  it('employees earning above their department average', () => {
    const r = db.execute(
      "SELECT e.name FROM employees e WHERE e.salary > (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.dept_id = e.dept_id) ORDER BY e.name"
    );
    // Eng avg = 85000: Alice (90k) above. Sales avg = 65000: Carol (70k) above.
    // HR: only Eve, avg = 50000, Eve = 50000 (not above)
    assert.equal(r.rows.length, 2);
    assert.deepEqual(r.rows.map(x => x.name), ['Alice', 'Carol']);
  });
  
  it('WHERE (SELECT SUM()) < threshold', () => {
    const r = db.execute(
      "SELECT d.name FROM departments d WHERE (SELECT SUM(salary) FROM employees e WHERE e.dept_id = d.id) < 100000 ORDER BY d.name"
    );
    // HR: 50000 < 100000
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].name, 'HR');
  });
});

describe('Composite PK Stress', () => {
  it('three-column composite PK with many inserts', () => {
    const db = new Database();
    db.execute("CREATE TABLE events (year INT, month INT, day INT, event TEXT, PRIMARY KEY (year, month, day))");
    
    let inserted = 0;
    for (let y = 2024; y <= 2026; y++) {
      for (let m = 1; m <= 12; m++) {
        for (let d = 1; d <= 28; d++) {
          db.execute(`INSERT INTO events VALUES (${y}, ${m}, ${d}, 'Event_${y}_${m}_${d}')`);
          inserted++;
        }
      }
    }
    
    const count = db.execute("SELECT COUNT(*) AS cnt FROM events");
    assert.equal(count.rows[0].cnt, inserted);
    
    // Duplicate should throw
    assert.throws(() => {
      db.execute("INSERT INTO events VALUES (2024, 1, 1, 'duplicate')");
    }, /duplicate|unique|constraint/i);
  });
  
  it('composite PK with joins', () => {
    const db = new Database();
    db.execute("CREATE TABLE order_items (order_id INT, line_no INT, product TEXT, qty INT, PRIMARY KEY (order_id, line_no))");
    db.execute("CREATE TABLE order_notes (order_id INT, line_no INT, note TEXT)");
    
    for (let o = 1; o <= 5; o++) {
      for (let l = 1; l <= 3; l++) {
        db.execute(`INSERT INTO order_items VALUES (${o}, ${l}, 'Product_${o}_${l}', ${l * 10})`);
        db.execute(`INSERT INTO order_notes VALUES (${o}, ${l}, 'Note_${o}_${l}')`);
      }
    }
    
    const r = db.execute(
      "SELECT oi.product, on2.note FROM order_items oi " +
      "INNER JOIN order_notes on2 ON oi.order_id = on2.order_id AND oi.line_no = on2.line_no " +
      "WHERE oi.order_id = 3 ORDER BY oi.line_no"
    );
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].product, 'Product_3_1');
  });
});

describe('Persistence Round-Trip', () => {
  it('survives serialize/deserialize with all data types', () => {
    const db = new Database();
    db.execute("CREATE TABLE mixed (id SERIAL PRIMARY KEY, name TEXT, age INT, score REAL, active BOOLEAN)");
    db.execute("CREATE TABLE linked (id INT PRIMARY KEY, mixed_id INT)");
    db.execute("CREATE INDEX idx_linked ON linked (mixed_id)");
    
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO mixed (name, age, score, active) VALUES ('Person${i}', ${20 + i}, ${i * 1.5}, ${i % 2 === 0})`);
    }
    for (let i = 0; i < 20; i++) {
      db.execute(`INSERT INTO linked VALUES (${i + 1}, ${1 + (i % 20)})`);
    }
    
    const db2 = Database.fromSerialized(db.serialize());
    
    // Verify data
    const mixed = db2.execute("SELECT COUNT(*) AS cnt FROM mixed");
    assert.equal(mixed.rows[0].cnt, 20);
    
    const linked = db2.execute("SELECT COUNT(*) AS cnt FROM linked");
    assert.equal(linked.rows[0].cnt, 20);
    
    // Verify SERIAL continues correctly after deserialization
    db2.execute("INSERT INTO mixed (name, age, score, active) VALUES ('NewPerson', 99, 10.5, true)");
    const newId = db2.execute("SELECT MAX(id) AS max_id FROM mixed");
    assert.equal(newId.rows[0].max_id, 21);
    
    // Verify JOINs work
    const joined = db2.execute(
      "SELECT m.name, l.id AS link_id FROM mixed m INNER JOIN linked l ON l.mixed_id = m.id WHERE m.id <= 3 ORDER BY m.id"
    );
    assert.ok(joined.rows.length >= 1);
  });
  
  it('composite PK survives round-trip', () => {
    const db = new Database();
    db.execute("CREATE TABLE scores (game INT, player INT, round INT, points INT, PRIMARY KEY (game, player, round))");
    
    for (let g = 1; g <= 3; g++) {
      for (let p = 1; p <= 4; p++) {
        for (let r = 1; r <= 2; r++) {
          db.execute(`INSERT INTO scores VALUES (${g}, ${p}, ${r}, ${g * p * r * 10})`);
        }
      }
    }
    
    const db2 = Database.fromSerialized(db.serialize());
    
    const count = db2.execute("SELECT COUNT(*) AS cnt FROM scores");
    assert.equal(count.rows[0].cnt, 24);
    
    // Composite PK should still be enforced
    assert.throws(() => {
      db2.execute("INSERT INTO scores VALUES (1, 1, 1, 999)");
    }, /duplicate|unique|constraint/i);
  });
});

describe('Column Validation', () => {
  let db;
  
  beforeEach(() => {
    db = new Database();
    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT)");
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')");
  });
  
  it('rejects non-existent column in SELECT', () => {
    assert.throws(() => {
      db.execute("SELECT nonexistent FROM users");
    }, /does not exist/i);
  });
  
  it('rejects non-existent column in WHERE', () => {
    assert.throws(() => {
      db.execute("SELECT * FROM users WHERE nonexistent = 1");
    }, /does not exist/i);
  });
  
  it('allows valid columns', () => {
    const r = db.execute("SELECT id, name, email FROM users");
    assert.equal(r.rows.length, 1);
  });
  
  it('allows SELECT *', () => {
    const r = db.execute("SELECT * FROM users WHERE id = 1");
    assert.equal(r.rows.length, 1);
  });
  
  it('rejects empty SELECT', () => {
    assert.throws(() => {
      db.execute("SELECT");
    }, /requires.*column|expression/i);
  });
});
