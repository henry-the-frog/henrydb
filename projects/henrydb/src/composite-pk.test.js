import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

describe('Composite Primary Keys', () => {
  it('basic composite PK with two columns', () => {
    const db = new Database();
    db.execute("CREATE TABLE assignments (emp_id INTEGER, proj_id INTEGER, hours INTEGER, PRIMARY KEY (emp_id, proj_id))");
    db.execute("INSERT INTO assignments VALUES (1, 1, 20)");
    db.execute("INSERT INTO assignments VALUES (1, 2, 30)");
    db.execute("INSERT INTO assignments VALUES (2, 1, 15)");
    
    const r = db.execute("SELECT * FROM assignments ORDER BY emp_id, proj_id");
    assert.equal(r.rows.length, 3);
  });

  it('composite PK enforces uniqueness', () => {
    const db = new Database();
    db.execute("CREATE TABLE votes (user_id INTEGER, item_id INTEGER, score INTEGER, PRIMARY KEY (user_id, item_id))");
    db.execute("INSERT INTO votes VALUES (1, 1, 5)");
    db.execute("INSERT INTO votes VALUES (1, 2, 3)");
    
    // Duplicate composite key should fail
    assert.throws(() => {
      db.execute("INSERT INTO votes VALUES (1, 1, 4)");
    }, /duplicate|unique|primary/i);
  });

  it('composite PK with three columns', () => {
    const db = new Database();
    db.execute("CREATE TABLE scores (game INTEGER, round INTEGER, player INTEGER, points INTEGER, PRIMARY KEY (game, round, player))");
    db.execute("INSERT INTO scores VALUES (1, 1, 1, 10)");
    db.execute("INSERT INTO scores VALUES (1, 1, 2, 20)");
    db.execute("INSERT INTO scores VALUES (1, 2, 1, 15)");
    
    const r = db.execute("SELECT SUM(points) AS total FROM scores WHERE game = 1 AND player = 1");
    assert.equal(r.rows[0].total, 25);
  });

  it('composite PK with JOIN', () => {
    const db = new Database();
    db.execute("CREATE TABLE orders (order_id INTEGER, line_no INTEGER, product TEXT, qty INTEGER, PRIMARY KEY (order_id, line_no))");
    db.execute("CREATE TABLE order_status (order_id INTEGER, line_no INTEGER, status TEXT)");
    db.execute("INSERT INTO orders VALUES (100, 1, 'Widget', 5)");
    db.execute("INSERT INTO orders VALUES (100, 2, 'Gadget', 3)");
    db.execute("INSERT INTO order_status VALUES (100, 1, 'shipped')");
    db.execute("INSERT INTO order_status VALUES (100, 2, 'pending')");
    
    const r = db.execute(`
      SELECT o.product, o.qty, s.status 
      FROM orders o JOIN order_status s ON o.order_id = s.order_id AND o.line_no = s.line_no
      ORDER BY o.line_no
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].status, 'shipped');
    assert.equal(r.rows[1].status, 'pending');
  });

  it('composite PK with UPDATE', () => {
    const db = new Database();
    db.execute("CREATE TABLE prefs (user_id INTEGER, setting TEXT, value TEXT, PRIMARY KEY (user_id, setting))");
    db.execute("INSERT INTO prefs VALUES (1, 'theme', 'dark')");
    db.execute("INSERT INTO prefs VALUES (1, 'lang', 'en')");
    
    db.execute("UPDATE prefs SET value = 'light' WHERE user_id = 1 AND setting = 'theme'");
    const r = db.execute("SELECT value FROM prefs WHERE user_id = 1 AND setting = 'theme'");
    assert.equal(r.rows[0].value, 'light');
  });

  it('composite PK with DELETE', () => {
    const db = new Database();
    db.execute("CREATE TABLE tags (entity_type TEXT, entity_id INTEGER, tag TEXT, PRIMARY KEY (entity_type, entity_id, tag))");
    db.execute("INSERT INTO tags VALUES ('user', 1, 'admin')");
    db.execute("INSERT INTO tags VALUES ('user', 1, 'active')");
    db.execute("INSERT INTO tags VALUES ('post', 1, 'featured')");
    
    db.execute("DELETE FROM tags WHERE entity_type = 'user' AND tag = 'admin'");
    const r = db.execute("SELECT * FROM tags ORDER BY entity_type, tag");
    assert.equal(r.rows.length, 2);
  });

  it('table-level UNIQUE constraint', () => {
    const db = new Database();
    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, sku TEXT, UNIQUE (sku))");
    db.execute("INSERT INTO products VALUES (1, 'Widget', 'W-001')");
    db.execute("INSERT INTO products VALUES (2, 'Gadget', 'G-001')");
    
    // Duplicate SKU should fail
    assert.throws(() => {
      db.execute("INSERT INTO products VALUES (3, 'Doohickey', 'W-001')");
    }, /duplicate|unique/i);
  });

  it('composite PK survives round-trip', () => {
    const db = new Database();
    db.execute("CREATE TABLE enrollment (student_id INTEGER, course_id INTEGER, grade TEXT, PRIMARY KEY (student_id, course_id))");
    db.execute("INSERT INTO enrollment VALUES (1, 101, 'A')");
    db.execute("INSERT INTO enrollment VALUES (1, 102, 'B')");
    db.execute("INSERT INTO enrollment VALUES (2, 101, 'C')");
    
    const db2 = Database.fromSerialized(db.serialize());
    const r = db2.execute("SELECT * FROM enrollment ORDER BY student_id, course_id");
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].grade, 'A');
    
    // PK should still be enforced
    assert.throws(() => {
      db2.execute("INSERT INTO enrollment VALUES (1, 101, 'D')");
    }, /duplicate|unique|primary/i);
  });
});
