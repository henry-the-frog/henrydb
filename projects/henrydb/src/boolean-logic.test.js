// boolean-logic.test.js — Complex boolean logic, OR, nested parens tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Boolean Logic', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT, price INT, category TEXT, active INT)');
    db.execute("INSERT INTO items VALUES (1, 'Widget', 10, 'A', 1)");
    db.execute("INSERT INTO items VALUES (2, 'Gadget', 50, 'B', 1)");
    db.execute("INSERT INTO items VALUES (3, 'Doohickey', 30, 'A', 0)");
    db.execute("INSERT INTO items VALUES (4, 'Thingamajig', 80, 'C', 1)");
    db.execute("INSERT INTO items VALUES (5, 'Whatchamacallit', 20, 'B', 0)");
    db.execute("INSERT INTO items VALUES (6, 'Gizmo', 60, 'A', 1)");
    db.execute("INSERT INTO items VALUES (7, 'Doodad', 40, 'C', 0)");
    db.execute("INSERT INTO items VALUES (8, 'Contraption', 90, 'B', 1)");
  });

  it('simple OR', () => {
    const r = db.execute("SELECT * FROM items WHERE category = 'A' OR category = 'C'");
    assert.equal(r.rows.length, 5);
  });

  it('AND has higher precedence than OR', () => {
    const r = db.execute("SELECT * FROM items WHERE category = 'A' AND active = 1 OR category = 'C' AND active = 1");
    assert.equal(r.rows.length, 3); // Widget, Gizmo, Thingamajig
  });

  it('NOT with OR', () => {
    const r = db.execute("SELECT * FROM items WHERE NOT (category = 'A' OR category = 'B')");
    assert.equal(r.rows.length, 2); // C items only
  });

  it('complex nested conditions', () => {
    const r = db.execute("SELECT * FROM items WHERE (category = 'A' OR category = 'B') AND price > 20 AND active = 1");
    assert.equal(r.rows.length, 3); // Gadget(50), Gizmo(60), Contraption(90)
  });

  it('triple OR', () => {
    const r = db.execute("SELECT * FROM items WHERE price = 10 OR price = 50 OR price = 90");
    assert.equal(r.rows.length, 3);
  });

  it('NOT with AND', () => {
    const r = db.execute("SELECT * FROM items WHERE NOT (active = 0)");
    assert.equal(r.rows.length, 5);
  });

  it('BETWEEN with OR', () => {
    const r = db.execute("SELECT * FROM items WHERE price BETWEEN 10 AND 30 OR price BETWEEN 80 AND 100");
    assert.equal(r.rows.length, 5); // 10,20,30 + 80,90
  });

  it('LIKE with OR', () => {
    const r = db.execute("SELECT * FROM items WHERE name LIKE 'G%' OR name LIKE 'D%'");
    assert.equal(r.rows.length, 4); // Gadget, Gizmo, Doohickey, Doodad
  });

  it('IS NULL with OR', () => {
    db.execute('CREATE TABLE nullable (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO nullable VALUES (1, NULL, 10)');
    db.execute('INSERT INTO nullable VALUES (2, 5, NULL)');
    db.execute('INSERT INTO nullable VALUES (3, NULL, NULL)');
    db.execute('INSERT INTO nullable VALUES (4, 5, 10)');
    const r = db.execute('SELECT * FROM nullable WHERE a IS NULL OR b IS NULL');
    assert.equal(r.rows.length, 3);
  });

  it('complex filter: active, category, price range', () => {
    const r = db.execute("SELECT * FROM items WHERE active = 1 AND (category = 'A' AND price >= 50 OR category = 'B' AND price >= 80)");
    assert.equal(r.rows.length, 2); // Gizmo(A,60), Contraption(B,90)
  });

  it('IN combined with AND/OR', () => {
    const r = db.execute("SELECT * FROM items WHERE category IN ('A', 'C') AND active = 1");
    assert.equal(r.rows.length, 3); // Widget, Gizmo, Thingamajig
  });
});
