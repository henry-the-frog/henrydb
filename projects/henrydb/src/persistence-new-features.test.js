import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

function roundTrip(db) {
  return Database.fromSerialized(db.serialize());
}

describe('Persistence: New Feature Round-Trip', () => {
  describe('Sequences', () => {
    it('sequence survives round-trip with correct state', () => {
      const db = new Database();
      db.execute("CREATE SEQUENCE test_seq START 10 INCREMENT 5");
      db.execute("SELECT NEXTVAL('test_seq')"); // 10
      db.execute("SELECT NEXTVAL('test_seq')"); // 15
      
      const db2 = roundTrip(db);
      const r = db2.execute("SELECT NEXTVAL('test_seq')");
      const val = Object.values(r.rows[0])[0];
      assert.equal(val, 20);
    });

    it('SERIAL column survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE items (id SERIAL PRIMARY KEY, name TEXT)");
      db.execute("INSERT INTO items (name) VALUES ('alpha')");
      db.execute("INSERT INTO items (name) VALUES ('beta')");
      
      const db2 = roundTrip(db);
      db2.execute("INSERT INTO items (name) VALUES ('gamma')");
      const r = db2.execute("SELECT * FROM items ORDER BY id");
      assert.equal(r.rows.length, 3);
      assert.equal(r.rows[2].id, 3);
      assert.equal(r.rows[2].name, 'gamma');
    });

    it('CURRVAL works after round-trip', () => {
      const db = new Database();
      db.execute("CREATE SEQUENCE my_seq");
      db.execute("SELECT NEXTVAL('my_seq')"); // 1
      
      const db2 = roundTrip(db);
      const r = db2.execute("SELECT CURRVAL('my_seq')");
      const val = Object.values(r.rows[0])[0];
      assert.equal(val, 1);
    });
  });

  describe('Materialized Views', () => {
    it('materialized view survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE sales (id INTEGER, amount INTEGER)");
      db.execute("INSERT INTO sales VALUES (1, 100)");
      db.execute("INSERT INTO sales VALUES (2, 200)");
      db.execute("CREATE MATERIALIZED VIEW sales_summary AS SELECT SUM(amount) AS total FROM sales");
      
      const db2 = roundTrip(db);
      const r = db2.execute("SELECT * FROM sales_summary");
      assert.equal(r.rows[0].total, 300);
    });
  });

  describe('Comments', () => {
    it('table comments survive round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE users (id INTEGER, name TEXT)");
      db.execute("COMMENT ON TABLE users IS 'Main user table'");
      
      const db2 = roundTrip(db);
      assert.equal(db2._comments.get('table:users'), 'Main user table');
    });
  });
});
