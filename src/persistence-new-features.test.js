import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

// Test that new features survive serialize/fromSerialized round-trip
function roundTrip(db) {
  return Database.fromSerialized(db.save());
}

describe('Persistence: New Feature Round-Trip', () => {
  describe('Expression Indexes', () => {
    it('expression index survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("INSERT INTO t VALUES (1, 'Alice')");
      db.execute("INSERT INTO t VALUES (2, 'Bob')");
      db.execute("CREATE INDEX idx_lower ON t (LOWER(name))");
      
      const db2 = roundTrip(db);
      const r = db2.execute("SELECT * FROM t WHERE LOWER(name) = 'alice'");
      assert.equal(r.rows.length, 1);
      assert.equal(r.rows[0].name, 'Alice');
    });

    it('UNIQUE expression index survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)");
      db.execute("CREATE UNIQUE INDEX idx_lower_email ON t (LOWER(email))");
      db.execute("INSERT INTO t VALUES (1, 'Alice@Test.com')");
      
      const db2 = roundTrip(db);
      assert.throws(() => {
        db2.execute("INSERT INTO t VALUES (2, 'alice@test.com')");
      }, /Duplicate key/);
    });
  });

  describe('Generated Columns', () => {
    it('STORED generated column survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      const db2 = roundTrip(db);
      const r = db2.execute("SELECT total FROM products");
      assert.equal(r.rows[0].total, 110);
    });

    it('generated column recomputes on UPDATE after round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      db.execute("INSERT INTO products (price, tax) VALUES (100, 10)");
      
      const db2 = roundTrip(db);
      db2.execute("UPDATE products SET price = 200");
      const r = db2.execute("SELECT total FROM products");
      assert.equal(r.rows[0].total, 210);
    });

    it('prevents INSERT to generated column after round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE products (price REAL, tax REAL, total REAL GENERATED ALWAYS AS (price + tax) STORED)");
      
      const db2 = roundTrip(db);
      assert.throws(() => {
        db2.execute("INSERT INTO products (price, tax, total) VALUES (100, 10, 999)");
      }, /Cannot INSERT.*generated/);
    });
  });

  describe('Foreign Keys', () => {
    it('FK RESTRICT survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id))");
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 1)");
      
      const db2 = roundTrip(db);
      assert.throws(() => {
        db2.execute("DELETE FROM departments WHERE id = 1");
      }, /Cannot delete.*referenced/);
    });

    it('FK CASCADE survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE)");
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 1)");
      db.execute("INSERT INTO employees VALUES (2, 1)");
      
      const db2 = roundTrip(db);
      db2.execute("DELETE FROM departments WHERE id = 1");
      assert.equal(db2.execute("SELECT * FROM employees").rows.length, 0);
    });

    it('FK SET NULL survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id) ON DELETE SET NULL)");
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 1)");
      
      const db2 = roundTrip(db);
      db2.execute("DELETE FROM departments WHERE id = 1");
      const emp = db2.execute("SELECT * FROM employees");
      assert.equal(emp.rows[0].dept_id, null);
    });

    it('FK validation on INSERT survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER REFERENCES departments(id))");
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      
      const db2 = roundTrip(db);
      assert.throws(() => {
        db2.execute("INSERT INTO employees VALUES (1, 99)");
      }, /Foreign key constraint violated/);
    });

    it('table-level FK survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE CASCADE)");
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 1)");
      
      const db2 = roundTrip(db);
      db2.execute("DELETE FROM departments WHERE id = 1");
      assert.equal(db2.execute("SELECT * FROM employees").rows.length, 0);
    });
  });

  describe('Table-level CHECK', () => {
    it('CHECK constraint survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (a REAL, b REAL, CHECK (a + b >= 0))");
      db.execute("INSERT INTO t VALUES (100, 50)");
      
      const db2 = roundTrip(db);
      assert.throws(() => {
        db2.execute("INSERT INTO t VALUES (-200, 50)");
      }, /CHECK constraint/);
    });

    it('column-level CHECK survives round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (price REAL CHECK (price > 0))");
      db.execute("INSERT INTO t VALUES (100)");
      
      const db2 = roundTrip(db);
      assert.throws(() => {
        db2.execute("INSERT INTO t VALUES (-10)");
      }, /CHECK constraint/);
    });
  });

  describe('Combined Features', () => {
    it('all features together survive round-trip', () => {
      const db = new Database();
      db.execute("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, discount REAL, net_price REAL GENERATED ALWAYS AS (price - discount) STORED, category_id INTEGER REFERENCES categories(id) ON DELETE CASCADE, CHECK (price > 0), CHECK (discount >= 0))");
      db.execute("CREATE INDEX idx_lower_name ON products (LOWER(name))");
      db.execute("CREATE INDEX idx_net ON products (net_price)");
      
      db.execute("INSERT INTO categories VALUES (1, 'Electronics')");
      db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (1, 'Laptop', 999, 100, 1)");
      db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (2, 'Phone', 599, 50, 1)");
      
      const db2 = roundTrip(db);
      
      // Generated column
      assert.equal(db2.execute("SELECT net_price FROM products WHERE id = 1").rows[0].net_price, 899);
      
      // Expression index
      assert.equal(db2.execute("SELECT id FROM products WHERE LOWER(name) = 'laptop'").rows[0].id, 1);
      
      // CHECK
      assert.throws(() => db2.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (3, 'Bad', -10, 0, 1)"), /CHECK/);
      
      // FK
      assert.throws(() => db2.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (3, 'Orphan', 100, 0, 99)"), /Foreign key/);
      
      // CASCADE
      db2.execute("DELETE FROM categories WHERE id = 1");
      assert.equal(db2.execute("SELECT * FROM products").rows.length, 0);
    });
  });
});
