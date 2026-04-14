import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

describe('Foreign Key Constraints', () => {
  describe('Column-level REFERENCES', () => {
    it('INSERT respects FK constraint', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      
      // Valid FK
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      assert.equal(db.execute("SELECT * FROM employees").rows.length, 1);
      
      // Invalid FK
      assert.throws(() => {
        db.execute("INSERT INTO employees VALUES (2, 'Bob', 99)");
      }, /Foreign key constraint violated/);
    });

    it('NULL FK values are allowed', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");
      
      // NULL FK should be allowed (no department assigned)
      db.execute("INSERT INTO employees VALUES (1, 'Alice', NULL)");
      assert.equal(db.execute("SELECT * FROM employees").rows.length, 1);
    });

    it('ON DELETE CASCADE', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE)");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO departments VALUES (2, 'Sales')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      db.execute("INSERT INTO employees VALUES (2, 'Bob', 1)");
      db.execute("INSERT INTO employees VALUES (3, 'Charlie', 2)");
      
      // Deleting dept 1 should cascade-delete Alice and Bob
      db.execute("DELETE FROM departments WHERE id = 1");
      const emps = db.execute("SELECT * FROM employees");
      assert.equal(emps.rows.length, 1);
      assert.equal(emps.rows[0].name, 'Charlie');
    });

    it('ON DELETE SET NULL', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id) ON DELETE SET NULL)");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      
      db.execute("DELETE FROM departments WHERE id = 1");
      const emp = db.execute("SELECT * FROM employees WHERE id = 1");
      assert.equal(emp.rows.length, 1);
      assert.equal(emp.rows[0].dept_id, null);
    });

    it('ON DELETE RESTRICT (default)', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      
      assert.throws(() => {
        db.execute("DELETE FROM departments WHERE id = 1");
      }, /Cannot delete.*referenced/);
    });

    it('ON UPDATE CASCADE', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE ON UPDATE CASCADE)");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      
      // ON UPDATE CASCADE would need update-side enforcement
      // For now just verify the parser accepts it
      assert.equal(db.execute("SELECT * FROM employees WHERE dept_id = 1").rows.length, 1);
    });
  });

  describe('Table-level FOREIGN KEY', () => {
    it('table-level FK enforcement on INSERT', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id))");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      assert.equal(db.execute("SELECT * FROM employees").rows.length, 1);
      
      assert.throws(() => {
        db.execute("INSERT INTO employees VALUES (2, 'Bob', 99)");
      }, /Foreign key constraint violated/);
    });

    it('table-level FK with ON DELETE CASCADE', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE CASCADE)");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO departments VALUES (2, 'Sales')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      db.execute("INSERT INTO employees VALUES (2, 'Bob', 2)");
      
      db.execute("DELETE FROM departments WHERE id = 1");
      const emps = db.execute("SELECT * FROM employees");
      assert.equal(emps.rows.length, 1);
      assert.equal(emps.rows[0].dept_id, 2);
    });

    it('table-level FK with ON DELETE RESTRICT', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE RESTRICT)");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      
      assert.throws(() => {
        db.execute("DELETE FROM departments WHERE id = 1");
      }, /Cannot delete.*referenced/);
    });

    it('table-level FK with ON DELETE SET NULL', () => {
      const db = new Database();
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE SET NULL)");
      
      db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      
      db.execute("DELETE FROM departments WHERE id = 1");
      const emp = db.execute("SELECT * FROM employees WHERE id = 1");
      assert.equal(emp.rows[0].dept_id, null);
    });
  });

  describe('Multi-table cascading', () => {
    it('cascading delete through multiple tables', () => {
      const db = new Database();
      db.execute("CREATE TABLE companies (id INTEGER PRIMARY KEY, name TEXT)");
      db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT, company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE)");
      db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id) ON DELETE CASCADE)");
      
      db.execute("INSERT INTO companies VALUES (1, 'Acme')");
      db.execute("INSERT INTO departments VALUES (1, 'Engineering', 1)");
      db.execute("INSERT INTO departments VALUES (2, 'Sales', 1)");
      db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)");
      db.execute("INSERT INTO employees VALUES (2, 'Bob', 2)");
      
      // Deleting the company should cascade through departments to employees
      db.execute("DELETE FROM companies WHERE id = 1");
      assert.equal(db.execute("SELECT * FROM departments").rows.length, 0);
      assert.equal(db.execute("SELECT * FROM employees").rows.length, 0);
    });
  });

  describe('Table-level PRIMARY KEY and UNIQUE', () => {
    it('table-level PRIMARY KEY', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (a INTEGER, b TEXT, PRIMARY KEY (a))");
      db.execute("INSERT INTO t VALUES (1, 'x')");
      db.execute("INSERT INTO t VALUES (2, 'y')");
      assert.equal(db.execute("SELECT * FROM t").rows.length, 2);
    });

    it('table-level UNIQUE', () => {
      const db = new Database();
      db.execute("CREATE TABLE t (a INTEGER PRIMARY KEY, b TEXT, UNIQUE (b))");
      db.execute("INSERT INTO t VALUES (1, 'x')");
      db.execute("INSERT INTO t VALUES (2, 'y')");
      assert.equal(db.execute("SELECT * FROM t").rows.length, 2);
    });
  });
});
