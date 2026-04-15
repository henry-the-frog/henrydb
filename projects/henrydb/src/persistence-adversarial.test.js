import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

function roundTrip(db) {
  return Database.fromSerialized(db.serialize());
}

describe('Adversarial Persistence Stress Tests', () => {
  it('FK + SERIAL + INSERT survives round-trip', () => {
    const db = new Database();
    db.execute("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)");
    db.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INTEGER REFERENCES users(id), total INTEGER)");
    db.execute("INSERT INTO users (name) VALUES ('Alice')");
    db.execute("INSERT INTO users (name) VALUES ('Bob')");
    db.execute("INSERT INTO orders (user_id, total) VALUES (1, 100)");
    db.execute("INSERT INTO orders (user_id, total) VALUES (2, 200)");
    
    const db2 = roundTrip(db);
    // SERIAL should continue from 3
    db2.execute("INSERT INTO users (name) VALUES ('Charlie')");
    const r = db2.execute("SELECT * FROM users ORDER BY id");
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[2].id, 3);
    
    // FK should still be enforced
    assert.throws(() => {
      db2.execute("INSERT INTO orders (user_id, total) VALUES (999, 50)");
    });
  });

  it('generated column + index survives double round-trip', () => {
    const db = new Database();
    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL, tax REAL GENERATED ALWAYS AS (price * 0.08) STORED)");
    db.execute("INSERT INTO products VALUES (1, 100.0)");
    db.execute("CREATE INDEX idx_tax ON products (tax)");
    
    const db2 = roundTrip(roundTrip(db));
    db2.execute("INSERT INTO products VALUES (2, 200.0)");
    const r = db2.execute("SELECT * FROM products ORDER BY id");
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[1].tax, 16); // 200 * 0.08
  });

  it('complex schema: CTE + window + sequence after round-trip', () => {
    const db = new Database();
    db.execute("CREATE SEQUENCE order_seq START 1000");
    db.execute("CREATE TABLE transactions (id INTEGER, dept TEXT, amount INTEGER, created TEXT)");
    db.execute("INSERT INTO transactions VALUES (1, 'eng', 500, '2025-01-01')");
    db.execute("INSERT INTO transactions VALUES (2, 'eng', 300, '2025-01-02')");
    db.execute("INSERT INTO transactions VALUES (3, 'sales', 200, '2025-01-01')");
    db.execute("INSERT INTO transactions VALUES (4, 'sales', 400, '2025-01-02')");
    
    const db2 = roundTrip(db);
    
    // CTE + window function should work
    const r = db2.execute(`
      WITH ranked AS (
        SELECT dept, amount, 
               ROW_NUMBER() OVER (PARTITION BY dept ORDER BY amount DESC) as rn
        FROM transactions
      )
      SELECT dept, amount FROM ranked WHERE rn = 1 ORDER BY dept
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].amount, 500); // eng top
    assert.equal(r.rows[1].amount, 400); // sales top
    
    // Sequence should be available
    const seq = db2.execute("SELECT NEXTVAL('order_seq')");
    assert.equal(Object.values(seq.rows[0])[0], 1000);
  });

  it('materialized view + base table mutation after round-trip', () => {
    const db = new Database();
    db.execute("CREATE TABLE metrics (id INTEGER, value INTEGER)");
    db.execute("INSERT INTO metrics VALUES (1, 10)");
    db.execute("INSERT INTO metrics VALUES (2, 20)");
    db.execute("CREATE MATERIALIZED VIEW metrics_agg AS SELECT SUM(value) AS total, COUNT(*) AS cnt FROM metrics");
    
    const db2 = roundTrip(db);
    
    // Mat view should have stale data
    let r = db2.execute("SELECT * FROM metrics_agg");
    assert.equal(r.rows[0].total, 30);
    
    // Mutate base table
    db2.execute("INSERT INTO metrics VALUES (3, 30)");
    
    // Mat view should still show stale data
    r = db2.execute("SELECT * FROM metrics_agg");
    assert.equal(r.rows[0].total, 30);
    
    // After refresh, should be updated
    db2.execute("REFRESH MATERIALIZED VIEW metrics_agg");
    r = db2.execute("SELECT * FROM metrics_agg");
    assert.equal(r.rows[0].total, 60);
  });

  it('10-table schema survives round-trip with all features', () => {
    const db = new Database();
    
    // Create 10 interconnected tables
    db.execute("CREATE SEQUENCE global_seq START 1");
    db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT UNIQUE)");
    db.execute("CREATE TABLE employees (id SERIAL PRIMARY KEY, dept_id INTEGER REFERENCES departments(id), name TEXT, salary INTEGER)");
    db.execute("CREATE TABLE projects (id SERIAL PRIMARY KEY, name TEXT, budget INTEGER)");
    db.execute("CREATE TABLE assignments (emp_id INTEGER, proj_id INTEGER, hours INTEGER, PRIMARY KEY (emp_id, proj_id))");
    db.execute("CREATE TABLE timesheets (id SERIAL PRIMARY KEY, emp_id INTEGER, date TEXT, hours REAL)");
    db.execute("CREATE TABLE expenses (id SERIAL PRIMARY KEY, emp_id INTEGER, amount REAL, category TEXT)");
    db.execute("CREATE TABLE reviews (id SERIAL PRIMARY KEY, emp_id INTEGER, score INTEGER CHECK (score BETWEEN 1 AND 5))");
    db.execute("CREATE TABLE locations (id INTEGER PRIMARY KEY, name TEXT, city TEXT)");
    db.execute("CREATE TABLE dept_locations (dept_id INTEGER, loc_id INTEGER)");
    db.execute("CREATE TABLE audit_log (id SERIAL PRIMARY KEY, action TEXT, entity TEXT, created TEXT)");
    
    // Insert data
    db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
    db.execute("INSERT INTO departments VALUES (2, 'Sales')");
    db.execute("INSERT INTO employees (dept_id, name, salary) VALUES (1, 'Alice', 120000)");
    db.execute("INSERT INTO employees (dept_id, name, salary) VALUES (1, 'Bob', 110000)");
    db.execute("INSERT INTO employees (dept_id, name, salary) VALUES (2, 'Charlie', 90000)");
    db.execute("INSERT INTO projects (name, budget) VALUES ('Alpha', 500000)");
    db.execute("INSERT INTO projects (name, budget) VALUES ('Beta', 300000)");
    db.execute("INSERT INTO assignments VALUES (1, 1, 20)");
    db.execute("INSERT INTO assignments VALUES (2, 1, 30)");
    db.execute("INSERT INTO assignments VALUES (3, 2, 40)");
    db.execute("INSERT INTO reviews (emp_id, score) VALUES (1, 5)");
    db.execute("INSERT INTO reviews (emp_id, score) VALUES (2, 4)");
    db.execute("INSERT INTO reviews (emp_id, score) VALUES (3, 3)");
    
    // Create materialized view
    db.execute("CREATE MATERIALIZED VIEW dept_summary AS SELECT d.name AS dept, COUNT(*) AS emp_count FROM departments d JOIN employees e ON d.id = e.dept_id GROUP BY d.name");
    
    // Add comments
    db.execute("COMMENT ON TABLE employees IS 'Main employee registry'");
    
    const db2 = roundTrip(db);
    
    // Verify complex query works
    const r = db2.execute(`
      SELECT e.name, d.name AS dept, r.score, a.hours
      FROM employees e
      JOIN departments d ON e.dept_id = d.id
      JOIN reviews r ON e.id = r.emp_id
      LEFT JOIN assignments a ON e.id = a.emp_id
      ORDER BY r.score DESC
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].name, 'Alice');
    assert.equal(r.rows[0].score, 5);
    
    // SERIAL should continue
    db2.execute("INSERT INTO employees (dept_id, name, salary) VALUES (2, 'Diana', 95000)");
    const emp = db2.execute("SELECT * FROM employees WHERE name = 'Diana'");
    assert.equal(emp.rows[0].id, 4);
    
    // CHECK constraint should still enforce
    assert.throws(() => {
      db2.execute("INSERT INTO reviews (emp_id, score) VALUES (4, 6)");
    });
    
    // Mat view should work
    const mv = db2.execute("SELECT * FROM dept_summary ORDER BY dept");
    assert.equal(mv.rows.length, 2);
    
    // Comments should persist
    assert.equal(db2._comments.get('table:employees'), 'Main employee registry');
    
    // Sequence should work
    const seq = db2.execute("SELECT NEXTVAL('global_seq')");
    assert.equal(Object.values(seq.rows[0])[0], 1);
  });
});
