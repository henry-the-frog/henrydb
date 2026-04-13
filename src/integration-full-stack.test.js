// integration-full-stack.test.js — Full-stack integration test
// Exercises: CREATE TABLE/VIEW/TRIGGER, PK/FK/UNIQUE/CHECK/DEFAULT/NOT NULL,
// INSERT/UPDATE/DELETE, sessions, savepoints, MVCC visibility, persistence
import { describe, it, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdirSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dir, db;

function fresh() {
  dir = join(tmpdir(), `henrydb-integ-${Date.now()}-${Math.random().toString(36).slice(2)}`);
  mkdirSync(dir, { recursive: true });
  return TransactionalDatabase.open(dir);
}

function cleanup() {
  try { db?.close(); } catch {}
  try { rmSync(dir, { recursive: true, force: true }); } catch {}
}

describe('Full-Stack Integration Test', () => {
  afterEach(cleanup);

  it('complete e-commerce scenario with all features', () => {
    db = fresh();
    
    // 1. Schema creation with all constraint types
    db.execute('CREATE TABLE categories (id INT PRIMARY KEY, name TEXT NOT NULL)');
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT NOT NULL, category_id INT REFERENCES categories(id), price INT CHECK (price > 0), stock INT DEFAULT 0)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, total INT DEFAULT 0)');
    db.execute('CREATE TABLE order_items (id INT, order_id INT REFERENCES orders(id) ON DELETE CASCADE, product_id INT REFERENCES products(id), quantity INT CHECK (quantity > 0))');
    db.execute('CREATE TABLE order_log (msg TEXT)');
    
    // 2. Create views and triggers
    db.execute('CREATE VIEW expensive_products AS SELECT * FROM products WHERE price > 100');
    db.execute("CREATE TRIGGER log_order AFTER INSERT ON orders INSERT INTO order_log VALUES ('new order')");
    
    // 3. Populate data
    db.execute("INSERT INTO categories VALUES (1, 'Electronics')");
    db.execute("INSERT INTO categories VALUES (2, 'Books')");
    db.execute("INSERT INTO products VALUES (1, 'Laptop', 1, 999, 10)");
    db.execute("INSERT INTO products VALUES (2, 'Phone', 1, 599, 20)");
    db.execute("INSERT INTO products VALUES (3, 'Novel', 2, 15, 100)");
    
    // 4. Test constraints
    assert.throws(() => db.execute("INSERT INTO products VALUES (4, 'Bad', 99, 10, 0)"), /foreign key/i); // FK violation
    assert.throws(() => db.execute("INSERT INTO products VALUES (4, 'Free', 1, 0, 0)"), /check/i); // CHECK violation
    assert.throws(() => db.execute("INSERT INTO products VALUES (1, 'Dup', 1, 10, 0)"), /duplicate|unique/i); // PK violation
    
    // 5. View works
    const expensive = db.execute('SELECT * FROM expensive_products');
    assert.equal(expensive.rows.length, 2); // Laptop and Phone
    
    // 6. Session transaction with savepoints
    const s = db.session();
    s.begin();
    s.execute('INSERT INTO orders VALUES (1, 0)');
    s.execute('INSERT INTO order_items VALUES (1, 1, 1, 2)'); // 2 laptops
    s.execute('SAVEPOINT before_phone');
    s.execute('INSERT INTO order_items VALUES (2, 1, 2, 1)'); // 1 phone
    
    // Check within session
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM order_items').rows[0].cnt, 2);
    
    // Rollback the phone
    s.execute('ROLLBACK TO before_phone');
    assert.equal(s.execute('SELECT COUNT(*) as cnt FROM order_items').rows[0].cnt, 1);
    
    s.commit();
    s.close();
    
    // Trigger should have fired
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM order_log').rows[0].cnt, 1);
    
    // 7. Concurrent session
    const reader = db.session();
    const writer = db.session();
    reader.begin();
    writer.begin();
    
    // Reader takes snapshot
    const stock = reader.execute('SELECT stock FROM products WHERE id = 1').rows[0].stock;
    assert.equal(stock, 10);
    
    // Writer reduces stock
    writer.execute('UPDATE products SET stock = stock - 2 WHERE id = 1');
    writer.commit();
    
    // Reader still sees old stock (snapshot isolation)
    assert.equal(reader.execute('SELECT stock FROM products WHERE id = 1').rows[0].stock, 10);
    reader.commit();
    
    // New query sees updated stock
    assert.equal(db.execute('SELECT stock FROM products WHERE id = 1').rows[0].stock, 8);
    
    reader.close();
    writer.close();
    
    // 8. CASCADE DELETE
    db.execute('DELETE FROM orders WHERE id = 1');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM order_items').rows[0].cnt, 0);
    
    // 9. Close and reopen — everything should persist
    db.close();
    db = TransactionalDatabase.open(dir);
    
    // Data persists
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM products').rows[0].cnt, 3);
    assert.equal(db.execute('SELECT stock FROM products WHERE id = 1').rows[0].stock, 8);
    
    // Constraints persist
    assert.throws(() => db.execute("INSERT INTO products VALUES (1, 'Dup', 1, 10, 0)"), /duplicate|unique/i);
    assert.throws(() => db.execute("INSERT INTO products VALUES (4, 'Bad', 99, 10, 0)"), /foreign key/i);
    
    // View persists
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM expensive_products').rows[0].cnt, 2);
    
    // Trigger persists
    db.execute('INSERT INTO orders VALUES (2, 500)');
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM order_log').rows[0].cnt, 2);
    
    // 10. ALTER TABLE
    db.execute('ALTER TABLE products ADD COLUMN discount INT DEFAULT 0');
    db.execute('UPDATE products SET discount = 10 WHERE id = 1');
    assert.equal(db.execute('SELECT discount FROM products WHERE id = 1').rows[0].discount, 10);
    
    // Close and reopen again
    db.close();
    db = TransactionalDatabase.open(dir);
    
    // ALTER persists
    db.execute('UPDATE products SET discount = 20 WHERE id = 2');
    assert.equal(db.execute('SELECT discount FROM products WHERE id = 2').rows[0].discount, 20);
    
    // Final count
    const totalProducts = db.execute('SELECT COUNT(*) as cnt FROM products').rows[0].cnt;
    assert.equal(totalProducts, 3, 'All products should survive double reopen');
  });
});
