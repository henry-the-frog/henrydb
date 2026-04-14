import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

describe('HenryDB Feature Showcase', () => {
  it('full e-commerce schema with all features', () => {
    const db = new Database();
    
    // === DDL with all constraint types ===
    db.execute(`CREATE TABLE categories (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      slug TEXT GENERATED ALWAYS AS (LOWER(name)) STORED
    )`);
    
    db.execute(`CREATE TABLE products (
      id INTEGER PRIMARY KEY,
      name TEXT NOT NULL,
      price REAL NOT NULL,
      discount REAL DEFAULT 0,
      net_price REAL GENERATED ALWAYS AS (price - discount) STORED,
      category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
      active INTEGER DEFAULT 1,
      CHECK (price > 0),
      CHECK (discount >= 0),
      CHECK (discount <= price)
    )`);
    
    db.execute(`CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      customer TEXT NOT NULL,
      total REAL DEFAULT 0,
      status TEXT DEFAULT 'pending',
      CHECK (status IN ('pending', 'shipped', 'delivered', 'cancelled'))
    )`);
    
    db.execute(`CREATE TABLE order_items (
      id INTEGER PRIMARY KEY,
      order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
      product_id INTEGER REFERENCES products(id) ON DELETE RESTRICT,
      quantity INTEGER CHECK (quantity > 0),
      unit_price REAL,
      line_total REAL GENERATED ALWAYS AS (quantity * unit_price) STORED
    )`);
    
    // === Indexes (regular + expression) ===
    db.execute('CREATE INDEX idx_product_name ON products (LOWER(name))');
    db.execute('CREATE INDEX idx_net_price ON products (net_price)');
    db.execute('CREATE UNIQUE INDEX idx_category_slug ON categories (slug)');
    db.execute('CREATE INDEX idx_order_status ON orders (status)');
    db.execute('CREATE INDEX idx_oi_order ON order_items (order_id)');
    
    // === INSERT data ===
    db.execute("INSERT INTO categories VALUES (1, 'Electronics')");
    db.execute("INSERT INTO categories VALUES (2, 'Books')");
    db.execute("INSERT INTO categories VALUES (3, 'Clothing')");
    
    db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (1, 'Laptop', 999.99, 100, 1)");
    db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (2, 'Phone', 599.99, 50, 1)");
    db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (3, 'Novel', 24.99, 5, 2)");
    db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (4, 'T-Shirt', 29.99, 0, 3)");
    db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (5, 'Headphones', 149.99, 20, 1)");
    
    db.execute("INSERT INTO orders VALUES (1, 'Alice', 0, 'pending')");
    db.execute("INSERT INTO orders VALUES (2, 'Bob', 0, 'shipped')");
    db.execute("INSERT INTO orders VALUES (3, 'Alice', 0, 'delivered')");
    
    db.execute("INSERT INTO order_items VALUES (1, 1, 1, 1, 899.99)");
    db.execute("INSERT INTO order_items VALUES (2, 1, 3, 2, 19.99)");
    db.execute("INSERT INTO order_items VALUES (3, 2, 2, 1, 549.99)");
    db.execute("INSERT INTO order_items VALUES (4, 3, 4, 3, 29.99)");
    db.execute("INSERT INTO order_items VALUES (5, 3, 5, 1, 129.99)");
    
    // === Verify generated columns ===
    const products = db.execute('SELECT name, price, discount, net_price FROM products ORDER BY net_price DESC');
    assert.equal(products.rows[0].net_price, 899.99); // Laptop
    assert.equal(products.rows[4].net_price, 19.99);  // Novel
    
    const lineItems = db.execute('SELECT product_id, quantity, unit_price, line_total FROM order_items');
    assert.equal(lineItems.rows[0].line_total, 899.99);
    assert.equal(lineItems.rows[3].line_total, 89.97);  // 3 * 29.99
    
    // Category slug generated
    assert.equal(db.execute("SELECT slug FROM categories WHERE id = 1").rows[0].slug, 'electronics');
    
    // === Expression index lookups ===
    const laptop = db.execute("SELECT id FROM products WHERE LOWER(name) = 'laptop'");
    assert.equal(laptop.rows[0].id, 1);
    
    // UNIQUE expression index — duplicate slug should fail
    assert.throws(() => db.execute("INSERT INTO categories VALUES (4, 'electronics')"), /Duplicate/);
    
    // === Complex queries ===
    // JOIN with aggregation
    const orderTotals = db.execute(`
      SELECT o.id, o.customer, SUM(oi.line_total) AS total
      FROM orders o
      JOIN order_items oi ON o.id = oi.order_id
      GROUP BY o.id, o.customer
      ORDER BY total DESC
    `);
    assert.ok(orderTotals.rows.length >= 2);
    
    // CTE: top customers
    const topCustomers = db.execute(`
      WITH customer_spend AS (
        SELECT o.customer, SUM(oi.line_total) AS total_spent
        FROM orders o
        JOIN order_items oi ON o.id = oi.order_id
        GROUP BY o.customer
      )
      SELECT customer, total_spent FROM customer_spend ORDER BY total_spent DESC
    `);
    assert.ok(topCustomers.rows.length >= 1);
    
    // Subquery: products more expensive than average
    const expensive = db.execute('SELECT name FROM products WHERE net_price > (SELECT AVG(net_price) FROM products) ORDER BY net_price DESC');
    assert.ok(expensive.rows.length >= 1);
    
    // DISTINCT ON: latest order per customer
    const latestOrders = db.execute('SELECT DISTINCT ON (customer) customer, status FROM orders ORDER BY customer, id DESC');
    assert.ok(latestOrders.rows.length >= 2);
    
    // Window function
    const ranked = db.execute('SELECT name, net_price, ROW_NUMBER() OVER (ORDER BY net_price DESC) AS rank FROM products');
    assert.equal(ranked.rows[0].rank, 1);
    
    // === FK enforcement ===
    // Can't delete product with order_items
    assert.throws(() => db.execute('DELETE FROM products WHERE id = 1'), /Cannot delete.*referenced/);
    
    // Can't insert order_item for non-existent product
    assert.throws(() => db.execute("INSERT INTO order_items VALUES (10, 1, 99, 1, 100)"), /Foreign key/);
    
    // CASCADE: delete order → deletes order_items
    db.execute('DELETE FROM orders WHERE id = 3');
    assert.equal(db.execute('SELECT * FROM order_items WHERE order_id = 3').rows.length, 0);
    
    // SET NULL: delete category → products.category_id becomes NULL
    db.execute('DELETE FROM categories WHERE id = 2');
    const novel = db.execute('SELECT category_id FROM products WHERE id = 3');
    assert.equal(novel.rows[0].category_id, null);
    
    // === CHECK constraints ===
    assert.throws(() => db.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (10, 'Bad', -1, 0, 1)"), /CHECK/);
    assert.throws(() => db.execute("INSERT INTO orders VALUES (10, 'Test', 0, 'invalid_status')"), /CHECK/);
    
    // === UPDATE with generated column recomputation ===
    db.execute('UPDATE products SET discount = 200 WHERE id = 1');
    assert.equal(db.execute('SELECT net_price FROM products WHERE id = 1').rows[0].net_price, 799.99);
    
    // === Persistence round-trip ===
    const db2 = Database.fromSerialized(db.save());
    
    // Verify everything survived
    assert.equal(db2.execute('SELECT COUNT(*) AS cnt FROM products').rows[0].cnt, 5);
    assert.equal(db2.execute("SELECT slug FROM categories WHERE id = 1").rows[0].slug, 'electronics');
    assert.equal(db2.execute("SELECT id FROM products WHERE LOWER(name) = 'phone'").rows[0].id, 2);
    
    // FK still works after restore
    assert.throws(() => db2.execute('DELETE FROM products WHERE id = 2'), /Cannot delete/);
    
    // CHECK still works
    assert.throws(() => db2.execute("INSERT INTO products (id, name, price, discount, category_id) VALUES (20, 'Bad', -5, 0, 1)"), /CHECK/);
  });
});
