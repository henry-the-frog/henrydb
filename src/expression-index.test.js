import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { Database } from './db.js';

describe('Expression Indexes', () => {
  it('CREATE INDEX with LOWER() expression', () => {
    const db = new Database();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
    db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')");
    db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@test.com')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@test.com')");
    
    db.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");
    
    // Verify the index was created
    const table = db.tables.get('users');
    assert.ok(table.indexes.size >= 2, 'Should have at least 2 indexes (PK + expression)');
    assert.ok(table.indexMeta.has('expr_0'), 'Should have expression index metadata');
    const meta = table.indexMeta.get('expr_0');
    assert.ok(meta.expressions, 'Should have expressions array');
    assert.ok(meta.expressions[0], 'First expression should be non-null');
  });

  it('expression index used for equality lookup', () => {
    const db = new Database();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.execute("INSERT INTO users VALUES (3, 'Charlie')");
    
    db.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");
    
    // Query using the expression — should use the expression index
    const result = db.execute("SELECT * FROM users WHERE LOWER(name) = 'alice'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Alice');
  });

  it('expression index maintained on INSERT', () => {
    const db = new Database();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    db.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");
    
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    
    const result = db.execute("SELECT * FROM users WHERE LOWER(name) = 'bob'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].name, 'Bob');
  });

  it('expression index maintained on UPDATE', () => {
    const db = new Database();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");
    
    // Verify initial state
    let result = db.execute("SELECT * FROM users WHERE LOWER(name) = 'alice'");
    assert.equal(result.rows.length, 1);
    
    // Update the name
    db.execute("UPDATE users SET name = 'Alicia' WHERE id = 1");
    
    // Old value should not match
    result = db.execute("SELECT * FROM users WHERE LOWER(name) = 'alice'");
    assert.equal(result.rows.length, 0);
    
    // New value should match
    result = db.execute("SELECT * FROM users WHERE LOWER(name) = 'alicia'");
    assert.equal(result.rows.length, 1);
  });

  it('expression index maintained on DELETE', () => {
    const db = new Database();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)");
    db.execute("INSERT INTO users VALUES (1, 'Alice')");
    db.execute("INSERT INTO users VALUES (2, 'Bob')");
    db.execute("CREATE INDEX idx_lower_name ON users (LOWER(name))");
    
    db.execute("DELETE FROM users WHERE id = 1");
    
    const result = db.execute("SELECT * FROM users WHERE LOWER(name) = 'alice'");
    assert.equal(result.rows.length, 0);
    
    const result2 = db.execute("SELECT * FROM users WHERE LOWER(name) = 'bob'");
    assert.equal(result2.rows.length, 1);
  });

  it('UNIQUE expression index enforces uniqueness', () => {
    const db = new Database();
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT)");
    db.execute("CREATE UNIQUE INDEX idx_lower_email ON users (LOWER(email))");
    
    db.execute("INSERT INTO users VALUES (1, 'Alice@Test.com')");
    
    // Same email in different case should violate uniqueness
    assert.throws(() => {
      db.execute("INSERT INTO users VALUES (2, 'alice@test.com')");
    }, /Duplicate key/);
  });

  it('expression index with arithmetic expression', () => {
    const db = new Database();
    db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price REAL, discount REAL)");
    db.execute("INSERT INTO products VALUES (1, 100, 10)");
    db.execute("INSERT INTO products VALUES (2, 200, 20)");
    db.execute("INSERT INTO products VALUES (3, 150, 30)");
    
    db.execute("CREATE INDEX idx_net_price ON products (price - discount)");
    
    const result = db.execute("SELECT * FROM products WHERE price - discount = 90");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 1);
  });

  it('expression index with UPPER()', () => {
    const db = new Database();
    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT)");
    db.execute("INSERT INTO items VALUES (1, 'abc')");
    db.execute("INSERT INTO items VALUES (2, 'def')");
    
    db.execute("CREATE INDEX idx_upper_code ON items (UPPER(code))");
    
    const result = db.execute("SELECT * FROM items WHERE UPPER(code) = 'ABC'");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].id, 1);
  });

  it('expression index with LENGTH()', () => {
    const db = new Database();
    db.execute("CREATE TABLE words (id INTEGER PRIMARY KEY, word TEXT)");
    db.execute("INSERT INTO words VALUES (1, 'hello')");
    db.execute("INSERT INTO words VALUES (2, 'hi')");
    db.execute("INSERT INTO words VALUES (3, 'hey')");
    
    db.execute("CREATE INDEX idx_word_len ON words (LENGTH(word))");
    
    const result = db.execute("SELECT * FROM words WHERE LENGTH(word) = 5");
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].word, 'hello');
  });

  it('regular column indexes still work after expression index changes', () => {
    const db = new Database();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INTEGER)");
    db.execute("INSERT INTO t VALUES (1, 'x', 10)");
    db.execute("INSERT INTO t VALUES (2, 'y', 20)");
    db.execute("INSERT INTO t VALUES (3, 'z', 30)");
    
    db.execute("CREATE INDEX idx_b ON t (b)");
    db.execute("CREATE INDEX idx_lower_a ON t (LOWER(a))");
    
    // Column index still works
    const r1 = db.execute("SELECT * FROM t WHERE b = 20");
    assert.equal(r1.rows.length, 1);
    assert.equal(r1.rows[0].id, 2);
    
    // Expression index works
    const r2 = db.execute("SELECT * FROM t WHERE LOWER(a) = 'z'");
    assert.equal(r2.rows.length, 1);
    assert.equal(r2.rows[0].id, 3);
  });

  it('partial expression index with WHERE clause', () => {
    const db = new Database();
    db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, status TEXT, total REAL)");
    db.execute("INSERT INTO orders VALUES (1, 'active', 100)");
    db.execute("INSERT INTO orders VALUES (2, 'active', 200)");
    db.execute("INSERT INTO orders VALUES (3, 'cancelled', 150)");
    
    db.execute("CREATE INDEX idx_active_total ON orders (total) WHERE status = 'active'");
    
    // Only active orders should be in the index
    const table = db.tables.get('orders');
    const idx = table.indexes.get('total');
    // Should have 2 entries (only active orders)
    const all = idx.range(-Infinity, Infinity);
    assert.equal(all.length, 2);
  });
});
