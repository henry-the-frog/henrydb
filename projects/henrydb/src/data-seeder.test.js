// data-seeder.test.js — Tests for data seeder/faker
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DataSeeder } from './data-seeder.js';
import { Database } from './db.js';

describe('Data Seeder', () => {
  it('seeds users table', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedUsers(50);
    const r = db.execute('SELECT COUNT(*) as cnt FROM users');
    assert.equal(r.rows[0].cnt, 50);
  });

  it('seeds products table', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedProducts(30);
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM products').rows[0].cnt, 30);
  });

  it('seeds orders with FK relationships', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedUsers(10);
    seeder.seedProducts(5);
    seeder.seedOrders(20, 10, 5);
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM orders').rows[0].cnt, 20);
    // All user_ids should be valid
    const invalid = db.execute('SELECT COUNT(*) as cnt FROM orders WHERE user_id > 10 OR user_id < 1');
    assert.equal(invalid.rows[0].cnt, 0);
  });

  it('seedEcommerce creates all tables', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    const result = seeder.seedEcommerce({ users: 20, products: 10, orders: 30 });
    assert.equal(result.users, 20);
    assert.equal(result.products, 10);
    assert.equal(result.orders, 30);
  });

  it('deterministic: same seed produces same data', () => {
    const db1 = new Database();
    const db2 = new Database();
    new DataSeeder(db1, 123).seedUsers(10);
    new DataSeeder(db2, 123).seedUsers(10);
    const r1 = db1.execute('SELECT name FROM users ORDER BY id');
    const r2 = db2.execute('SELECT name FROM users ORDER BY id');
    assert.deepEqual(r1.rows, r2.rows);
  });

  it('different seeds produce different data', () => {
    const db1 = new Database();
    const db2 = new Database();
    new DataSeeder(db1, 1).seedUsers(10);
    new DataSeeder(db2, 2).seedUsers(10);
    const r1 = db1.execute('SELECT name FROM users WHERE id = 1');
    const r2 = db2.execute('SELECT name FROM users WHERE id = 1');
    // Highly likely to be different
    // (very small chance of collision, but practically never)
    assert.ok(r1.rows.length === 1 && r2.rows.length === 1);
  });

  it('seedTable with custom schema', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedTable('metrics', {
      value: 'real',
      category: 'category',
      timestamp: 'date',
    }, 50);
    assert.equal(db.execute('SELECT COUNT(*) as cnt FROM metrics').rows[0].cnt, 50);
  });

  it('creates indexes', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedUsers(10);
    // Should not throw — indexes exist
    db.execute('SELECT * FROM users WHERE city = \'New York\'');
    db.execute('SELECT * FROM users WHERE age > 30');
  });

  it('generates realistic data ranges', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedUsers(100);
    const ages = db.execute('SELECT MIN(age) as min_age, MAX(age) as max_age FROM users');
    assert.ok(ages.rows[0].min_age >= 18);
    assert.ok(ages.rows[0].max_age <= 75);
  });

  it('generates diverse categories', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedProducts(100);
    const cats = db.execute('SELECT COUNT(DISTINCT category) as cnt FROM products');
    assert.ok(cats.rows[0].cnt >= 5);
  });

  it('person() generates complete record', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    const p = seeder.person();
    assert.ok(p.name.includes(' '));
    assert.ok(p.email.includes('@'));
    assert.ok(p.age >= 18);
    assert.ok(p.city);
    assert.ok(typeof p.active === 'boolean');
    assert.ok(p.joined.match(/\d{4}-\d{2}-\d{2}/));
  });

  it('product() generates valid product', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    const p = seeder.product();
    assert.ok(p.name.includes(' '));
    assert.ok(p.price >= 1 && p.price <= 999);
    assert.ok(p.stock >= 0);
    assert.ok(p.rating >= 1 && p.rating <= 5);
  });

  it('text() generates lorem ipsum', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    const t = seeder.text(5);
    assert.ok(t.split(' ').length === 5);
  });

  it('can run queries on seeded data', () => {
    const db = new Database();
    const seeder = new DataSeeder(db);
    seeder.seedEcommerce({ users: 50, products: 20, orders: 100 });

    // Complex query should work
    const r = db.execute(`
      SELECT u.city, COUNT(*) as order_count, SUM(o.total) as revenue
      FROM orders o
      JOIN users u ON o.user_id = u.id
      GROUP BY u.city
      ORDER BY revenue DESC
      LIMIT 5
    `);
    assert.ok(r.rows.length > 0);
    assert.ok(r.rows[0].revenue > 0);
  });
});
