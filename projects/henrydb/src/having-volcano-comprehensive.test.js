import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Volcano HAVING comprehensive (2026-04-22)', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE sales (id INT, product TEXT, amount INT, discount INT, region TEXT)');
    db.execute("INSERT INTO sales VALUES (1,'Widget',100,NULL,'East')");
    db.execute("INSERT INTO sales VALUES (2,'Widget',200,10,'West')");
    db.execute("INSERT INTO sales VALUES (3,'Gadget',150,NULL,'East')");
    db.execute("INSERT INTO sales VALUES (4,'Gadget',300,20,'East')");
    db.execute("INSERT INTO sales VALUES (5,'Widget',50,NULL,'East')");
    db.execute("INSERT INTO sales VALUES (6,'Doohickey',80,5,'West')");
  });

  it('HAVING AND: compound filter', () => {
    const r = db.execute('SELECT product, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY product HAVING SUM(amount) > 100 AND COUNT(*) >= 2');
    assert.equal(r.rows.length, 2); // Widget (350, 3) and Gadget (450, 2)
    assert.ok(r.rows.every(row => row.total > 100 && row.cnt >= 2));
  });

  it('HAVING OR: either condition', () => {
    const r = db.execute('SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) > 400 OR COUNT(*) = 1');
    assert.equal(r.rows.length, 2); // Gadget (450) and Doohickey (cnt=1)
    const products = r.rows.map(row => row.product).sort();
    assert.deepEqual(products, ['Doohickey', 'Gadget']);
  });

  it('HAVING NOT: negated condition', () => {
    const r = db.execute("SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING NOT (product = 'Doohickey')");
    assert.ok(r.rows.every(row => row.product !== 'Doohickey'));
    assert.equal(r.rows.length, 2);
  });

  it('HAVING BETWEEN: range on aggregate', () => {
    const r = db.execute('SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) BETWEEN 100 AND 400');
    assert.equal(r.rows.length, 1); // Widget (350)
    assert.equal(r.rows[0].product, 'Widget');
  });

  it('HAVING NOT BETWEEN', () => {
    const r = db.execute('SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) NOT BETWEEN 100 AND 400');
    assert.equal(r.rows.length, 2); // Gadget (450) and Doohickey (80)
  });

  it('HAVING IN_LIST: membership test', () => {
    const r = db.execute("SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING product IN ('Widget', 'Gadget')");
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.every(row => ['Widget', 'Gadget'].includes(row.product)));
  });

  it('HAVING NOT IN: negated membership', () => {
    const r = db.execute("SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING product NOT IN ('Doohickey')");
    assert.equal(r.rows.length, 2);
    assert.ok(r.rows.every(row => row.product !== 'Doohickey'));
  });

  it('HAVING IS NOT NULL: aggregate null check', () => {
    // AVG(discount) — Widget: 10, Gadget: 20, Doohickey: 5 (all non-null aggregates because NULL rows are excluded from AVG)
    const r = db.execute('SELECT product, AVG(discount) AS avg_disc FROM sales GROUP BY product HAVING AVG(discount) IS NOT NULL');
    assert.ok(r.rows.length >= 1);
  });

  it('HAVING LIKE: pattern match on group key', () => {
    const r = db.execute("SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING product LIKE 'G%'");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].product, 'Gadget');
  });

  it('HAVING NOT LIKE', () => {
    const r = db.execute("SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING product NOT LIKE 'W%'");
    assert.equal(r.rows.length, 2); // Gadget, Doohickey
    assert.ok(r.rows.every(row => !row.product.startsWith('W')));
  });

  it('HAVING COALESCE-wrapped aggregate', () => {
    const r = db.execute('SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING COALESCE(SUM(discount), 0) > 5');
    // Widget: SUM(discount)=10 > 5 ✓, Gadget: 20 > 5 ✓, Doohickey: 5 not > 5
    assert.equal(r.rows.length, 2);
    const products = r.rows.map(row => row.product).sort();
    assert.deepEqual(products, ['Gadget', 'Widget']);
  });

  it('HAVING aggregate not in SELECT', () => {
    const r = db.execute('SELECT product FROM sales GROUP BY product HAVING MAX(amount) > 100');
    // Widget: MAX=200, Gadget: MAX=300, Doohickey: MAX=80
    assert.equal(r.rows.length, 2);
    const products = r.rows.map(row => row.product).sort();
    assert.deepEqual(products, ['Gadget', 'Widget']);
  });

  it('HAVING arithmetic expression: SUM/COUNT', () => {
    const r = db.execute('SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) / COUNT(*) > 100');
    // Widget: 350/3=116.7, Gadget: 450/2=225, Doohickey: 80/1=80
    assert.equal(r.rows.length, 2);
    const products = r.rows.map(row => row.product).sort();
    assert.deepEqual(products, ['Gadget', 'Widget']);
  });

  it('HAVING with multiple aggregates not in SELECT', () => {
    const r = db.execute('SELECT product FROM sales GROUP BY product HAVING SUM(amount) > 200 AND MAX(discount) >= 10');
    // Widget: SUM=350 > 200, MAX(disc)=10 >= 10 ✓
    // Gadget: SUM=450 > 200, MAX(disc)=20 >= 10 ✓
    // Doohickey: SUM=80 not > 200
    assert.equal(r.rows.length, 2);
  });
});
