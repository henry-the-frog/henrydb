// compiled-update.test.js — Verify compiled SET expressions in UPDATE pipeline
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { compileSetExpr, compileSetBatch } from './where-compiler.js';

describe('Compiled SET Expressions', () => {
  describe('compileSetExpr unit tests', () => {
    it('compiles literal value', () => {
      const fn = compileSetExpr({ type: 'literal', value: 42 });
      assert.ok(fn);
      assert.equal(fn({}), 42);
    });

    it('compiles string literal', () => {
      const fn = compileSetExpr({ type: 'literal', value: 'hello' });
      assert.ok(fn);
      assert.equal(fn({}), 'hello');
    });

    it('compiles null literal', () => {
      const fn = compileSetExpr({ type: 'literal', value: null });
      assert.ok(fn);
      assert.equal(fn({}), null);
    });

    it('compiles column reference', () => {
      const fn = compileSetExpr({ type: 'column_ref', name: 'price' });
      assert.ok(fn);
      assert.equal(fn({ price: 99.50 }), 99.50);
    });

    it('compiles addition', () => {
      const fn = compileSetExpr({
        op: '+',
        left: { type: 'column_ref', name: 'x' },
        right: { type: 'literal', value: 10 }
      });
      assert.ok(fn);
      assert.equal(fn({ x: 5 }), 15);
    });

    it('compiles multiplication', () => {
      const fn = compileSetExpr({
        op: '*',
        left: { type: 'column_ref', name: 'price' },
        right: { type: 'literal', value: 1.1 }
      });
      assert.ok(fn);
      assert.ok(Math.abs(fn({ price: 100 }) - 110) < 0.001, `Expected close to 110, got ${fn({ price: 100 })}`);
    });

    it('compiles nested arithmetic', () => {
      // (a + b) * 2
      const fn = compileSetExpr({
        op: '*',
        left: {
          op: '+',
          left: { type: 'column_ref', name: 'a' },
          right: { type: 'column_ref', name: 'b' }
        },
        right: { type: 'literal', value: 2 }
      });
      assert.ok(fn);
      assert.equal(fn({ a: 3, b: 7 }), 20);
    });

    it('returns null for empty CASE expressions', () => {
      const fn = compileSetExpr({ type: 'CASE', whens: [] });
      assert.equal(fn, null);
    });

    it('compiles valid CASE expressions', () => {
      const fn = compileSetExpr({
        type: 'CASE',
        whens: [
          { when: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'x' }, right: { type: 'literal', value: 10 } }, then: { type: 'literal', value: 'big' } },
          { when: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'x' }, right: { type: 'literal', value: 5 } }, then: { type: 'literal', value: 'medium' } }
        ],
        else: { type: 'literal', value: 'small' }
      });
      assert.ok(fn);
      assert.equal(fn({ x: 15 }), 'big');
      assert.equal(fn({ x: 7 }), 'medium');
      assert.equal(fn({ x: 2 }), 'small');
    });

    it('returns null for function calls', () => {
      const fn = compileSetExpr({ type: 'function_call', func: 'UNKNOWN_FUNC', args: [] });
      assert.equal(fn, null);
    });

    it('compiles UPPER function', () => {
      const fn = compileSetExpr({ type: 'function_call', func: 'UPPER', args: [{ type: 'column_ref', name: 'name' }] });
      assert.ok(fn);
      assert.equal(fn({ name: 'hello' }), 'HELLO');
    });

    it('compiles ABS function', () => {
      const fn = compileSetExpr({ type: 'function_call', func: 'ABS', args: [{ type: 'column_ref', name: 'val' }] });
      assert.ok(fn);
      assert.equal(fn({ val: -42 }), 42);
    });

    it('compiles COALESCE function', () => {
      const fn = compileSetExpr({ type: 'function_call', func: 'COALESCE', args: [{ type: 'literal', value: null }, { type: 'column_ref', name: 'x' }] });
      assert.ok(fn);
      assert.equal(fn({ x: 'fallback' }), 'fallback');
    });
  });

  describe('compileSetBatch', () => {
    const schema = [
      { name: 'id' },
      { name: 'name' },
      { name: 'price' },
      { name: 'quantity' }
    ];

    it('compiles single assignment', () => {
      const result = compileSetBatch(
        [{ column: 'price', value: { type: 'literal', value: 50 } }],
        schema
      );
      assert.ok(result);
      assert.equal(result.length, 1);
      assert.equal(result[0].colIdx, 2);
      assert.equal(result[0].fn({}), 50);
    });

    it('compiles multiple assignments', () => {
      const result = compileSetBatch(
        [
          { column: 'price', value: { op: '*', left: { type: 'column_ref', name: 'price' }, right: { type: 'literal', value: 2 } } },
          { column: 'quantity', value: { type: 'literal', value: 0 } }
        ],
        schema
      );
      assert.ok(result);
      assert.equal(result.length, 2);
      assert.equal(result[0].fn({ price: 25 }), 50);
      assert.equal(result[1].fn({}), 0);
    });

    it('returns null for unknown column', () => {
      const result = compileSetBatch(
        [{ column: 'nonexistent', value: { type: 'literal', value: 1 } }],
        schema
      );
      assert.equal(result, null);
    });

    it('returns null if any expression unsupported', () => {
      const result = compileSetBatch(
        [
          { column: 'price', value: { type: 'literal', value: 1 } },
          { column: 'name', value: { type: 'function_call', name: 'UPPER' } }
        ],
        schema
      );
      assert.equal(result, null);
    });
  });

  describe('UPDATE integration (compiled path)', () => {
    let db;

    beforeEach(() => {
      db = new Database();
      db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price REAL, stock INT)');
      db.execute("INSERT INTO products VALUES (1, 'Widget', 9.99, 100)");
      db.execute("INSERT INTO products VALUES (2, 'Gadget', 29.99, 50)");
      db.execute("INSERT INTO products VALUES (3, 'Doohickey', 4.99, 200)");
    });

    it('simple SET literal', () => {
      db.execute('UPDATE products SET stock = 0 WHERE id = 1');
      const r = db.execute('SELECT stock FROM products WHERE id = 1');
      assert.equal(r.rows[0].stock, 0);
    });

    it('SET with column arithmetic', () => {
      db.execute('UPDATE products SET price = price * 1.1');
      const r = db.execute('SELECT price FROM products WHERE id = 1');
      assert.ok(Math.abs(r.rows[0].price - 10.989) < 0.01, `Expected close to 10.989, got ${r.rows[0].price}`);
    });

    it('SET with addition and WHERE', () => {
      db.execute('UPDATE products SET stock = stock + 50 WHERE price > 10');
      const r = db.execute('SELECT stock FROM products WHERE id = 2');
      assert.equal(r.rows[0].stock, 100);
      // id=1 (price=9.99) should be unchanged
      const r2 = db.execute('SELECT stock FROM products WHERE id = 1');
      assert.equal(r2.rows[0].stock, 100);
    });

    it('multiple SET columns', () => {
      db.execute('UPDATE products SET price = price - 1, stock = stock + 10 WHERE id = 3');
      const r = db.execute('SELECT price, stock FROM products WHERE id = 3');
      assert.ok(Math.abs(r.rows[0].price - 3.99) < 0.01, `Expected close to 3.99, got ${r.rows[0].price}`);
      assert.equal(r.rows[0].stock, 210);
    });

    it('SET all rows', () => {
      db.execute('UPDATE products SET stock = 0');
      const r = db.execute('SELECT SUM(stock) as total FROM products');
      assert.equal(r.rows[0].total, 0);
    });

    it('large batch UPDATE uses compiled path efficiently', () => {
      // Insert many rows
      for (let i = 4; i <= 1000; i++) {
        db.execute(`INSERT INTO products VALUES (${i}, 'Item${i}', ${i * 0.5}, ${i})`);
      }
      const start = Date.now();
      db.execute('UPDATE products SET price = price * 2, stock = stock - 1');
      const elapsed = Date.now() - start;
      // Compiled path should handle 1000 rows quickly
      const r = db.execute('SELECT price, stock FROM products WHERE id = 500');
      assert.ok(Math.abs(r.rows[0].price - 500) < 0.01, `Expected close to 500, got ${r.rows[0].price}`);
      assert.equal(r.rows[0].stock, 499);
      // Should be fast (< 500ms for 1000 rows)
      assert.ok(elapsed < 500, `UPDATE took ${elapsed}ms`);
    });
  });
});
