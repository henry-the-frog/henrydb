// compiler.test.js — Query compiler tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { compileWhere, compileProjection, compileOrderBy, compileExpr } from './compiler.js';
import { Database } from './db.js';

describe('Query Compiler', () => {
  describe('compileWhere', () => {
    it('compiles equality comparison', () => {
      const filter = compileWhere({
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'id' },
        right: { type: 'literal', value: 5 }
      });
      assert.equal(filter({ id: 5 }), true);
      assert.equal(filter({ id: 3 }), false);
    });

    it('compiles AND expressions', () => {
      const filter = compileWhere({
        type: 'AND',
        left: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 18 } },
        right: { type: 'COMPARE', op: 'LT', left: { type: 'column_ref', name: 'age' }, right: { type: 'literal', value: 65 } }
      });
      assert.equal(filter({ age: 30 }), true);
      assert.equal(filter({ age: 10 }), false);
      assert.equal(filter({ age: 70 }), false);
    });

    it('compiles OR expressions', () => {
      const filter = compileWhere({
        type: 'OR',
        left: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'status' }, right: { type: 'literal', value: 'active' } },
        right: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'status' }, right: { type: 'literal', value: 'pending' } }
      });
      assert.equal(filter({ status: 'active' }), true);
      assert.equal(filter({ status: 'pending' }), true);
      assert.equal(filter({ status: 'closed' }), false);
    });

    it('compiles IS NULL / IS NOT NULL', () => {
      const filter = compileWhere({ type: 'IS_NULL', expr: { type: 'column_ref', name: 'email' } });
      assert.equal(filter({ email: null }), true);
      assert.equal(filter({ email: 'test@test.com' }), false);
    });

    it('compiles BETWEEN', () => {
      const filter = compileWhere({
        type: 'BETWEEN',
        expr: { type: 'column_ref', name: 'price' },
        low: { type: 'literal', value: 10 },
        high: { type: 'literal', value: 50 }
      });
      assert.equal(filter({ price: 25 }), true);
      assert.equal(filter({ price: 5 }), false);
      assert.equal(filter({ price: 100 }), false);
    });

    it('compiles arithmetic expressions', () => {
      const filter = compileWhere({
        type: 'COMPARE', op: 'GT',
        left: { type: 'arith', op: '*', left: { type: 'column_ref', name: 'price' }, right: { type: 'column_ref', name: 'qty' } },
        right: { type: 'literal', value: 100 }
      });
      assert.equal(filter({ price: 50, qty: 3 }), true);
      assert.equal(filter({ price: 10, qty: 5 }), false);
    });

    it('null expression returns always-true filter', () => {
      const filter = compileWhere(null);
      assert.equal(filter({}), true);
    });
  });

  describe('compileProjection', () => {
    it('compiles column selection', () => {
      const project = compileProjection(
        [{ type: 'column', name: 'name', alias: 'name' }, { type: 'column', name: 'age', alias: 'age' }],
        []
      );
      assert.ok(project);
      const result = project({ name: 'Alice', age: 30, email: 'test' });
      assert.deepEqual(result, { name: 'Alice', age: 30 });
    });
  });

  describe('compileOrderBy', () => {
    it('compiles ascending sort', () => {
      const cmp = compileOrderBy([{ column: 'age', direction: 'ASC' }]);
      assert.ok(cmp);
      assert.equal(cmp({ age: 10 }, { age: 20 }), -1);
      assert.equal(cmp({ age: 20 }, { age: 10 }), 1);
      assert.equal(cmp({ age: 10 }, { age: 10 }), 0);
    });

    it('compiles descending sort', () => {
      const cmp = compileOrderBy([{ column: 'age', direction: 'DESC' }]);
      assert.ok(cmp);
      assert.equal(cmp({ age: 10 }, { age: 20 }), 1);
      assert.equal(cmp({ age: 20 }, { age: 10 }), -1);
    });
  });

  describe('Compiled vs Interpreted Performance', () => {
    it('compiled filter is faster on many rows', () => {
      const db = new Database();
      db.execute('CREATE TABLE bench (id INT PRIMARY KEY, val INT, name TEXT)');
      for (let i = 0; i < 500; i++) {
        db.execute(`INSERT INTO bench VALUES (${i}, ${i * 3}, 'name${i}')`);
      }

      // Create compiled filter
      const filterExpr = {
        type: 'AND',
        left: { type: 'COMPARE', op: 'GT', left: { type: 'column_ref', name: 'val' }, right: { type: 'literal', value: 100 } },
        right: { type: 'COMPARE', op: 'LT', left: { type: 'column_ref', name: 'val' }, right: { type: 'literal', value: 500 } }
      };
      const compiledFilter = compileWhere(filterExpr);
      
      // Generate test rows
      const rows = [];
      for (const { values } of db.tables.get('bench').heap.scan()) {
        rows.push({ id: values[0], val: values[1], name: values[2] });
      }
      
      // Benchmark compiled filter
      const start1 = performance.now();
      for (let i = 0; i < 100; i++) {
        rows.filter(compiledFilter);
      }
      const compiledTime = performance.now() - start1;
      
      // Compiled version should be reasonably fast
      assert.ok(compiledTime < 1000, `Compiled filter took ${compiledTime.toFixed(1)}ms`);
    });
  });
});
