// math-expr.test.js — Math and expression evaluation tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Math and Expression Evaluation', () => {
  let db;
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE nums (id INT PRIMARY KEY, a INT, b INT)');
    db.execute('INSERT INTO nums VALUES (1, 10, 3)');
    db.execute('INSERT INTO nums VALUES (2, 20, 7)');
    db.execute('INSERT INTO nums VALUES (3, 15, 5)');
    db.execute('INSERT INTO nums VALUES (4, 0, 10)');
    db.execute('INSERT INTO nums VALUES (5, -5, 2)');
  });

  describe('Arithmetic in SELECT', () => {
    it('addition', () => {
      const result = db.execute('SELECT a + b AS sum FROM nums WHERE id = 1');
      assert.equal(result.rows[0].sum, 13);
    });

    it('subtraction', () => {
      const result = db.execute('SELECT a - b AS diff FROM nums WHERE id = 2');
      assert.equal(result.rows[0].diff, 13);
    });

    it('multiplication', () => {
      const result = db.execute('SELECT a * b AS product FROM nums WHERE id = 3');
      assert.equal(result.rows[0].product, 75);
    });

    it('division', () => {
      const result = db.execute('SELECT a / b AS quot FROM nums WHERE id = 1');
      assert.equal(result.rows[0].quot, 3); // integer division
    });

    it('modulo', () => {
      const result = db.execute('SELECT a % b AS rem FROM nums WHERE id = 1');
      assert.equal(result.rows[0].rem, 1); // 10 % 3 = 1
    });

    it('negative number arithmetic', () => {
      const result = db.execute('SELECT a + b AS sum FROM nums WHERE id = 5');
      assert.equal(result.rows[0].sum, -3); // -5 + 2
    });

    it('zero in arithmetic', () => {
      const result = db.execute('SELECT a * b AS product FROM nums WHERE id = 4');
      assert.equal(result.rows[0].product, 0);
    });
  });

  describe('Expressions in WHERE', () => {
    it('comparison with expression', () => {
      const result = db.execute('SELECT id FROM nums WHERE a + b > 20');
      assert.ok(result.rows.length > 0);
      assert.ok(result.rows.every(r => {
        const row = db.execute(`SELECT a, b FROM nums WHERE id = ${r.id}`).rows[0];
        return row.a + row.b > 20;
      }));
    });

    it('expression on both sides', () => {
      const result = db.execute('SELECT id FROM nums WHERE a * 2 > b * 3');
      assert.ok(result.rows.length > 0);
    });

    it('complex expression chain', () => {
      const result = db.execute('SELECT a, b, a + b AS sum, a - b AS diff, a * b AS prod FROM nums ORDER BY id');
      assert.equal(result.rows.length, 5);
      assert.equal(result.rows[0].sum, 13);
      assert.equal(result.rows[0].diff, 7);
      assert.equal(result.rows[0].prod, 30);
    });
  });

  describe('ABS function', () => {
    it('ABS of negative', () => {
      const result = db.execute('SELECT ABS(a) AS abs_a FROM nums WHERE id = 5');
      assert.equal(result.rows[0].abs_a, 5);
    });

    it('ABS of positive', () => {
      const result = db.execute('SELECT ABS(a) AS abs_a FROM nums WHERE id = 1');
      assert.equal(result.rows[0].abs_a, 10);
    });

    it('ABS of zero', () => {
      const result = db.execute('SELECT ABS(a) AS abs_a FROM nums WHERE id = 4');
      assert.equal(result.rows[0].abs_a, 0);
    });
  });

  describe('Aggregate expressions', () => {
    it('SUM of column', () => {
      const result = db.execute('SELECT SUM(a) AS total FROM nums');
      assert.equal(result.rows[0].total, 40); // 10+20+15+0+(-5)
    });

    it('COUNT with WHERE', () => {
      const result = db.execute('SELECT COUNT(*) AS cnt FROM nums WHERE a > 0');
      assert.equal(result.rows[0].cnt, 3);
    });

    it('MIN and MAX', () => {
      const result = db.execute('SELECT MIN(a) AS mn, MAX(a) AS mx FROM nums');
      assert.equal(result.rows[0].mn, -5);
      assert.equal(result.rows[0].mx, 20);
    });

    it('AVG', () => {
      const result = db.execute('SELECT AVG(a) AS avg_a FROM nums');
      assert.equal(result.rows[0].avg_a, 8); // 40/5
    });

    it('multiple aggregates', () => {
      const result = db.execute('SELECT COUNT(*) AS cnt, SUM(a) AS sum, AVG(b) AS avg_b FROM nums');
      assert.equal(result.rows[0].cnt, 5);
      assert.equal(result.rows[0].sum, 40);
      // AVG(b) = (3+7+5+10+2)/5 = 27/5 = 5.4 or 5 (integer)
    });
  });

  describe('Literal expressions', () => {
    it('computed column with literal', () => {
      const result = db.execute('SELECT id, a * 100 + b AS score FROM nums ORDER BY score DESC');
      assert.equal(result.rows.length, 5);
      assert.equal(result.rows[0].score, 2007); // 20*100+7
    });
  });
});
