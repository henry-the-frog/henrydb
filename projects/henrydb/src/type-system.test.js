// type-system.test.js — Tests for SQL type system
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SQLType, resolveAffinity, binaryResultType, aggregateResultType, 
         functionResultType, canCoerce, TypeChecker } from './type-system.js';

describe('SQL Type System', () => {

  describe('resolveAffinity', () => {
    it('resolves INTEGER types', () => {
      assert.equal(resolveAffinity('INTEGER'), SQLType.INTEGER);
      assert.equal(resolveAffinity('INT'), SQLType.INTEGER);
      assert.equal(resolveAffinity('BIGINT'), SQLType.INTEGER);
      assert.equal(resolveAffinity('SMALLINT'), SQLType.INTEGER);
    });

    it('resolves REAL types', () => {
      assert.equal(resolveAffinity('REAL'), SQLType.REAL);
      assert.equal(resolveAffinity('FLOAT'), SQLType.REAL);
      assert.equal(resolveAffinity('DOUBLE'), SQLType.REAL);
      assert.equal(resolveAffinity('NUMERIC'), SQLType.REAL);
    });

    it('resolves TEXT types', () => {
      assert.equal(resolveAffinity('TEXT'), SQLType.TEXT);
      assert.equal(resolveAffinity('VARCHAR(255)'), SQLType.TEXT);
      assert.equal(resolveAffinity('CHAR(10)'), SQLType.TEXT);
    });

    it('resolves special types', () => {
      assert.equal(resolveAffinity('BOOLEAN'), SQLType.BOOLEAN);
      assert.equal(resolveAffinity('JSON'), SQLType.JSON);
      assert.equal(resolveAffinity('DATE'), SQLType.DATE);
    });

    it('defaults to TEXT for unknown types', () => {
      assert.equal(resolveAffinity('FOOBAR'), SQLType.TEXT);
    });
  });

  describe('binaryResultType', () => {
    it('arithmetic: INT + INT = INT', () => {
      assert.equal(binaryResultType(SQLType.INTEGER, SQLType.INTEGER, '+'), SQLType.INTEGER);
    });

    it('arithmetic: INT + REAL = REAL', () => {
      assert.equal(binaryResultType(SQLType.INTEGER, SQLType.REAL, '*'), SQLType.REAL);
    });

    it('comparison returns BOOLEAN', () => {
      assert.equal(binaryResultType(SQLType.INTEGER, SQLType.INTEGER, '='), SQLType.BOOLEAN);
      assert.equal(binaryResultType(SQLType.TEXT, SQLType.TEXT, 'LIKE'), SQLType.BOOLEAN);
    });

    it('NULL propagation', () => {
      assert.equal(binaryResultType(SQLType.NULL, SQLType.INTEGER, '+'), SQLType.NULL);
    });

    it('string concatenation returns TEXT', () => {
      assert.equal(binaryResultType(SQLType.TEXT, SQLType.TEXT, '||'), SQLType.TEXT);
    });

    it('boolean operations return BOOLEAN', () => {
      assert.equal(binaryResultType(SQLType.BOOLEAN, SQLType.BOOLEAN, 'AND'), SQLType.BOOLEAN);
    });
  });

  describe('aggregateResultType', () => {
    it('COUNT always returns INTEGER', () => {
      assert.equal(aggregateResultType('COUNT', SQLType.TEXT), SQLType.INTEGER);
    });

    it('SUM preserves INTEGER', () => {
      assert.equal(aggregateResultType('SUM', SQLType.INTEGER), SQLType.INTEGER);
    });

    it('SUM promotes to REAL', () => {
      assert.equal(aggregateResultType('SUM', SQLType.REAL), SQLType.REAL);
    });

    it('AVG always returns REAL', () => {
      assert.equal(aggregateResultType('AVG', SQLType.INTEGER), SQLType.REAL);
    });

    it('MIN/MAX preserve input type', () => {
      assert.equal(aggregateResultType('MIN', SQLType.TEXT), SQLType.TEXT);
      assert.equal(aggregateResultType('MAX', SQLType.INTEGER), SQLType.INTEGER);
    });
  });

  describe('functionResultType', () => {
    it('string functions return TEXT', () => {
      assert.equal(functionResultType('UPPER'), SQLType.TEXT);
      assert.equal(functionResultType('LOWER'), SQLType.TEXT);
    });

    it('math functions return appropriate types', () => {
      assert.equal(functionResultType('ABS'), SQLType.INTEGER);
      assert.equal(functionResultType('ROUND'), SQLType.REAL);
      assert.equal(functionResultType('SQRT'), SQLType.REAL);
    });

    it('JSON functions return JSON', () => {
      assert.equal(functionResultType('JSON_EXTRACT'), SQLType.JSON);
    });
  });

  describe('canCoerce', () => {
    it('same types always coerce', () => {
      assert.ok(canCoerce(SQLType.INTEGER, SQLType.INTEGER));
    });

    it('NULL coerces to anything', () => {
      assert.ok(canCoerce(SQLType.NULL, SQLType.INTEGER));
      assert.ok(canCoerce(SQLType.NULL, SQLType.TEXT));
    });

    it('INT → REAL (safe widening)', () => {
      assert.ok(canCoerce(SQLType.INTEGER, SQLType.REAL));
    });

    it('anything → TEXT', () => {
      assert.ok(canCoerce(SQLType.INTEGER, SQLType.TEXT));
      assert.ok(canCoerce(SQLType.REAL, SQLType.TEXT));
    });

    it('REAL → INT is not allowed', () => {
      assert.ok(!canCoerce(SQLType.REAL, SQLType.INTEGER));
    });
  });

  describe('TypeChecker', () => {
    it('infers literal types', () => {
      const tc = new TypeChecker();
      assert.equal(tc.inferType({ type: 'literal', value: 42 }), SQLType.INTEGER);
      assert.equal(tc.inferType({ type: 'literal', value: 3.14 }), SQLType.REAL);
      assert.equal(tc.inferType({ type: 'literal', value: 'hello' }), SQLType.TEXT);
      assert.equal(tc.inferType({ type: 'literal', value: null }), SQLType.NULL);
    });

    it('infers column types from schema', () => {
      const tc = new TypeChecker({ users: { id: SQLType.INTEGER, name: SQLType.TEXT } });
      assert.equal(tc.inferType({ type: 'column_ref', name: 'id' }), SQLType.INTEGER);
      assert.equal(tc.inferType({ type: 'column_ref', name: 'name' }), SQLType.TEXT);
    });

    it('infers comparison as BOOLEAN', () => {
      const tc = new TypeChecker();
      assert.equal(tc.inferType({ type: 'COMPARE', left: {}, right: {} }), SQLType.BOOLEAN);
    });

    it('infers CAST type', () => {
      const tc = new TypeChecker();
      assert.equal(tc.inferType({ type: 'CAST', targetType: 'INTEGER' }), SQLType.INTEGER);
    });
  });
});
