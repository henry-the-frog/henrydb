// table-schema.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TableSchema } from './table-schema.js';

describe('TableSchema', () => {
  it('create schema', () => {
    const schema = new TableSchema('users', [
      { name: 'id', type: 'INT', primaryKey: true },
      { name: 'name', type: 'VARCHAR', nullable: false },
      { name: 'email', type: 'VARCHAR', unique: true },
      { name: 'active', type: 'BOOLEAN', defaultValue: true },
    ]);
    assert.equal(schema.name, 'users');
    assert.equal(schema.columns.length, 4);
  });

  it('validates correct row', () => {
    const schema = new TableSchema('t', [
      { name: 'id', type: 'INT' },
      { name: 'name', type: 'VARCHAR' },
    ]);
    assert.deepEqual(schema.validateRow({ id: 1, name: 'Alice' }), []);
  });

  it('catches type mismatch', () => {
    const schema = new TableSchema('t', [{ name: 'id', type: 'INT' }]);
    const errors = schema.validateRow({ id: 'not a number' });
    assert.ok(errors.length > 0);
  });

  it('catches NOT NULL violation', () => {
    const schema = new TableSchema('t', [{ name: 'id', type: 'INT', nullable: false }]);
    const errors = schema.validateRow({ id: null });
    assert.ok(errors.length > 0);
  });

  it('applies defaults', () => {
    const schema = new TableSchema('t', [
      { name: 'id', type: 'INT' },
      { name: 'active', type: 'BOOLEAN', defaultValue: true },
    ]);
    const row = schema.applyDefaults({ id: 1 });
    assert.equal(row.active, true);
  });

  it('primary key detection', () => {
    const schema = new TableSchema('t', [
      { name: 'id', type: 'INT', primaryKey: true },
      { name: 'name', type: 'VARCHAR' },
    ]);
    assert.deepEqual(schema.primaryKey, ['id']);
  });

  it('toSQL generates DDL', () => {
    const schema = new TableSchema('users', [
      { name: 'id', type: 'INT', primaryKey: true },
      { name: 'name', type: 'VARCHAR', nullable: false },
    ]);
    const sql = schema.toSQL();
    assert.ok(sql.includes('CREATE TABLE users'));
    assert.ok(sql.includes('PRIMARY KEY'));
    assert.ok(sql.includes('NOT NULL'));
  });

  it('rejects invalid type', () => {
    assert.throws(() => new TableSchema('t', [{ name: 'x', type: 'INVALID_TYPE' }]));
  });

  it('rejects duplicate column names', () => {
    assert.throws(() => new TableSchema('t', [
      { name: 'id', type: 'INT' },
      { name: 'id', type: 'VARCHAR' },
    ]));
  });

  it('allows nullable columns', () => {
    const schema = new TableSchema('t', [{ name: 'x', type: 'INT', nullable: true }]);
    assert.deepEqual(schema.validateRow({ x: null }), []);
  });
});
