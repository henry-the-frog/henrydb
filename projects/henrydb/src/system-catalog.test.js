// system-catalog.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { SystemCatalog } from './system-catalog.js';

// Mock database with tables
function createMockDb() {
  const tables = new Map();
  tables.set('users', {
    columns: [
      { name: 'id', type: 'INTEGER', notNull: true },
      { name: 'name', type: 'TEXT' },
      { name: 'age', type: 'INTEGER' },
    ],
    rows: [{ id: 1, name: 'Alice', age: 30 }, { id: 2, name: 'Bob', age: 25 }],
  });
  tables.set('orders', {
    columns: [
      { name: 'id', type: 'INTEGER', notNull: true },
      { name: 'user_id', type: 'INTEGER' },
      { name: 'total', type: 'NUMERIC' },
    ],
    rows: [{ id: 1, user_id: 1, total: 99.99 }],
  });

  const indexes = new Map();
  indexes.set('idx_users_name', { table: 'users', columns: ['name'], unique: false });
  indexes.set('pk_users', { table: 'users', columns: ['id'], unique: true, primary: true });

  return { _tables: tables, _indexes: indexes };
}

let catalog, db;

describe('SystemCatalog', () => {
  beforeEach(() => {
    db = createMockDb();
    catalog = new SystemCatalog(db);
  });

  test('pg_class lists tables', () => {
    const result = catalog.query('pg_class');
    const tables = result.rows.filter(r => r.relkind === 'r');
    assert.equal(tables.length, 2);
    assert.ok(tables.some(t => t.relname === 'users'));
    assert.ok(tables.some(t => t.relname === 'orders'));
  });

  test('pg_class lists indexes', () => {
    const result = catalog.query('pg_class');
    const indexes = result.rows.filter(r => r.relkind === 'i');
    assert.equal(indexes.length, 2);
    assert.ok(indexes.some(i => i.relname === 'idx_users_name'));
  });

  test('pg_class with filter', () => {
    const result = catalog.query('pg_class', { relkind: 'r' });
    assert.ok(result.rows.every(r => r.relkind === 'r'));
  });

  test('pg_attribute lists columns', () => {
    const result = catalog.query('pg_attribute');
    const userCols = result.rows.filter(r => r.attrelid === 'users');
    assert.equal(userCols.length, 3);
    assert.ok(userCols.some(c => c.attname === 'id'));
    assert.ok(userCols.some(c => c.attname === 'name'));
  });

  test('pg_attribute maps types to OIDs', () => {
    const result = catalog.query('pg_attribute');
    const idCol = result.rows.find(r => r.attrelid === 'users' && r.attname === 'id');
    assert.equal(idCol.atttypid, 23); // INTEGER
    const nameCol = result.rows.find(r => r.attrelid === 'users' && r.attname === 'name');
    assert.equal(nameCol.atttypid, 25); // TEXT
  });

  test('pg_type lists data types', () => {
    const result = catalog.query('pg_type');
    assert.ok(result.rows.length > 10);
    assert.ok(result.rows.some(t => t.typname === 'integer'));
    assert.ok(result.rows.some(t => t.typname === 'text'));
    assert.ok(result.rows.some(t => t.typname === 'boolean'));
  });

  test('pg_index lists indexes', () => {
    const result = catalog.query('pg_index');
    assert.equal(result.rows.length, 2);
    const pk = result.rows.find(r => r.indexrelid === 'pk_users');
    assert.ok(pk.indisprimary);
    assert.ok(pk.indisunique);
  });

  test('pg_namespace lists schemas', () => {
    const result = catalog.query('pg_namespace');
    assert.ok(result.rows.some(r => r.nspname === 'public'));
    assert.ok(result.rows.some(r => r.nspname === 'pg_catalog'));
  });

  test('pg_database returns henrydb', () => {
    const result = catalog.query('pg_database');
    assert.equal(result.rows[0].datname, 'henrydb');
  });

  test('information_schema.tables', () => {
    const result = catalog.query('information_schema.tables');
    assert.equal(result.rows.length, 2);
    assert.ok(result.rows.every(r => r.table_schema === 'public'));
    assert.ok(result.rows.every(r => r.table_type === 'BASE TABLE'));
  });

  test('information_schema.columns', () => {
    const result = catalog.query('information_schema.columns');
    const userCols = result.rows.filter(r => r.table_name === 'users');
    assert.equal(userCols.length, 3);
    assert.ok(userCols.some(c => c.column_name === 'id' && c.is_nullable === 'NO'));
    assert.ok(userCols.some(c => c.column_name === 'name' && c.is_nullable === 'YES'));
  });

  test('information_schema.columns with filter', () => {
    const result = catalog.query('information_schema.columns', { table_name: 'orders' });
    assert.equal(result.rows.length, 3);
    assert.ok(result.rows.every(r => r.table_name === 'orders'));
  });

  test('pg_stat_user_tables', () => {
    const result = catalog.query('pg_stat_user_tables');
    assert.equal(result.rows.length, 2);
    const users = result.rows.find(r => r.relname === 'users');
    assert.equal(users.n_live_tup, 2);
  });

  test('unknown table throws', () => {
    assert.throws(() => catalog.query('pg_nonexistent'), /not found/);
  });

  test('OID generation', () => {
    const oid1 = catalog.nextOid();
    const oid2 = catalog.nextOid();
    assert.ok(oid2 > oid1);
  });
});
