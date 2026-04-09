// schema-management.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { SchemaManager } from './schema-management.js';

let sm;

describe('SchemaManager', () => {
  beforeEach(() => { sm = new SchemaManager(); });

  test('built-in schemas exist', () => {
    assert.ok(sm.has('public'));
    assert.ok(sm.has('pg_catalog'));
    assert.ok(sm.has('information_schema'));
  });

  test('CREATE SCHEMA', () => {
    const info = sm.create('app', { owner: 'henry' });
    assert.equal(info.name, 'app');
    assert.equal(info.owner, 'henry');
  });

  test('IF NOT EXISTS', () => {
    sm.create('app');
    const info = sm.create('app', { ifNotExists: true });
    assert.ok(info);
  });

  test('duplicate create throws', () => {
    sm.create('app');
    assert.throws(() => sm.create('app'), /already exists/);
  });

  test('DROP SCHEMA', () => {
    sm.create('temp');
    sm.drop('temp');
    assert.ok(!sm.has('temp'));
  });

  test('DROP non-empty RESTRICT throws', () => {
    sm.create('app');
    sm.registerObject('app', 'users', 'table');
    assert.throws(() => sm.drop('app'), /contains objects/);
  });

  test('DROP CASCADE', () => {
    sm.create('app');
    sm.registerObject('app', 'users', 'table');
    sm.drop('app', { cascade: true });
    assert.ok(!sm.has('app'));
  });

  test('DROP built-in throws', () => {
    assert.throws(() => sm.drop('public'), /Cannot drop built-in/);
  });

  test('ALTER SCHEMA RENAME', () => {
    sm.create('old_name');
    sm.alter('old_name', { renameTo: 'new_name' });
    assert.ok(!sm.has('old_name'));
    assert.ok(sm.has('new_name'));
  });

  test('ALTER SCHEMA OWNER', () => {
    sm.create('app');
    sm.alter('app', { owner: 'admin' });
    assert.equal(sm.getInfo('app').owner, 'admin');
  });

  test('resolve unqualified name via search_path', () => {
    sm.registerObject('public', 'users', 'table');
    const result = sm.resolve('users');
    assert.equal(result.schema, 'public');
    assert.equal(result.name, 'users');
  });

  test('resolve qualified name', () => {
    sm.create('app');
    sm.registerObject('app', 'orders', 'table');
    const result = sm.resolve('app.orders');
    assert.equal(result.schema, 'app');
    assert.equal(result.name, 'orders');
  });

  test('search_path priority', () => {
    sm.create('app');
    sm.registerObject('public', 'users', 'table');
    sm.registerObject('app', 'users', 'table');
    sm.setSearchPath(['app', 'public']);

    const result = sm.resolve('users');
    assert.equal(result.schema, 'app'); // app is first in path
  });

  test('pg_catalog always searched first', () => {
    sm.registerObject('pg_catalog', 'pg_class', 'table');
    sm.registerObject('public', 'pg_class', 'table');
    
    const result = sm.resolve('pg_class');
    assert.equal(result.schema, 'pg_catalog');
  });

  test('resolve returns null for unknown object', () => {
    assert.equal(sm.resolve('nonexistent'), null);
  });

  test('getDefaultSchema', () => {
    assert.equal(sm.getDefaultSchema(), 'public');
    sm.create('app');
    sm.setSearchPath(['app', 'public']);
    assert.equal(sm.getDefaultSchema(), 'app');
  });

  test('registerObject and unregisterObject', () => {
    sm.registerObject('public', 'test_table', 'table');
    assert.ok(sm.resolve('test_table'));
    sm.unregisterObject('public', 'test_table');
    assert.equal(sm.resolve('test_table'), null);
  });

  test('list schemas', () => {
    const initialCount = sm.list().length;
    sm.create('s1');
    sm.create('s2');
    assert.equal(sm.list().length, initialCount + 2);
  });

  test('case-insensitive', () => {
    sm.create('MySchema');
    assert.ok(sm.has('myschema'));
  });
});
