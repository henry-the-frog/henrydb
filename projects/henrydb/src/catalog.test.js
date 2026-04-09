// catalog.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Catalog } from './catalog.js';

describe('Catalog', () => {
  it('create and list tables', () => {
    const cat = new Catalog();
    cat.createTable('users', [{ name: 'id', type: 'int' }, { name: 'name', type: 'varchar' }]);
    assert.deepEqual(cat.listTables(), ['users']);
    assert.equal(cat.getColumnType('users', 'id'), 'int');
  });

  it('indexes', () => {
    const cat = new Catalog();
    cat.createTable('users', []);
    cat.createIndex('users', 'idx_name', ['name'], 'btree');
    assert.equal(cat.getIndexes('users').length, 1);
    assert.equal(cat.getIndexes('users')[0].type, 'btree');
  });
});
