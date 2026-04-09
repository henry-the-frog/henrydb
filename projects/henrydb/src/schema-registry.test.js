// schema-registry.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SchemaRegistry } from './schema-registry.js';

describe('SchemaRegistry', () => {
  it('register and validate', () => {
    const sr = new SchemaRegistry();
    sr.register('users', [{ name: 'id', type: 'int' }, { name: 'name', type: 'string' }]);
    
    assert.equal(sr.validate('users', [1, 'Alice']).valid, true);
    assert.equal(sr.validate('users', ['bad', 'Alice']).valid, false);
  });

  it('list schemas', () => {
    const sr = new SchemaRegistry();
    sr.register('a', []); sr.register('b', []);
    assert.deepEqual(sr.list(), ['a', 'b']);
  });
});
