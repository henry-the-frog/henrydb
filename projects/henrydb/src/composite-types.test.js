// composite-types.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { CompositeTypeManager } from './composite-types.js';

let ctm;

describe('CompositeTypeManager', () => {
  beforeEach(() => { ctm = new CompositeTypeManager(); });

  test('create composite type', () => {
    const info = ctm.create('address', [
      { name: 'street', type: 'TEXT' },
      { name: 'city', type: 'TEXT' },
      { name: 'zip', type: 'TEXT' },
    ]);
    assert.equal(info.name, 'address');
    assert.equal(info.fields.length, 3);
  });

  test('create value from array', () => {
    ctm.create('point', [{ name: 'x', type: 'FLOAT' }, { name: 'y', type: 'FLOAT' }]);
    const val = ctm.createValue('point', [1.5, 2.5]);
    assert.equal(val.x, 1.5);
    assert.equal(val.y, 2.5);
  });

  test('create value from object', () => {
    ctm.create('point', [{ name: 'x', type: 'FLOAT' }, { name: 'y', type: 'FLOAT' }]);
    const val = ctm.createValue('point', { x: 3, y: 4 });
    assert.equal(val.x, 3);
    assert.equal(val.y, 4);
  });

  test('get field', () => {
    ctm.create('person', [{ name: 'first', type: 'TEXT' }, { name: 'last', type: 'TEXT' }]);
    const val = ctm.createValue('person', ['John', 'Doe']);
    assert.equal(ctm.getField('person', val, 'first'), 'John');
    assert.equal(ctm.getField('person', val, 'last'), 'Doe');
  });

  test('compare composite values', () => {
    ctm.create('point', [{ name: 'x', type: 'INT' }, { name: 'y', type: 'INT' }]);
    const a = ctm.createValue('point', [1, 2]);
    const b = ctm.createValue('point', [1, 3]);
    const c = ctm.createValue('point', [1, 2]);
    
    assert.equal(ctm.compare('point', a, b), -1);
    assert.equal(ctm.compare('point', b, a), 1);
    assert.equal(ctm.compare('point', a, c), 0);
  });

  test('toString ROW format', () => {
    ctm.create('item', [{ name: 'id', type: 'INT' }, { name: 'name', type: 'TEXT' }]);
    const val = ctm.createValue('item', [1, 'Widget']);
    const str = ctm.toString('item', val);
    assert.equal(str, '(1,"Widget")');
  });

  test('NULL field handling', () => {
    ctm.create('item', [{ name: 'id', type: 'INT' }, { name: 'name', type: 'TEXT' }]);
    const val = ctm.createValue('item', [1, null]);
    const str = ctm.toString('item', val);
    assert.equal(str, '(1,)');
  });

  test('ALTER TYPE ADD ATTRIBUTE', () => {
    ctm.create('person', [{ name: 'name', type: 'TEXT' }]);
    ctm.alter('person', { type: 'ADD_ATTRIBUTE', name: 'age', dataType: 'INTEGER' });
    
    const info = ctm.list().find(t => t.name === 'person');
    assert.equal(info.fields.length, 2);
  });

  test('ALTER TYPE DROP ATTRIBUTE', () => {
    ctm.create('person', [
      { name: 'name', type: 'TEXT' },
      { name: 'age', type: 'INTEGER' },
    ]);
    ctm.alter('person', { type: 'DROP_ATTRIBUTE', name: 'age' });
    
    const info = ctm.list().find(t => t.name === 'person');
    assert.equal(info.fields.length, 1);
  });

  test('DROP TYPE', () => {
    ctm.create('temp', [{ name: 'x', type: 'INT' }]);
    ctm.drop('temp');
    assert.ok(!ctm.has('temp'));
  });

  test('DROP IF EXISTS', () => {
    assert.equal(ctm.drop('nonexistent', true), false);
  });

  test('duplicate create throws', () => {
    ctm.create('pt', [{ name: 'x', type: 'INT' }]);
    assert.throws(() => ctm.create('pt', [{ name: 'y', type: 'INT' }]), /already exists/);
  });

  test('missing fields default to null', () => {
    ctm.create('item', [
      { name: 'a', type: 'INT' },
      { name: 'b', type: 'INT' },
      { name: 'c', type: 'INT' },
    ]);
    const val = ctm.createValue('item', [1]);
    assert.equal(val.a, 1);
    assert.equal(val.b, null);
    assert.equal(val.c, null);
  });

  test('case-insensitive', () => {
    ctm.create('MyType', [{ name: 'x', type: 'INT' }]);
    assert.ok(ctm.has('mytype'));
  });

  test('list types', () => {
    ctm.create('a', [{ name: 'x', type: 'INT' }]);
    ctm.create('b', [{ name: 'y', type: 'TEXT' }]);
    assert.equal(ctm.list().length, 2);
  });
});
