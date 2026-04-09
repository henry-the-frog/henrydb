// enum-types.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { EnumManager } from './enum-types.js';

let em;

describe('EnumManager', () => {
  beforeEach(() => { em = new EnumManager(); });

  test('create enum type', () => {
    const info = em.create('mood', ['happy', 'sad', 'neutral']);
    assert.equal(info.name, 'mood');
    assert.deepEqual(info.values, ['happy', 'sad', 'neutral']);
    assert.ok(em.has('mood'));
  });

  test('validate enum values', () => {
    em.create('color', ['red', 'green', 'blue']);
    assert.ok(em.validate('color', 'red'));
    assert.ok(em.validate('color', 'blue'));
    assert.ok(!em.validate('color', 'yellow'));
  });

  test('enum ordering', () => {
    em.create('size', ['small', 'medium', 'large', 'xlarge']);
    assert.equal(em.compare('size', 'small', 'large'), -1);
    assert.equal(em.compare('size', 'large', 'small'), 1);
    assert.equal(em.compare('size', 'medium', 'medium'), 0);
  });

  test('ADD VALUE at end', () => {
    em.create('status', ['draft', 'published']);
    em.addValue('status', 'archived');
    assert.deepEqual(em.getValues('status'), ['draft', 'published', 'archived']);
  });

  test('ADD VALUE BEFORE', () => {
    em.create('status', ['draft', 'published']);
    em.addValue('status', 'review', { before: 'published' });
    assert.deepEqual(em.getValues('status'), ['draft', 'review', 'published']);
  });

  test('ADD VALUE AFTER', () => {
    em.create('status', ['draft', 'published']);
    em.addValue('status', 'review', { after: 'draft' });
    assert.deepEqual(em.getValues('status'), ['draft', 'review', 'published']);
  });

  test('ADD VALUE IF NOT EXISTS', () => {
    em.create('status', ['draft']);
    em.addValue('status', 'draft', { ifNotExists: true }); // No error
    assert.equal(em.getValues('status').length, 1);
  });

  test('ADD duplicate VALUE throws', () => {
    em.create('status', ['draft']);
    assert.throws(() => em.addValue('status', 'draft'), /already exists/);
  });

  test('RENAME VALUE', () => {
    em.create('color', ['red', 'green', 'blue']);
    em.renameValue('color', 'red', 'crimson');
    assert.ok(!em.validate('color', 'red'));
    assert.ok(em.validate('color', 'crimson'));
    assert.deepEqual(em.getValues('color'), ['crimson', 'green', 'blue']);
  });

  test('DROP TYPE', () => {
    em.create('temp', ['a', 'b']);
    em.drop('temp');
    assert.ok(!em.has('temp'));
  });

  test('DROP IF EXISTS', () => {
    assert.equal(em.drop('nonexistent', true), false);
  });

  test('duplicate create throws', () => {
    em.create('color', ['red']);
    assert.throws(() => em.create('color', ['blue']), /already exists/);
  });

  test('compare invalid value throws', () => {
    em.create('size', ['small', 'large']);
    assert.throws(() => em.compare('size', 'small', 'medium'), /not a valid value/);
  });

  test('ordering preserved after ADD VALUE', () => {
    em.create('priority', ['low', 'high']);
    em.addValue('priority', 'medium', { after: 'low' });
    assert.equal(em.compare('priority', 'low', 'medium'), -1);
    assert.equal(em.compare('priority', 'medium', 'high'), -1);
  });

  test('list enum types', () => {
    em.create('a', ['x', 'y']);
    em.create('b', ['1', '2']);
    assert.equal(em.list().length, 2);
  });

  test('case-insensitive type names', () => {
    em.create('MyEnum', ['a', 'b']);
    assert.ok(em.has('myenum'));
    assert.ok(em.validate('MYENUM', 'a'));
  });
});
