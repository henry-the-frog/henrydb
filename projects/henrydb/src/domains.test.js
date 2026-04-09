// domains.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { DomainManager } from './domains.js';

let dm;

describe('DomainManager', () => {
  beforeEach(() => { dm = new DomainManager(); });

  test('create domain', () => {
    const info = dm.create('email', 'TEXT');
    assert.equal(info.name, 'email');
    assert.equal(info.baseType, 'TEXT');
    assert.ok(dm.has('email'));
  });

  test('domain with NOT NULL', () => {
    dm.create('positive_int', 'INTEGER', { notNull: true });
    assert.ok(dm.validate('positive_int', 42).valid);
    assert.ok(!dm.validate('positive_int', null).valid);
  });

  test('domain with CHECK constraint', () => {
    dm.create('age', 'INTEGER', {
      check: (v) => v >= 0 && v <= 150,
      constraintName: 'valid_age',
    });
    assert.ok(dm.validate('age', 25).valid);
    assert.ok(!dm.validate('age', -1).valid);
    assert.ok(!dm.validate('age', 200).valid);
  });

  test('domain with DEFAULT', () => {
    dm.create('status', 'TEXT', { default: 'active' });
    assert.equal(dm.getDefault('status', null), 'active');
    assert.equal(dm.getDefault('status', 'inactive'), 'inactive');
  });

  test('multiple constraints', () => {
    dm.create('score', 'INTEGER');
    dm.alter('score', { type: 'ADD_CONSTRAINT', name: 'min_score', check: v => v >= 0 });
    dm.alter('score', { type: 'ADD_CONSTRAINT', name: 'max_score', check: v => v <= 100 });
    
    assert.ok(dm.validate('score', 50).valid);
    assert.ok(!dm.validate('score', -5).valid);
    assert.ok(!dm.validate('score', 105).valid);
  });

  test('ALTER SET NOT NULL', () => {
    dm.create('name', 'TEXT');
    assert.ok(dm.validate('name', null).valid);
    
    dm.alter('name', { type: 'SET_NOT_NULL' });
    assert.ok(!dm.validate('name', null).valid);
  });

  test('ALTER DROP NOT NULL', () => {
    dm.create('name', 'TEXT', { notNull: true });
    dm.alter('name', { type: 'DROP_NOT_NULL' });
    assert.ok(dm.validate('name', null).valid);
  });

  test('ALTER SET DEFAULT', () => {
    dm.create('priority', 'INTEGER');
    dm.alter('priority', { type: 'SET_DEFAULT', value: 5 });
    assert.equal(dm.getDefault('priority', null), 5);
  });

  test('ALTER DROP DEFAULT', () => {
    dm.create('priority', 'INTEGER', { default: 5 });
    dm.alter('priority', { type: 'DROP_DEFAULT' });
    assert.equal(dm.getDefault('priority', null), null);
  });

  test('ALTER DROP CONSTRAINT', () => {
    dm.create('val', 'INTEGER', { check: v => v > 0, constraintName: 'positive' });
    assert.ok(!dm.validate('val', -1).valid);
    
    dm.alter('val', { type: 'DROP_CONSTRAINT', name: 'positive' });
    assert.ok(dm.validate('val', -1).valid);
  });

  test('DROP domain', () => {
    dm.create('temp', 'TEXT');
    dm.drop('temp');
    assert.ok(!dm.has('temp'));
  });

  test('DROP IF EXISTS', () => {
    assert.equal(dm.drop('nonexistent', true), false);
  });

  test('duplicate create throws', () => {
    dm.create('email', 'TEXT');
    assert.throws(() => dm.create('email', 'TEXT'), /already exists/);
  });

  test('list domains', () => {
    dm.create('email', 'TEXT');
    dm.create('age', 'INTEGER');
    assert.equal(dm.list().length, 2);
  });

  test('NULL passes check when allowed', () => {
    dm.create('optional', 'TEXT', { check: v => v.length > 0 });
    assert.ok(dm.validate('optional', null).valid); // NULL bypasses CHECK
    assert.ok(!dm.validate('optional', '').valid);
  });

  test('case-insensitive', () => {
    dm.create('MyDomain', 'TEXT');
    assert.ok(dm.has('mydomain'));
    assert.ok(dm.validate('MYDOMAIN', 'hello').valid);
  });
});
