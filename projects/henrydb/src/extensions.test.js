// extensions.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { ExtensionManager } from './extensions.js';

let em;

describe('ExtensionManager', () => {
  beforeEach(() => { em = new ExtensionManager(); });

  test('built-in extensions available', () => {
    assert.ok(em.isAvailable('uuid-ossp'));
    assert.ok(em.isAvailable('pgcrypto'));
    assert.ok(em.isAvailable('pg_trgm'));
    assert.ok(em.isAvailable('hstore'));
    assert.ok(em.isAvailable('postgis'));
  });

  test('CREATE EXTENSION', () => {
    const info = em.create('pgcrypto');
    assert.equal(info.name, 'pgcrypto');
    assert.ok(info.objects.length > 0);
    assert.ok(em.isInstalled('pgcrypto'));
  });

  test('CREATE EXTENSION IF NOT EXISTS', () => {
    em.create('pgcrypto');
    const info = em.create('pgcrypto', { ifNotExists: true });
    assert.ok(info);
  });

  test('duplicate create throws', () => {
    em.create('pgcrypto');
    assert.throws(() => em.create('pgcrypto'), /already exists/);
  });

  test('unavailable extension throws', () => {
    assert.throws(() => em.create('nonexistent'), /not available/);
  });

  test('DROP EXTENSION', () => {
    em.create('pgcrypto');
    em.drop('pgcrypto');
    assert.ok(!em.isInstalled('pgcrypto'));
  });

  test('DROP IF EXISTS', () => {
    assert.equal(em.drop('nonexistent', { ifExists: true }), false);
  });

  test('custom schema', () => {
    const info = em.create('uuid-ossp', { schema: 'extensions' });
    assert.equal(info.schema, 'extensions');
  });

  test('UPDATE extension version', () => {
    em.create('hstore');
    const result = em.update('hstore', '2.0');
    assert.equal(result.oldVersion, '1.8');
    assert.equal(result.newVersion, '2.0');
  });

  test('objects installed with extension', () => {
    const info = em.create('pgcrypto');
    const names = info.objects.map(o => o.name);
    assert.ok(names.includes('gen_random_uuid'));
    assert.ok(names.includes('crypt'));
  });

  test('listInstalled', () => {
    em.create('pgcrypto');
    em.create('hstore');
    assert.equal(em.listInstalled().length, 2);
  });

  test('listAvailable shows installed status', () => {
    em.create('pgcrypto');
    const available = em.listAvailable();
    const pgcrypto = available.find(e => e.name === 'pgcrypto');
    assert.ok(pgcrypto.installed);
    const hstore = available.find(e => e.name === 'hstore');
    assert.ok(!hstore.installed);
  });

  test('dependency checking', () => {
    em.register('ext_child', {
      version: '1.0',
      dependencies: ['pgcrypto'],
      install: () => [],
    });

    assert.throws(() => em.create('ext_child'), /not installed/);
    em.create('pgcrypto');
    em.create('ext_child'); // Now works
    assert.ok(em.isInstalled('ext_child'));
  });

  test('DROP dependent without CASCADE throws', () => {
    em.register('child', {
      version: '1.0',
      dependencies: ['pgcrypto'],
      install: () => [],
    });
    em.create('pgcrypto');
    em.create('child');
    
    assert.throws(() => em.drop('pgcrypto'), /depends on it/);
  });

  test('DROP CASCADE removes dependents', () => {
    em.register('child', {
      version: '1.0',
      dependencies: ['pgcrypto'],
      install: () => [],
    });
    em.create('pgcrypto');
    em.create('child');
    em.drop('pgcrypto', { cascade: true });
    
    assert.ok(!em.isInstalled('pgcrypto'));
    assert.ok(!em.isInstalled('child'));
  });

  test('register custom extension', () => {
    em.register('my_ext', {
      version: '0.1',
      description: 'My custom extension',
      install: () => [{ type: 'function', name: 'my_func' }],
    });
    assert.ok(em.isAvailable('my_ext'));
    em.create('my_ext');
    assert.ok(em.isInstalled('my_ext'));
  });
});
