// foreign-data-wrapper.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { FDWManager } from './foreign-data-wrapper.js';

let fdw;

describe('FDWManager', () => {
  beforeEach(() => {
    fdw = new FDWManager();
  });

  test('built-in wrappers exist', () => {
    // Can create servers with built-in wrappers
    fdw.createServer('remote1', 'postgres_fdw', { host: 'db.example.com' });
    assert.ok(fdw.hasServer('remote1'));
  });

  test('CREATE SERVER', () => {
    const info = fdw.createServer('myremote', 'postgres_fdw', {
      host: '10.0.0.5',
      port: 5433,
      dbname: 'analytics',
    });
    assert.equal(info.name, 'myremote');
  });

  test('CREATE USER MAPPING', () => {
    fdw.createServer('remote1', 'postgres_fdw');
    const info = fdw.createUserMapping('henry', 'remote1', { password: 'secret' });
    assert.equal(info.user, 'henry');
  });

  test('CREATE FOREIGN TABLE', () => {
    fdw.createServer('remote1', 'postgres_fdw');
    const info = fdw.createForeignTable('remote_users', 'remote1', [
      { name: 'id', type: 'INTEGER' },
      { name: 'name', type: 'TEXT' },
    ], { remoteTable: 'users', remoteSchema: 'public' });

    assert.equal(info.name, 'remote_users');
    assert.deepEqual(info.columns, ['id', 'name']);
  });

  test('query foreign table with data provider', () => {
    fdw.createServer('test_server', 'postgres_fdw');
    fdw.createForeignTable('remote_data', 'test_server', [
      { name: 'id', type: 'INTEGER' },
      { name: 'value', type: 'TEXT' },
    ]);

    fdw.registerDataProvider('test_server', (ctx) => {
      const data = [
        { id: 1, value: 'alpha' },
        { id: 2, value: 'beta' },
        { id: 3, value: 'gamma' },
      ];
      if (ctx.where) return data.filter(ctx.where);
      if (ctx.limit) return data.slice(0, ctx.limit);
      return data;
    });

    const results = fdw.query('remote_data');
    assert.equal(results.length, 3);
  });

  test('query with predicate pushdown', () => {
    fdw.createServer('ts', 'postgres_fdw');
    fdw.createForeignTable('ft', 'ts', [{ name: 'id', type: 'INT' }]);
    fdw.registerDataProvider('ts', (ctx) => {
      const data = Array.from({ length: 100 }, (_, i) => ({ id: i + 1 }));
      if (ctx.where) return data.filter(ctx.where);
      return data;
    });

    const results = fdw.query('ft', { where: (r) => r.id <= 5 });
    assert.equal(results.length, 5);
  });

  test('query with limit pushdown', () => {
    fdw.createServer('ts', 'postgres_fdw');
    fdw.createForeignTable('ft', 'ts', [{ name: 'id', type: 'INT' }]);
    fdw.registerDataProvider('ts', (ctx) => {
      const data = Array.from({ length: 100 }, (_, i) => ({ id: i + 1 }));
      if (ctx.limit) return data.slice(0, ctx.limit);
      return data;
    });

    const results = fdw.query('ft', { limit: 10 });
    assert.equal(results.length, 10);
  });

  test('DROP FOREIGN TABLE', () => {
    fdw.createServer('ts', 'postgres_fdw');
    fdw.createForeignTable('ft', 'ts', [{ name: 'id', type: 'INT' }]);
    fdw.dropForeignTable('ft');
    assert.ok(!fdw.hasForeignTable('ft'));
  });

  test('DROP SERVER CASCADE', () => {
    fdw.createServer('ts', 'postgres_fdw');
    fdw.createForeignTable('ft1', 'ts', [{ name: 'id', type: 'INT' }]);
    fdw.createForeignTable('ft2', 'ts', [{ name: 'id', type: 'INT' }]);
    fdw.dropServer('ts', false, true);
    assert.ok(!fdw.hasServer('ts'));
    assert.ok(!fdw.hasForeignTable('ft1'));
    assert.ok(!fdw.hasForeignTable('ft2'));
  });

  test('CREATE SERVER with unknown FDW throws', () => {
    assert.throws(() => fdw.createServer('x', 'unknown_fdw'), /does not exist/);
  });

  test('CREATE FOREIGN TABLE with unknown server throws', () => {
    assert.throws(() => fdw.createForeignTable('ft', 'nonexistent', []), /does not exist/);
  });

  test('listServers', () => {
    fdw.createServer('s1', 'postgres_fdw', { host: 'a' });
    fdw.createServer('s2', 'file_fdw', { host: 'b' });
    assert.equal(fdw.listServers().length, 2);
  });

  test('listForeignTables', () => {
    fdw.createServer('ts', 'postgres_fdw');
    fdw.createForeignTable('ft1', 'ts', [{ name: 'x', type: 'INT' }]);
    fdw.createForeignTable('ft2', 'ts', [{ name: 'y', type: 'TEXT' }]);
    assert.equal(fdw.listForeignTables().length, 2);
  });

  test('custom FDW wrapper', () => {
    fdw.createWrapper('csv_fdw', { handler: 'csv_handler' });
    fdw.createServer('csv_srv', 'csv_fdw');
    assert.ok(fdw.hasServer('csv_srv'));
  });
});
