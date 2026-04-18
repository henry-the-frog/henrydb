import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('pg_catalog virtual tables', () => {
  let db;
  
  it('pg_class lists tables', () => {
    db = new Database();
    db.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, total FLOAT)');
    const r = db.execute("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r' ORDER BY relname");
    assert.deepStrictEqual(r.rows, [
      { relname: 'orders', relkind: 'r' },
      { relname: 'users', relkind: 'r' },
    ]);
  });

  it('pg_class lists indexes via indexCatalog', () => {
    db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price FLOAT)');
    db.execute('CREATE INDEX idx_products_name ON products (name)');
    const r = db.execute("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'i' ORDER BY relname");
    assert.ok(r.rows.some(row => row.relname === 'idx_products_name'));
  });

  it('pg_class lists views', () => {
    db = new Database();
    db.execute('CREATE TABLE employees (id INT, name TEXT, salary FLOAT)');
    db.execute('CREATE VIEW high_earners AS SELECT * FROM employees WHERE salary > 100000');
    const r = db.execute("SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'v'");
    assert.deepStrictEqual(r.rows, [{ relname: 'high_earners', relkind: 'v' }]);
  });

  it('pg_class has correct column counts', () => {
    db = new Database();
    db.execute('CREATE TABLE wide_table (a INT, b INT, c TEXT, d FLOAT, e BOOLEAN)');
    const r = db.execute("SELECT relname, relnatts FROM pg_catalog.pg_class WHERE relname = 'wide_table'");
    assert.equal(r.rows[0].relnatts, 5);
  });

  it('pg_attribute returns columns with correct types', () => {
    db = new Database();
    db.execute('CREATE TABLE typed (id INT PRIMARY KEY, name TEXT, price FLOAT, active BOOLEAN)');
    const r = db.execute('SELECT attname, atttypid, attnum, attnotnull FROM pg_catalog.pg_attribute ORDER BY attnum');
    assert.equal(r.rows.length, 4);
    assert.equal(r.rows[0].attname, 'id');
    assert.equal(r.rows[0].atttypid, 23); // INT = 23
    assert.equal(r.rows[0].attnotnull, true); // PK is not null
    assert.equal(r.rows[1].attname, 'name');
    assert.equal(r.rows[1].atttypid, 25); // TEXT = 25
    assert.equal(r.rows[2].atttypid, 701); // FLOAT = 701
    assert.equal(r.rows[3].atttypid, 16); // BOOLEAN = 16
  });

  it('pg_type has standard PostgreSQL types', () => {
    db = new Database();
    const r = db.execute('SELECT typname, oid FROM pg_catalog.pg_type ORDER BY oid');
    assert.ok(r.rows.length >= 10);
    const byName = Object.fromEntries(r.rows.map(r => [r.typname, r.oid]));
    assert.equal(byName.bool, 16);
    assert.equal(byName.int4, 23);
    assert.equal(byName.text, 25);
    assert.equal(byName.float8, 701);
    assert.equal(byName.varchar, 1043);
  });

  it('pg_index tracks indexes correctly', () => {
    db = new Database();
    db.execute('CREATE TABLE items (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE INDEX idx_items_name ON items (name)');
    const r = db.execute('SELECT indisunique, indisprimary FROM pg_catalog.pg_index');
    // Should have at least the user-created index and the PK index
    const pkIdx = r.rows.find(row => row.indisprimary === true);
    const userIdx = r.rows.find(row => row.indisprimary === false);
    assert.ok(pkIdx, 'should have PK index');
    assert.equal(pkIdx.indisunique, true);
    assert.ok(userIdx, 'should have user index');
  });

  it('pg_namespace returns standard namespaces', () => {
    db = new Database();
    const r = db.execute('SELECT nspname FROM pg_catalog.pg_namespace ORDER BY nspname');
    const names = r.rows.map(row => row.nspname);
    assert.ok(names.includes('pg_catalog'));
    assert.ok(names.includes('public'));
    assert.ok(names.includes('information_schema'));
  });

  it('pg_settings exposes cost model parameters', () => {
    db = new Database();
    const r = db.execute("SELECT name, setting FROM pg_catalog.pg_settings");
    const byName = Object.fromEntries(r.rows.map(r => [r.name, r.setting]));
    assert.ok('seq_page_cost' in byName);
    assert.ok('random_page_cost' in byName);
    assert.ok('server_version' in byName);
  });

  it('pg_stat_user_tables shows live tuple counts', () => {
    db = new Database();
    db.execute('CREATE TABLE counts (id INT PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO counts VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    const r = db.execute("SELECT relname, n_live_tup FROM pg_stat_user_tables");
    assert.equal(r.rows[0].relname, 'counts');
    assert.equal(r.rows[0].n_live_tup, 3);
  });

  it('short form without pg_catalog prefix works', () => {
    db = new Database();
    db.execute('CREATE TABLE test1 (id INT)');
    const r = db.execute("SELECT relname FROM pg_class WHERE relkind = 'r'");
    assert.deepStrictEqual(r.rows, [{ relname: 'test1' }]);
  });

  it('pg_catalog falls through for unknown tables', () => {
    db = new Database();
    db.execute('CREATE TABLE pg_custom (id INT)');
    db.execute('INSERT INTO pg_custom VALUES (42)');
    const r = db.execute('SELECT * FROM pg_custom');
    assert.deepStrictEqual(r.rows, [{ id: 42 }]);
  });

  it('pg_attribute filters by attrelid (JOIN with pg_class)', () => {
    db = new Database();
    db.execute('CREATE TABLE alpha (x INT, y TEXT)');
    db.execute('CREATE TABLE beta (a FLOAT, b BOOLEAN, c INT)');
    // Get alpha's columns via JOIN
    const r = db.execute(`
      SELECT a.attname as col_name, a.atttypid as type_oid
      FROM pg_catalog.pg_attribute a
      JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
      WHERE c.relname = 'alpha'
      ORDER BY a.attnum
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].col_name, 'x');
    assert.equal(r.rows[1].col_name, 'y');
  });

  it('psql-style \\dt query works', () => {
    // psql \\dt is: SELECT ... FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND n.nspname = 'public'
    db = new Database();
    db.execute('CREATE TABLE my_table (id INT)');
    const r = db.execute(`
      SELECT c.relname as table_name
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
      WHERE c.relkind = 'r' AND n.nspname = 'public'
    `);
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].table_name, 'my_table');
  });
});

describe('pg_catalog with joins', () => {
  it('JOIN pg_class with pg_attribute for column discovery', () => {
    const db = new Database();
    db.execute('CREATE TABLE alpha (x INT, y TEXT)');
    db.execute('CREATE TABLE beta (a FLOAT, b BOOLEAN)');
    const r = db.execute(`
      SELECT c.relname as tbl, a.attname as col, a.atttypid as tid
      FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
      WHERE c.relname = 'beta'
      ORDER BY a.attnum
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].col, 'a');
    assert.equal(r.rows[1].col, 'b');
  });

  it('JOIN pg_index with pg_class for index details', () => {
    const db = new Database();
    db.execute('CREATE TABLE products (id INT PRIMARY KEY, name TEXT)');
    db.execute('CREATE UNIQUE INDEX idx_name ON products (name)');
    const r = db.execute(`
      SELECT c.relname as idx_name, i.indisunique as is_unique, i.indisprimary as is_primary
      FROM pg_catalog.pg_index i
      JOIN pg_catalog.pg_class c ON c.oid = i.indexrelid
      ORDER BY c.relname
    `);
    assert.ok(r.rows.length >= 1);
    const named = r.rows.find(row => row.idx_name === 'idx_name');
    assert.ok(named, 'should find idx_name index');
    assert.equal(named.is_unique, true);
  });
});
