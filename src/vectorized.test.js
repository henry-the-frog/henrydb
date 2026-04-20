import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { VectorizedScan, VectorizedFilter, VectorizedProject, VectorizedLimit, collectAll, DataBatch } from './vectorized.js';

function query(db, sql) {
  return db.execute(sql).rows;
}

describe('Vectorized Execution Engine', () => {

  it('VectorizedScan reads all rows in batches', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER, val TEXT)');
    for (let i = 0; i < 100; i++) {
      db.execute("INSERT INTO t VALUES (" + i + ", 'v" + i + "')");
    }
    const table = db.tables.get('t');
    const scan = new VectorizedScan(table.heap, table.schema, 't', 32);
    const rows = collectAll(scan, ['id', 'val']);
    assert.equal(rows.length, 100);
    assert.equal(rows[0].id, 0);
    assert.equal(rows[99].id, 99);
  });

  it('VectorizedFilter uses selection vectors (zero-copy)', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER, value NUMERIC)');
    for (let i = 0; i < 1000; i++) {
      db.execute('INSERT INTO nums VALUES (' + i + ', ' + (i * 2.5) + ')');
    }
    const table = db.tables.get('nums');
    const scan = new VectorizedScan(table.heap, table.schema, 'nums', 256);
    const filter = new VectorizedFilter(scan, (batch, i) => {
      return batch.getValue('id', i) >= 500;
    });
    const rows = collectAll(filter, ['id', 'value']);
    assert.equal(rows.length, 500);
    assert.equal(rows[0].id, 500);
  });

  it('VectorizedProject computes new columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (x INTEGER, y INTEGER)');
    for (let i = 0; i < 50; i++) {
      db.execute('INSERT INTO data VALUES (' + i + ', ' + (i * 10) + ')');
    }
    const table = db.tables.get('data');
    const scan = new VectorizedScan(table.heap, table.schema, 'data', 64);
    const project = new VectorizedProject(scan, [
      { name: 'x', compute: (b, i) => b.getValue('x', i) },
      { name: 'sum_xy', compute: (b, i) => b.getValue('x', i) + b.getValue('y', i) },
    ]);
    const rows = collectAll(project, ['x', 'sum_xy']);
    assert.equal(rows.length, 50);
    assert.equal(rows[0].sum_xy, 0); // 0 + 0
    assert.equal(rows[10].sum_xy, 110); // 10 + 100
  });

  it('VectorizedLimit stops after N rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INTEGER)');
    for (let i = 0; i < 1000; i++) {
      db.execute('INSERT INTO big VALUES (' + i + ')');
    }
    const table = db.tables.get('big');
    const scan = new VectorizedScan(table.heap, table.schema, 'big', 128);
    const limit = new VectorizedLimit(scan, 10);
    const rows = collectAll(limit, ['id']);
    assert.equal(rows.length, 10);
  });

  it('chained operators: scan → filter → project → limit', () => {
    const db = new Database();
    db.execute('CREATE TABLE chain (id INTEGER, price NUMERIC, category TEXT)');
    for (let i = 0; i < 200; i++) {
      const cat = i % 3 === 0 ? 'A' : (i % 3 === 1 ? 'B' : 'C');
      db.execute("INSERT INTO chain VALUES (" + i + ", " + (i * 1.5) + ", '" + cat + "')");
    }
    const table = db.tables.get('chain');
    const scan = new VectorizedScan(table.heap, table.schema, 'chain', 64);
    const filter = new VectorizedFilter(scan, (b, i) => b.getValue('category', i) === 'A');
    const project = new VectorizedProject(filter, [
      { name: 'id', compute: (b, i) => b.getValue('id', i) },
      { name: 'discounted', compute: (b, i) => b.getValue('price', i) * 0.9 },
    ]);
    const limit = new VectorizedLimit(project, 5);
    const rows = collectAll(limit, ['id', 'discounted']);
    assert.equal(rows.length, 5);
    // Category A: ids 0, 3, 6, 9, 12
    assert.equal(rows[0].id, 0);
    assert.equal(rows[1].id, 3);
    assert.equal(rows[4].id, 12);
  });

  it('empty table produces no batches', () => {
    const db = new Database();
    db.execute('CREATE TABLE empty (id INTEGER)');
    const table = db.tables.get('empty');
    const scan = new VectorizedScan(table.heap, table.schema, 'empty', 32);
    const rows = collectAll(scan, ['id']);
    assert.equal(rows.length, 0);
  });

  it('filter that eliminates all rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE nopass (id INTEGER)');
    for (let i = 0; i < 100; i++) {
      db.execute('INSERT INTO nopass VALUES (' + i + ')');
    }
    const table = db.tables.get('nopass');
    const scan = new VectorizedScan(table.heap, table.schema, 'nopass', 32);
    const filter = new VectorizedFilter(scan, () => false);
    const rows = collectAll(filter, ['id']);
    assert.equal(rows.length, 0);
  });

  it('benchmark: vectorized vs volcano on 10K rows', () => {
    const db = new Database();
    db.execute('CREATE TABLE perf (id INTEGER, value NUMERIC, flag INTEGER)');
    for (let i = 0; i < 10000; i++) {
      db.execute('INSERT INTO perf VALUES (' + i + ', ' + (i * 1.1) + ', ' + (i % 5) + ')');
    }

    // Volcano (standard)
    const t0 = Date.now();
    const volcanoResult = query(db, 'SELECT id, value * 2 AS doubled FROM perf WHERE flag = 0 LIMIT 100');
    const volcanoTime = Date.now() - t0;

    // Vectorized
    const table = db.tables.get('perf');
    const t1 = Date.now();
    const scan = new VectorizedScan(table.heap, table.schema, 'perf', 1024);
    const filter = new VectorizedFilter(scan, (b, i) => b.getValue('flag', i) === 0);
    const project = new VectorizedProject(filter, [
      { name: 'id', compute: (b, i) => b.getValue('id', i) },
      { name: 'doubled', compute: (b, i) => b.getValue('value', i) * 2 },
    ]);
    const limit = new VectorizedLimit(project, 100);
    const vecResult = collectAll(limit, ['id', 'doubled']);
    const vecTime = Date.now() - t1;

    console.log('Volcano:', volcanoTime + 'ms, Vectorized:', vecTime + 'ms');
    
    // Verify correctness: same results
    assert.equal(volcanoResult.length, 100);
    assert.equal(vecResult.length, 100);
    assert.equal(vecResult[0].id, 0);
    assert.equal(vecResult[0].doubled, 0);
    assert.equal(vecResult[1].id, 5); // Next row with flag=0
  });
});
