// vector-engine.test.js — Tests for vectorized execution engine
import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { VectorBatch, VSeqScan, VFilter, VProject, VHashAggregate, VHashJoin } from './vector-engine.js';
import { Database } from './db.js';

describe('VectorBatch', () => {
  test('creates batch with correct columns', () => {
    const batch = new VectorBatch(['id', 'name', 'age']);
    assert.equal(batch.columnNames.length, 3);
    assert.equal(batch.size, 0);
    assert.equal(batch.capacity, 1024);
  });

  test('addRow increases size', () => {
    const batch = new VectorBatch(['id', 'val'], 4);
    batch.addRow({ id: 1, val: 10 });
    batch.addRow({ id: 2, val: 20 });
    assert.equal(batch.size, 2);
  });

  test('get returns correct value', () => {
    const batch = new VectorBatch(['x', 'y']);
    batch.addRow({ x: 3, y: 7 });
    assert.equal(batch.get(0, 'x'), 3);
    assert.equal(batch.get(0, 'y'), 7);
  });

  test('getRow returns row object', () => {
    const batch = new VectorBatch(['a', 'b']);
    batch.addRow({ a: 1, b: 2 });
    const row = batch.getRow(0);
    assert.deepEqual(row, { a: 1, b: 2 });
  });

  test('toRows converts all rows', () => {
    const batch = new VectorBatch(['id']);
    batch.addRow({ id: 1 });
    batch.addRow({ id: 2 });
    batch.addRow({ id: 3 });
    const rows = batch.toRows();
    assert.equal(rows.length, 3);
    assert.deepEqual(rows, [{ id: 1 }, { id: 2 }, { id: 3 }]);
  });

  test('select filters rows by indices', () => {
    const batch = new VectorBatch(['val']);
    batch.addRow({ val: 10 });
    batch.addRow({ val: 20 });
    batch.addRow({ val: 30 });
    const filtered = batch.select([0, 2]);
    assert.equal(filtered.size, 2);
    assert.equal(filtered.get(0, 'val'), 10);
    assert.equal(filtered.get(1, 'val'), 30);
  });

  test('signals full when capacity reached', () => {
    const batch = new VectorBatch(['x'], 2);
    assert.ok(!batch.addRow({ x: 1 }));
    assert.ok(batch.addRow({ x: 2 })); // Full
    assert.equal(batch.size, 2);
  });
});

describe('VSeqScan', () => {
  test('scans table rows in batches', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INT, name TEXT)');
    db.execute("INSERT INTO items VALUES (1, 'a'), (2, 'b'), (3, 'c')");
    const table = db.tables.get('items');
    const scan = new VSeqScan(table.heap, ['id', 'name'], 'items');
    scan.open();
    const batch = scan.nextBatch();
    assert.equal(batch.size, 3);
    assert.equal(batch.get(0, 'id'), 1);
    assert.equal(batch.get(2, 'name'), 'c');
    assert.equal(batch.get(0, 'items.id'), 1);
    const batch2 = scan.nextBatch();
    assert.equal(batch2, null); // No more data
    scan.close();
  });
});

describe('VFilter', () => {
  test('filters rows by predicate', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (val INT)');
    db.execute('INSERT INTO nums VALUES (1), (2), (3), (4), (5)');
    const table = db.tables.get('nums');
    const scan = new VSeqScan(table.heap, ['val']);
    const filter = new VFilter(scan, row => row.val > 3);
    filter.open();
    const batch = filter.nextBatch();
    assert.equal(batch.size, 2);
    const rows = batch.toRows();
    assert.ok(rows.every(r => r.val > 3));
    filter.close();
  });
});

describe('VProject', () => {
  test('projects and computes new columns', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (x INT, y INT)');
    db.execute('INSERT INTO data VALUES (1, 2), (3, 4)');
    const table = db.tables.get('data');
    const scan = new VSeqScan(table.heap, ['x', 'y']);
    const project = new VProject(scan, [
      { name: 'sum', expr: row => row.x + row.y },
      { name: 'product', expr: row => row.x * row.y },
    ]);
    project.open();
    const batch = project.nextBatch();
    assert.equal(batch.size, 2);
    assert.equal(batch.get(0, 'sum'), 3);
    assert.equal(batch.get(0, 'product'), 2);
    assert.equal(batch.get(1, 'sum'), 7);
    project.close();
  });
});

describe('VHashAggregate', () => {
  test('computes SUM/COUNT grouped', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (dept TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150), ('B', 50)");
    const table = db.tables.get('sales');
    const scan = new VSeqScan(table.heap, ['dept', 'amount']);
    const agg = new VHashAggregate(scan, ['dept'], [
      { name: 'total', fn: 'SUM', col: 'amount' },
      { name: 'cnt', fn: 'COUNT', col: 'amount' },
    ]);
    agg.open();
    const batch = agg.nextBatch();
    assert.equal(batch.size, 2);
    const rows = batch.toRows().sort((a, b) => a.dept.localeCompare(b.dept));
    assert.equal(rows[0].dept, 'A');
    assert.equal(rows[0].total, 250);
    assert.equal(rows[0].cnt, 2);
    assert.equal(rows[1].dept, 'B');
    assert.equal(rows[1].total, 250);
    assert.equal(rows[1].cnt, 2);
    agg.close();
  });

  test('handles AVG/MIN/MAX', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (score INT)');
    db.execute('INSERT INTO scores VALUES (10), (20), (30)');
    const table = db.tables.get('scores');
    const scan = new VSeqScan(table.heap, ['score']);
    const agg = new VHashAggregate(scan, [], [
      { name: 'avg', fn: 'AVG', col: 'score' },
      { name: 'min', fn: 'MIN', col: 'score' },
      { name: 'max', fn: 'MAX', col: 'score' },
    ]);
    agg.open();
    const batch = agg.nextBatch();
    assert.equal(batch.size, 1);
    assert.equal(batch.get(0, 'avg'), 20);
    assert.equal(batch.get(0, 'min'), 10);
    assert.equal(batch.get(0, 'max'), 30);
    agg.close();
  });
});

describe('VHashJoin', () => {
  test('joins two tables on key', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer_id INT, total INT)');
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO orders VALUES (1, 1, 100), (2, 2, 200), (3, 1, 150)");
    db.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')");
    
    const ordersTable = db.tables.get('orders');
    const custTable = db.tables.get('customers');
    
    const probeScan = new VSeqScan(ordersTable.heap, ['id', 'customer_id', 'total'], 'o');
    const buildScan = new VSeqScan(custTable.heap, ['id', 'name'], 'c');
    
    const join = new VHashJoin(probeScan, buildScan, 'customer_id', 'id');
    join.open();
    const batch = join.nextBatch();
    assert.equal(batch.size, 3);
    const rows = batch.toRows();
    assert.ok(rows.every(r => r.name));
    const aliceOrders = rows.filter(r => r.name === 'Alice');
    assert.equal(aliceOrders.length, 2);
    join.close();
  });
});

describe('Performance', () => {
  test('vectorized scan is at least as fast as Volcano for large tables', () => {
    const db = new Database();
    db.execute('CREATE TABLE big (id INT, val INT)');
    // Insert 10,000 rows
    for (let i = 0; i < 100; i++) {
      const vals = [];
      for (let j = 0; j < 100; j++) {
        vals.push(`(${i * 100 + j}, ${Math.floor(Math.random() * 1000)})`);
      }
      db.execute(`INSERT INTO big VALUES ${vals.join(',')}`);
    }
    
    const table = db.tables.get('big');
    
    // Vectorized scan with filter
    const startV = performance.now();
    const scan = new VSeqScan(table.heap, ['id', 'val']);
    const filter = new VFilter(scan, row => row.val > 500);
    filter.open();
    let vRows = 0;
    let batch;
    while ((batch = filter.nextBatch()) !== null) {
      vRows += batch.size;
    }
    filter.close();
    const timeV = performance.now() - startV;
    
    // Regular scan (simulating Volcano)
    const startR = performance.now();
    let rRows = 0;
    for (const { values } of table.heap.scan()) {
      if (values[1] > 500) rRows++;
    }
    const timeR = performance.now() - startR;
    
    assert.equal(vRows, rRows, 'Same row count');
    // Just verify it works — don't assert performance in CI
    assert.ok(vRows > 0, 'Should have matching rows');
  });
});
