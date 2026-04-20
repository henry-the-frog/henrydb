import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { VectorizedScan, VectorizedFilter, VectorizedProject, VectorizedLimit,
         VectorizedHashAggregate, VectorizedHashJoin, collectAll, DataBatch } from './vectorized.js';

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

  it('VectorizedHashAggregate: GROUP BY with multiple aggregates', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (product TEXT, region TEXT, amount NUMERIC)');
    db.execute("INSERT INTO sales VALUES ('Widget', 'East', 100)");
    db.execute("INSERT INTO sales VALUES ('Widget', 'West', 150)");
    db.execute("INSERT INTO sales VALUES ('Gadget', 'East', 200)");
    db.execute("INSERT INTO sales VALUES ('Widget', 'East', 75)");
    db.execute("INSERT INTO sales VALUES ('Gadget', 'West', 300)");

    const table = db.tables.get('sales');
    const scan = new VectorizedScan(table.heap, table.schema, 'sales', 32);
    const agg = new VectorizedHashAggregate(scan, ['product'], [
      { name: 'total', func: 'SUM', column: 'amount' },
      { name: 'cnt', func: 'COUNT_STAR' },
      { name: 'avg_amt', func: 'AVG', column: 'amount' },
      { name: 'min_amt', func: 'MIN', column: 'amount' },
      { name: 'max_amt', func: 'MAX', column: 'amount' },
    ]);
    const rows = collectAll(agg);
    
    const widget = rows.find(r => r.product === 'Widget');
    const gadget = rows.find(r => r.product === 'Gadget');
    
    assert.equal(widget.total, 325);
    assert.equal(widget.cnt, 3);
    assert.ok(Math.abs(widget.avg_amt - 108.33) < 0.01);
    assert.equal(widget.min_amt, 75);
    assert.equal(widget.max_amt, 150);
    
    assert.equal(gadget.total, 500);
    assert.equal(gadget.cnt, 2);
    assert.equal(gadget.avg_amt, 250);
  });

  it('VectorizedHashAggregate: global aggregation (no GROUP BY)', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (val INTEGER)');
    for (let i = 1; i <= 100; i++) {
      db.execute('INSERT INTO nums VALUES (' + i + ')');
    }
    const table = db.tables.get('nums');
    const scan = new VectorizedScan(table.heap, table.schema, 'nums', 64);
    const agg = new VectorizedHashAggregate(scan, [], [
      { name: 'total', func: 'SUM', column: 'val' },
      { name: 'cnt', func: 'COUNT_STAR' },
      { name: 'avg_val', func: 'AVG', column: 'val' },
    ]);
    const rows = collectAll(agg);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].total, 5050);
    assert.equal(rows[0].cnt, 100);
    assert.equal(rows[0].avg_val, 50.5);
  });

  it('VectorizedHashAggregate: filter then aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (category TEXT, value NUMERIC)');
    for (let i = 0; i < 100; i++) {
      const cat = i % 3 === 0 ? 'A' : (i % 3 === 1 ? 'B' : 'C');
      db.execute("INSERT INTO data VALUES ('" + cat + "', " + i + ")");
    }
    const table = db.tables.get('data');
    const scan = new VectorizedScan(table.heap, table.schema, 'data', 64);
    const filter = new VectorizedFilter(scan, (b, i) => b.getValue('value', i) >= 50);
    const agg = new VectorizedHashAggregate(filter, ['category'], [
      { name: 'cnt', func: 'COUNT_STAR' },
      { name: 'total', func: 'SUM', column: 'value' },
    ]);
    const rows = collectAll(agg);
    assert.equal(rows.length, 3); // A, B, C
    const totalCount = rows.reduce((s, r) => s + r.cnt, 0);
    assert.equal(totalCount, 50); // values 50-99
  });

  it('VectorizedHashJoin: basic equi-join', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INTEGER, customer_id INTEGER, total NUMERIC)');
    db.execute('CREATE TABLE customers (id INTEGER, name TEXT)');
    db.execute("INSERT INTO customers VALUES (1, 'Alice')");
    db.execute("INSERT INTO customers VALUES (2, 'Bob')");
    db.execute('INSERT INTO orders VALUES (10, 1, 100)');
    db.execute('INSERT INTO orders VALUES (11, 2, 200)');
    db.execute('INSERT INTO orders VALUES (12, 1, 50)');

    const orders = db.tables.get('orders');
    const customers = db.tables.get('customers');
    const buildScan = new VectorizedScan(customers.heap, customers.schema, 'customers', 32);
    const probeScan = new VectorizedScan(orders.heap, orders.schema, 'orders', 32);
    const join = new VectorizedHashJoin(buildScan, probeScan, 'id', 'customer_id', ['id', 'name'], ['id', 'total']);
    const rows = collectAll(join);
    
    assert.equal(rows.length, 3);
    const alice = rows.filter(r => r.build_name === 'Alice');
    assert.equal(alice.length, 2); // orders 10 and 12
    const bob = rows.filter(r => r.build_name === 'Bob');
    assert.equal(bob.length, 1);
    assert.equal(bob[0].total, 200);
  });

  it('VectorizedHashJoin: no matches produces empty result', () => {
    const db = new Database();
    db.execute('CREATE TABLE left_t (id INTEGER)');
    db.execute('CREATE TABLE right_t (ref_id INTEGER)');
    db.execute('INSERT INTO left_t VALUES (1)');
    db.execute('INSERT INTO left_t VALUES (2)');
    db.execute('INSERT INTO right_t VALUES (3)');
    db.execute('INSERT INTO right_t VALUES (4)');

    const left = db.tables.get('left_t');
    const right = db.tables.get('right_t');
    const build = new VectorizedScan(left.heap, left.schema, 'left_t', 32);
    const probe = new VectorizedScan(right.heap, right.schema, 'right_t', 32);
    const join = new VectorizedHashJoin(build, probe, 'id', 'ref_id', ['id'], ['ref_id']);
    const rows = collectAll(join);
    assert.equal(rows.length, 0);
  });

  it('VectorizedHashJoin + aggregate: join then GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE items (id INTEGER, category_id INTEGER, price NUMERIC)');
    db.execute('CREATE TABLE categories (id INTEGER, name TEXT)');
    db.execute("INSERT INTO categories VALUES (1, 'Electronics')");
    db.execute("INSERT INTO categories VALUES (2, 'Books')");
    db.execute('INSERT INTO items VALUES (1, 1, 500)');
    db.execute('INSERT INTO items VALUES (2, 1, 300)');
    db.execute('INSERT INTO items VALUES (3, 2, 20)');
    db.execute('INSERT INTO items VALUES (4, 2, 15)');
    db.execute('INSERT INTO items VALUES (5, 2, 25)');

    const items = db.tables.get('items');
    const categories = db.tables.get('categories');
    const buildScan = new VectorizedScan(categories.heap, categories.schema, 'categories', 32);
    const probeScan = new VectorizedScan(items.heap, items.schema, 'items', 32);
    const join = new VectorizedHashJoin(buildScan, probeScan, 'id', 'category_id', ['name'], ['price']);
    const agg = new VectorizedHashAggregate(join, ['build_name'], [
      { name: 'total', func: 'SUM', column: 'price' },
      { name: 'cnt', func: 'COUNT_STAR' },
    ]);
    const rows = collectAll(agg);
    
    const electronics = rows.find(r => r.build_name === 'Electronics');
    const books = rows.find(r => r.build_name === 'Books');
    assert.equal(electronics.total, 800);
    assert.equal(electronics.cnt, 2);
    assert.equal(books.total, 60);
    assert.equal(books.cnt, 3);
  });
});
