// materialized-views.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { MaterializedViewManager } from './materialized-views.js';

let db, mvm;

describe('MaterializedViewManager', () => {
  beforeEach(() => {
    db = new Database();
    mvm = new MaterializedViewManager(db);
    db.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, amount INTEGER, region TEXT)');
    db.execute("INSERT INTO sales VALUES (1, 'Widget', 100, 'East')");
    db.execute("INSERT INTO sales VALUES (2, 'Gadget', 200, 'West')");
    db.execute("INSERT INTO sales VALUES (3, 'Widget', 150, 'East')");
    db.execute("INSERT INTO sales VALUES (4, 'Gadget', 300, 'East')");
    db.execute("INSERT INTO sales VALUES (5, 'Doohickey', 50, 'West')");
  });

  test('create and query materialized view', () => {
    const stats = mvm.create('sales_summary', 'SELECT product, SUM(amount) as total FROM sales GROUP BY product');
    assert.equal(stats.isPopulated, true);
    assert.equal(stats.stale, false);
    assert.ok(stats.rowCount > 0);

    const result = mvm.query('sales_summary');
    assert.ok(result.rows.length >= 3);
    assert.ok(!result.stale);
  });

  test('materialized view with filter', () => {
    mvm.create('region_totals', 'SELECT region, SUM(amount) as total FROM sales GROUP BY region');
    const result = mvm.query('region_totals', { region: 'East' });
    assert.equal(result.rows.length, 1);
    assert.equal(result.rows[0].region, 'East');
  });

  test('CREATE WITH NO DATA', () => {
    const stats = mvm.create('lazy_view', 'SELECT * FROM sales', { noData: true });
    assert.equal(stats.isPopulated, false);
    assert.throws(() => mvm.query('lazy_view'), /not been populated/);
  });

  test('REFRESH populates view', () => {
    mvm.create('lazy_view', 'SELECT * FROM sales', { noData: true });
    mvm.refresh('lazy_view');
    const result = mvm.query('lazy_view');
    assert.equal(result.rows.length, 5);
  });

  test('stale detection on table change', () => {
    mvm.create('sales_mv', 'SELECT * FROM sales');
    assert.ok(!mvm.query('sales_mv').stale);

    const staleCount = mvm.notifyTableChange('sales');
    assert.equal(staleCount, 1);
    assert.ok(mvm.query('sales_mv').stale);
  });

  test('stale views tracked', () => {
    mvm.create('mv1', 'SELECT * FROM sales');
    mvm.create('mv2', 'SELECT product FROM sales');
    
    mvm.notifyTableChange('sales');
    
    const stale = mvm.getStaleViews();
    assert.equal(stale.length, 2);
  });

  test('refreshAllStale refreshes stale views', () => {
    mvm.create('mv1', 'SELECT * FROM sales');
    mvm.create('mv2', 'SELECT product FROM sales');
    
    mvm.notifyTableChange('sales');
    assert.equal(mvm.getStaleViews().length, 2);
    
    const refreshed = mvm.refreshAllStale();
    assert.equal(refreshed.length, 2);
    assert.equal(mvm.getStaleViews().length, 0);
  });

  test('refresh updates data', () => {
    mvm.create('count_mv', 'SELECT COUNT(*) as cnt FROM sales');
    assert.equal(mvm.query('count_mv').rows[0].cnt, 5);

    db.execute("INSERT INTO sales VALUES (6, 'New', 999, 'South')");
    mvm.notifyTableChange('sales');
    mvm.refresh('count_mv');
    assert.equal(mvm.query('count_mv').rows[0].cnt, 6);
  });

  test('drop materialized view', () => {
    mvm.create('temp_mv', 'SELECT * FROM sales');
    assert.ok(mvm.has('temp_mv'));
    mvm.drop('temp_mv');
    assert.ok(!mvm.has('temp_mv'));
  });

  test('drop IF EXISTS', () => {
    assert.equal(mvm.drop('nonexistent', true), false);
  });

  test('list all views', () => {
    mvm.create('mv1', 'SELECT * FROM sales');
    mvm.create('mv2', 'SELECT product FROM sales');
    
    const list = mvm.list();
    assert.equal(list.length, 2);
    assert.ok(list[0].name);
    assert.ok(list[0].sql);
  });

  test('multi-table dependency tracking', () => {
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO products VALUES (1, 'Widget')");

    mvm.create('joined_mv', 'SELECT s.amount, p.name FROM sales s JOIN products p ON s.product = p.name');
    
    // Changing either table should stale the view
    mvm.notifyTableChange('sales');
    assert.ok(mvm.query('joined_mv').stale);
    
    mvm.refresh('joined_mv');
    mvm.notifyTableChange('products');
    assert.ok(mvm.query('joined_mv').stale);
  });

  test('OR REPLACE replaces existing view', () => {
    mvm.create('my_mv', 'SELECT * FROM sales');
    assert.equal(mvm.query('my_mv').rows.length, 5);
    
    assert.throws(() => mvm.create('my_mv', 'SELECT * FROM sales WHERE amount > 100'));
    
    mvm.create('my_mv', 'SELECT * FROM sales WHERE amount > 100', { orReplace: true });
    assert.ok(mvm.query('my_mv').rows.length < 5);
  });

  test('refresh count tracked', () => {
    mvm.create('mv', 'SELECT * FROM sales');
    mvm.refresh('mv');
    mvm.refresh('mv');
    
    const stats = mvm.list().find(v => v.name === 'mv');
    assert.equal(stats.refreshCount, 3); // 1 auto + 2 manual
  });
});
