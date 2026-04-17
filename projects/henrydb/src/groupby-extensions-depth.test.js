// groupby-extensions-depth.test.js — GROUP BY ROLLUP, CUBE, GROUPING SETS tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-grp-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE sales (region TEXT, product TEXT, amount INT)');
  db.execute("INSERT INTO sales VALUES ('east', 'A', 100)");
  db.execute("INSERT INTO sales VALUES ('east', 'A', 150)");
  db.execute("INSERT INTO sales VALUES ('east', 'B', 200)");
  db.execute("INSERT INTO sales VALUES ('west', 'A', 300)");
  db.execute("INSERT INTO sales VALUES ('west', 'B', 250)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('GROUP BY ROLLUP', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ROLLUP produces subtotals and grand total', () => {
    const r = rows(db.execute(
      'SELECT region, product, SUM(amount) AS total ' +
      'FROM sales GROUP BY ROLLUP(region, product) ' +
      'ORDER BY region, product'
    ));
    // Expected groups:
    // (east, A) = 250
    // (east, B) = 200
    // (east, NULL) = 450 (subtotal for east)
    // (west, A) = 300
    // (west, B) = 250
    // (west, NULL) = 550 (subtotal for west)
    // (NULL, NULL) = 1000 (grand total)
    assert.ok(r.length >= 7, `ROLLUP should produce 7+ rows, got ${r.length}`);
    
    // Find grand total
    const grandTotal = r.find(x => x.region === null && x.product === null);
    assert.ok(grandTotal, 'ROLLUP should produce grand total');
    assert.equal(grandTotal.total, 1000);
  });

  it('single-column ROLLUP', () => {
    const r = rows(db.execute(
      'SELECT region, SUM(amount) AS total ' +
      'FROM sales GROUP BY ROLLUP(region) ' +
      'ORDER BY region'
    ));
    // (east) = 450, (west) = 550, (NULL) = 1000
    assert.ok(r.length >= 3);
    const total = r.find(x => x.region === null);
    assert.ok(total);
    assert.equal(total.total, 1000);
  });
});

describe('GROUP BY CUBE', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('CUBE produces all combinations', () => {
    const r = rows(db.execute(
      'SELECT region, product, SUM(amount) AS total ' +
      'FROM sales GROUP BY CUBE(region, product) ' +
      'ORDER BY region, product'
    ));
    // CUBE produces all 2^n combinations:
    // (east, A), (east, B), (east, NULL), (west, A), (west, B), (west, NULL),
    // (NULL, A), (NULL, B), (NULL, NULL)
    assert.ok(r.length >= 9, `CUBE should produce 9+ rows, got ${r.length}`);
    
    // Find product subtotal (across regions)
    const productA = r.find(x => x.region === null && x.product === 'A');
    if (productA) {
      assert.equal(productA.total, 550, 'Product A total: 100+150+300');
    }
  });
});

describe('GROUPING SETS', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('GROUPING SETS produces specified groupings', () => {
    const r = rows(db.execute(
      'SELECT region, product, SUM(amount) AS total ' +
      'FROM sales GROUP BY GROUPING SETS ((region), (product)) ' +
      'ORDER BY region, product'
    ));
    // Should have: region groups + product groups
    // east=450, west=550, A=550, B=450
    assert.ok(r.length >= 4);
  });
});

describe('GROUP BY with Expression', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('GROUP BY CASE WHEN expression', () => {
    const r = rows(db.execute(
      "SELECT CASE WHEN amount > 200 THEN 'high' ELSE 'low' END AS tier, " +
      'COUNT(*) AS cnt FROM sales ' +
      "GROUP BY CASE WHEN amount > 200 THEN 'high' ELSE 'low' END"
    ));
    assert.equal(r.length, 2);
    const high = r.find(x => x.tier === 'high');
    const low = r.find(x => x.tier === 'low');
    assert.ok(high);
    assert.ok(low);
  });
});
