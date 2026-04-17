// aggregate-depth.test.js — Aggregate function + GROUP BY depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-agg-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE sales (id INT, product TEXT, region TEXT, amount INT)');
  db.execute("INSERT INTO sales VALUES (1, 'A', 'east', 100)");
  db.execute("INSERT INTO sales VALUES (2, 'B', 'east', 200)");
  db.execute("INSERT INTO sales VALUES (3, 'A', 'west', 150)");
  db.execute("INSERT INTO sales VALUES (4, 'B', 'west', NULL)");
  db.execute("INSERT INTO sales VALUES (5, 'A', NULL, 75)");
  db.execute("INSERT INTO sales VALUES (6, NULL, 'east', 300)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('GROUP BY with NULLs', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('NULL forms its own group', () => {
    const r = rows(db.execute(
      'SELECT product, COUNT(*) AS cnt FROM sales GROUP BY product ORDER BY product'
    ));
    // Groups: NULL, 'A', 'B'
    assert.equal(r.length, 3);
    const nullGroup = r.find(x => x.product === null);
    assert.ok(nullGroup, 'NULL should form its own group');
    assert.equal(nullGroup.cnt, 1);
  });

  it('GROUP BY with NULL in non-grouped column', () => {
    const r = rows(db.execute(
      'SELECT region, SUM(amount) AS total FROM sales GROUP BY region ORDER BY region'
    ));
    // Regions: NULL, 'east', 'west'
    assert.equal(r.length, 3);
    const nullRegion = r.find(x => x.region === null);
    assert.ok(nullRegion, 'NULL region should be a group');
    assert.equal(nullRegion.total, 75);
  });

  it('GROUP BY multiple columns with NULLs', () => {
    const r = rows(db.execute(
      'SELECT product, region, COUNT(*) AS cnt FROM sales GROUP BY product, region ORDER BY product, region'
    ));
    // Should have: (NULL,east), (A,NULL), (A,east), (A,west), (B,east), (B,west)
    assert.ok(r.length >= 6, `Expected 6 groups, got ${r.length}`);
  });
});

describe('HAVING', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('HAVING filters groups', () => {
    const r = rows(db.execute(
      'SELECT product, COUNT(*) AS cnt FROM sales WHERE product IS NOT NULL GROUP BY product HAVING COUNT(*) >= 2 ORDER BY product'
    ));
    assert.equal(r.length, 2); // A(3 rows), B(2 rows)
    assert.equal(r[0].product, 'A');
    assert.equal(r[1].product, 'B');
  });

  it('HAVING with SUM', () => {
    const r = rows(db.execute(
      'SELECT product, SUM(amount) AS total FROM sales WHERE product IS NOT NULL GROUP BY product HAVING SUM(amount) > 200 ORDER BY product'
    ));
    // A: 100+150+75=325 > 200
    // B: 200+NULL=200, not > 200
    assert.equal(r.length, 1);
    assert.equal(r[0].product, 'A');
    assert.equal(r[0].total, 325);
  });
});

describe('COUNT DISTINCT', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('COUNT DISTINCT ignores duplicates', () => {
    const r = rows(db.execute('SELECT COUNT(DISTINCT product) AS cnt FROM sales'));
    // Distinct non-NULL products: 'A', 'B' = 2
    assert.equal(r[0].cnt, 2);
  });

  it('COUNT DISTINCT with NULLs', () => {
    const r = rows(db.execute('SELECT COUNT(DISTINCT region) AS cnt FROM sales'));
    // Distinct non-NULL regions: 'east', 'west' = 2 (NULL excluded from COUNT)
    assert.equal(r[0].cnt, 2);
  });

  it('COUNT(*) vs COUNT(column) with NULLs', () => {
    const rAll = rows(db.execute('SELECT COUNT(*) AS cnt FROM sales'));
    const rCol = rows(db.execute('SELECT COUNT(amount) AS cnt FROM sales'));
    
    assert.equal(rAll[0].cnt, 6, 'COUNT(*) should count all rows including NULLs');
    assert.equal(rCol[0].cnt, 5, 'COUNT(amount) should exclude NULL amounts');
  });
});

describe('Aggregates on Empty Groups', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SUM on empty result is NULL', () => {
    const r = rows(db.execute('SELECT SUM(amount) AS total FROM sales WHERE 1 = 0'));
    assert.equal(r.length, 1);
    assert.equal(r[0].total, null, 'SUM on empty set should be NULL');
  });

  it('COUNT on empty result is 0', () => {
    const r = rows(db.execute('SELECT COUNT(*) AS cnt FROM sales WHERE 1 = 0'));
    assert.equal(r[0].cnt, 0, 'COUNT on empty set should be 0');
  });

  it('AVG on empty result is NULL', () => {
    const r = rows(db.execute('SELECT AVG(amount) AS avg_amt FROM sales WHERE 1 = 0'));
    assert.equal(r[0].avg_amt, null, 'AVG on empty set should be NULL');
  });

  it('MIN/MAX on empty result is NULL', () => {
    const rMin = rows(db.execute('SELECT MIN(amount) AS m FROM sales WHERE 1 = 0'));
    const rMax = rows(db.execute('SELECT MAX(amount) AS m FROM sales WHERE 1 = 0'));
    assert.equal(rMin[0].m, null, 'MIN on empty set should be NULL');
    assert.equal(rMax[0].m, null, 'MAX on empty set should be NULL');
  });

  it('SUM ignores NULLs', () => {
    // amount has one NULL: B/west
    const r = rows(db.execute('SELECT SUM(amount) AS total FROM sales'));
    // 100 + 200 + 150 + 75 + 300 = 825 (NULL excluded)
    assert.equal(r[0].total, 825);
  });

  it('AVG ignores NULLs in denominator', () => {
    const r = rows(db.execute('SELECT AVG(amount) AS avg_amt FROM sales'));
    // 825 / 5 = 165 (not 825/6)
    assert.equal(r[0].avg_amt, 165);
  });
});

describe('Aggregate + MVCC', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('aggregate sees snapshot-consistent data', () => {
    const s1 = db.session();
    s1.begin();
    
    const r1 = rows(s1.execute('SELECT SUM(amount) AS total FROM sales'));
    assert.equal(r1[0].total, 825);

    // Concurrent insert
    db.execute('INSERT INTO sales VALUES (7, \'C\', \'east\', 500)');

    // s1 should still see 825
    const r2 = rows(s1.execute('SELECT SUM(amount) AS total FROM sales'));
    assert.equal(r2[0].total, 825, 'Snapshot should see original total');

    // New read should see 1325
    const r3 = rows(db.execute('SELECT SUM(amount) AS total FROM sales'));
    assert.equal(r3[0].total, 1325);

    s1.commit();
    s1.close();
  });

  it('GROUP BY with concurrent delete', () => {
    const s1 = db.session();
    s1.begin();
    
    const r1 = rows(s1.execute('SELECT product, COUNT(*) AS cnt FROM sales WHERE product IS NOT NULL GROUP BY product'));
    const aCount = r1.find(x => x.product === 'A')?.cnt;
    assert.equal(aCount, 3);

    // Delete an 'A' product row
    db.execute("DELETE FROM sales WHERE id = 1");

    // s1 should still see 3 'A' rows
    const r2 = rows(s1.execute('SELECT product, COUNT(*) AS cnt FROM sales WHERE product IS NOT NULL GROUP BY product'));
    const aCount2 = r2.find(x => x.product === 'A')?.cnt;
    assert.equal(aCount2, 3, 'Snapshot should preserve group count');

    s1.commit();
    s1.close();
  });
});
