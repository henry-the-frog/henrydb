// window-function-depth.test.js — Window function correctness depth tests

import { describe, it, beforeEach, afterEach } from 'node:test';
import { strict as assert } from 'node:assert';
import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

let dbDir, db;

function setup() {
  dbDir = mkdtempSync(join(tmpdir(), 'henrydb-window-'));
  db = TransactionalDatabase.open(dbDir);
  db.execute('CREATE TABLE scores (name TEXT, dept TEXT, score INT)');
  db.execute("INSERT INTO scores VALUES ('Alice', 'eng', 90)");
  db.execute("INSERT INTO scores VALUES ('Bob', 'eng', 85)");
  db.execute("INSERT INTO scores VALUES ('Carol', 'eng', 85)");
  db.execute("INSERT INTO scores VALUES ('Dave', 'sales', 95)");
  db.execute("INSERT INTO scores VALUES ('Eve', 'sales', 88)");
  db.execute("INSERT INTO scores VALUES ('Frank', 'sales', 88)");
  db.execute("INSERT INTO scores VALUES ('Grace', 'hr', 92)");
}
function teardown() {
  try { db.close(); } catch {}
  rmSync(dbDir, { recursive: true, force: true });
}
function rows(r) { return Array.isArray(r) ? r : r?.rows || []; }

describe('ROW_NUMBER', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('ROW_NUMBER assigns sequential numbers within partition', () => {
    const r = rows(db.execute(
      'SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score DESC) AS rn FROM scores ORDER BY dept, rn'
    ));
    // eng: Alice(90)=1, Bob(85)=2, Carol(85)=3
    const eng = r.filter(x => x.dept === 'eng');
    assert.equal(eng[0].rn, 1);
    assert.equal(eng[1].rn, 2);
    assert.equal(eng[2].rn, 3);
  });

  it('ROW_NUMBER without PARTITION BY numbers all rows', () => {
    const r = rows(db.execute(
      'SELECT name, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM scores'
    ));
    assert.equal(r.length, 7);
    const rns = r.map(x => x.rn).sort((a, b) => a - b);
    assert.deepEqual(rns, [1, 2, 3, 4, 5, 6, 7]);
  });
});

describe('RANK with ties', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('RANK assigns same rank for ties, with gaps', () => {
    const r = rows(db.execute(
      'SELECT name, dept, score, RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM scores WHERE dept = \'eng\' ORDER BY rnk'
    ));
    // Alice(90)=rank 1, Bob(85)=rank 2, Carol(85)=rank 2
    assert.equal(r[0].rnk, 1); // Alice
    assert.equal(r[1].rnk, 2); // Bob (tied)
    assert.equal(r[2].rnk, 2); // Carol (tied)
  });

  it('RANK leaves gap after ties', () => {
    // After two rank-2 entries, next should be rank 4 (not 3)
    db.execute("INSERT INTO scores VALUES ('Hank', 'eng', 80)");
    const r = rows(db.execute(
      'SELECT name, score, RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS rnk FROM scores WHERE dept = \'eng\' ORDER BY rnk'
    ));
    // Alice(90)=1, Bob(85)=2, Carol(85)=2, Hank(80)=4
    const hank = r.find(x => x.name === 'Hank');
    assert.equal(hank.rnk, 4, 'RANK should leave gap after ties');
  });
});

describe('DENSE_RANK', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('DENSE_RANK does not leave gaps', () => {
    db.execute("INSERT INTO scores VALUES ('Hank', 'eng', 80)");
    const r = rows(db.execute(
      'SELECT name, score, DENSE_RANK() OVER (PARTITION BY dept ORDER BY score DESC) AS drnk FROM scores WHERE dept = \'eng\' ORDER BY drnk'
    ));
    // Alice(90)=1, Bob(85)=2, Carol(85)=2, Hank(80)=3 (not 4!)
    const hank = r.find(x => x.name === 'Hank');
    assert.equal(hank.rnk || hank.drnk, 3, 'DENSE_RANK should not leave gaps');
  });

  it('DENSE_RANK with all same values', () => {
    db.execute('CREATE TABLE same (val INT)');
    db.execute('INSERT INTO same VALUES (5)');
    db.execute('INSERT INTO same VALUES (5)');
    db.execute('INSERT INTO same VALUES (5)');

    const r = rows(db.execute(
      'SELECT val, DENSE_RANK() OVER (ORDER BY val) AS drnk FROM same'
    ));
    // All same → all rank 1
    assert.equal(r.length, 3);
    for (const row of r) {
      assert.equal(row.drnk, 1, 'All same values should have dense_rank 1');
    }
  });
});

describe('LAG and LEAD', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('LAG returns previous row value', () => {
    const r = rows(db.execute(
      'SELECT name, score, LAG(score) OVER (ORDER BY score DESC) AS prev_score FROM scores ORDER BY score DESC'
    ));
    // First row should have prev_score = NULL
    assert.equal(r[0].prev_score, null, 'First row LAG should be NULL');
    // Second row should have first row's score
    assert.equal(r[1].prev_score, r[0].score, 'LAG should return previous value');
  });

  it('LEAD returns next row value', () => {
    const r = rows(db.execute(
      'SELECT name, score, LEAD(score) OVER (ORDER BY score DESC) AS next_score FROM scores ORDER BY score DESC'
    ));
    // Last row should have next_score = NULL
    assert.equal(r[r.length - 1].next_score, null, 'Last row LEAD should be NULL');
    // First row should have second row's score
    assert.equal(r[0].next_score, r[1].score, 'LEAD should return next value');
  });

  it('LAG with offset > 1', () => {
    const r = rows(db.execute(
      'SELECT name, score, LAG(score, 2) OVER (ORDER BY score DESC) AS lag2 FROM scores ORDER BY score DESC'
    ));
    // First 2 rows should have lag2 = NULL
    assert.equal(r[0].lag2, null);
    assert.equal(r[1].lag2, null);
    // Third row should have first row's score
    assert.equal(r[2].lag2, r[0].score);
  });
});

describe('SUM/AVG OVER with PARTITION BY', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('SUM OVER PARTITION BY computes per-partition totals', () => {
    const r = rows(db.execute(
      'SELECT name, dept, score, SUM(score) OVER (PARTITION BY dept) AS dept_total FROM scores ORDER BY dept, name'
    ));
    
    const eng = r.filter(x => x.dept === 'eng');
    const sales = r.filter(x => x.dept === 'sales');
    const hr = r.filter(x => x.dept === 'hr');
    
    // eng: 90 + 85 + 85 = 260
    for (const row of eng) {
      assert.equal(row.dept_total, 260, `eng total should be 260, got ${row.dept_total}`);
    }
    // sales: 95 + 88 + 88 = 271
    for (const row of sales) {
      assert.equal(row.dept_total, 271, `sales total should be 271`);
    }
    // hr: 92
    for (const row of hr) {
      assert.equal(row.dept_total, 92, `hr total should be 92`);
    }
  });

  it('AVG OVER PARTITION BY computes per-partition average', () => {
    const r = rows(db.execute(
      'SELECT dept, AVG(score) OVER (PARTITION BY dept) AS avg_score FROM scores'
    ));
    const eng = r.find(x => x.dept === 'eng');
    const sales = r.find(x => x.dept === 'sales');
    
    // eng: (90+85+85)/3 = 86.67
    assert.ok(Math.abs(eng.avg_score - 86.666666) < 1, `eng avg should be ~86.67, got ${eng.avg_score}`);
  });

  it('COUNT OVER PARTITION BY counts per partition', () => {
    const r = rows(db.execute(
      'SELECT dept, COUNT(*) OVER (PARTITION BY dept) AS cnt FROM scores'
    ));
    const eng = r.find(x => x.dept === 'eng');
    const sales = r.find(x => x.dept === 'sales');
    const hr = r.find(x => x.dept === 'hr');
    
    assert.equal(eng.cnt, 3);
    assert.equal(sales.cnt, 3);
    assert.equal(hr.cnt, 1);
  });
});

describe('Window Function Edge Cases', () => {
  beforeEach(setup);
  afterEach(teardown);

  it('window function on single-row partition', () => {
    const r = rows(db.execute(
      'SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY score) AS rn, ' +
      'RANK() OVER (PARTITION BY dept ORDER BY score) AS rnk ' +
      'FROM scores WHERE dept = \'hr\''
    ));
    assert.equal(r.length, 1);
    assert.equal(r[0].rn, 1);
    assert.equal(r[0].rnk, 1);
  });

  it('window function on empty result set', () => {
    const r = rows(db.execute(
      'SELECT name, ROW_NUMBER() OVER (ORDER BY score) AS rn FROM scores WHERE 1 = 0'
    ));
    assert.equal(r.length, 0);
  });

  it('multiple window functions in same query', () => {
    const r = rows(db.execute(
      'SELECT name, score, ' +
      'ROW_NUMBER() OVER (ORDER BY score DESC) AS rn, ' +
      'RANK() OVER (ORDER BY score DESC) AS rnk, ' +
      'SUM(score) OVER () AS total ' +
      'FROM scores ORDER BY score DESC'
    ));
    assert.equal(r.length, 7);
    assert.equal(r[0].rn, 1);
    // Total should be sum of all scores: 90+85+85+95+88+88+92 = 623
    assert.equal(r[0].total, 623);
  });
});
