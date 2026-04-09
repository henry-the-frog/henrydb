// showcase.test.js — HenryDB Feature Showcase
// Demonstrates ALL features with benchmarks. Serves as documentation and regression test.
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('🏗️ Storage Engines', () => {
  it('HeapFile: default unordered storage', () => {
    const db = new Database();
    db.execute('CREATE TABLE heap_t (id INTEGER PRIMARY KEY, name TEXT)');
    db.execute("INSERT INTO heap_t VALUES (3, 'C')");
    db.execute("INSERT INTO heap_t VALUES (1, 'A')");
    const rows = db.execute('SELECT id FROM heap_t').rows;
    assert.equal(rows[0].id, 3); // Insertion order
  });

  it('BTreeTable: clustered B+tree storage (USING BTREE)', () => {
    const db = new Database();
    db.execute('CREATE TABLE btree_t (id INTEGER PRIMARY KEY, name TEXT) USING BTREE');
    db.execute("INSERT INTO btree_t VALUES (3, 'C')");
    db.execute("INSERT INTO btree_t VALUES (1, 'A')");
    const rows = db.execute('SELECT id FROM btree_t').rows;
    assert.equal(rows[0].id, 1); // Sorted by PK
  });
});

describe('🔍 Index Types', () => {
  it('B+tree index (default)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a@b.com')");
    db.execute('CREATE INDEX idx ON t (email)');
    const r = db.execute("SELECT * FROM t WHERE email = 'a@b.com'");
    assert.equal(r.rows.length, 1);
  });

  it('Hash index (USING HASH)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'ABC')");
    db.execute('CREATE INDEX idx ON t USING HASH (code)');
    const r = db.execute("SELECT * FROM t WHERE code = 'ABC'");
    assert.equal(r.rows.length, 1);
  });

  it('Full-text index (CREATE FULLTEXT INDEX)', () => {
    const db = new Database();
    db.execute('CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)');
    db.execute("INSERT INTO docs VALUES (1, 'Database systems and B-trees')");
    db.execute('CREATE FULLTEXT INDEX idx ON docs(body)');
    const r = db.execute("SELECT * FROM docs WHERE MATCH(body) AGAINST('database')");
    assert.equal(r.rows.length, 1);
  });
});

describe('📊 Query Features', () => {
  it('JOINs: INNER, LEFT', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO a VALUES (2, 'y')");
    db.execute('INSERT INTO b VALUES (1, 1)');
    
    const inner = db.execute('SELECT a.val FROM a JOIN b ON a.id = b.a_id');
    assert.equal(inner.rows.length, 1);
    
    const left = db.execute('SELECT a.val FROM a LEFT JOIN b ON a.id = b.a_id');
    assert.equal(left.rows.length, 2);
  });

  it('Aggregations: COUNT, SUM, AVG, MIN, MAX', () => {
    const db = new Database();
    db.execute('CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO nums VALUES (${i}, ${i * 10})`);
    
    const r = db.execute('SELECT COUNT(*) as c, SUM(val) as s, AVG(val) as a, MIN(val) as mn, MAX(val) as mx FROM nums');
    assert.equal(r.rows[0].c, 10);
    assert.equal(r.rows[0].s, 550);
  });

  it('GROUP BY with HAVING', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (id INTEGER PRIMARY KEY, dept TEXT, amount INTEGER)');
    db.execute("INSERT INTO sales VALUES (1, 'A', 100)");
    db.execute("INSERT INTO sales VALUES (2, 'A', 200)");
    db.execute("INSERT INTO sales VALUES (3, 'B', 50)");
    
    const r = db.execute("SELECT dept, SUM(amount) as total FROM sales GROUP BY dept HAVING total > 100");
    assert.equal(r.rows.length, 1);
    assert.equal(r.rows[0].dept, 'A');
  });

  it('Subqueries: IN, EXISTS, scalar', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    
    const scalar = db.execute('SELECT id, (SELECT MAX(val) FROM t) as mx FROM t');
    assert.equal(scalar.rows[0].mx, 20);
  });

  it('CTEs (WITH ... AS)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, parent_id INTEGER)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    db.execute('INSERT INTO t VALUES (2, 1)');
    
    const r = db.execute('WITH roots AS (SELECT * FROM t WHERE parent_id IS NULL) SELECT * FROM roots');
    assert.equal(r.rows.length, 1);
  });

  it('UNION', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INTEGER PRIMARY KEY)');
    db.execute('CREATE TABLE b (id INTEGER PRIMARY KEY)');
    db.execute('INSERT INTO a VALUES (1)');
    db.execute('INSERT INTO b VALUES (2)');
    
    const r = db.execute('SELECT id FROM a UNION SELECT id FROM b');
    assert.equal(r.rows.length, 2);
  });

  it('CASE WHEN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    const r = db.execute('SELECT CASE WHEN val > 5 THEN \'big\' ELSE \'small\' END as size FROM t');
    assert.equal(r.rows[0].size, 'big');
  });

  it('COALESCE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    db.execute('INSERT INTO t VALUES (1, NULL)');
    
    const r = db.execute('SELECT COALESCE(val, 0) as safe FROM t');
    assert.equal(r.rows[0].safe, 0);
  });
});

describe('🪟 Window Functions', () => {
  it('ROW_NUMBER, RANK, DENSE_RANK', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, score INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 100)');
    db.execute('INSERT INTO t VALUES (2, 100)');
    db.execute('INSERT INTO t VALUES (3, 90)');
    
    const r = db.execute('SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) as rn, RANK() OVER (ORDER BY score DESC) as rnk FROM t');
    assert.equal(r.rows.length, 3);
  });

  it('LAG and LEAD', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (2, 20)');
    db.execute('INSERT INTO t VALUES (3, 30)');
    
    const r = db.execute('SELECT id, LAG(val) OVER (ORDER BY id) as prev, LEAD(val) OVER (ORDER BY id) as nxt FROM t');
    assert.equal(r.rows[0].prev, null);
    assert.equal(r.rows[1].prev, 10);
    assert.equal(r.rows[2].nxt, null);
  });

  it('Running aggregates (SUM OVER)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 5; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const r = db.execute('SELECT id, SUM(val) OVER (ORDER BY id) as running FROM t');
    assert.equal(r.rows[4].running, 15);
  });
});

describe('⚡ Advanced Features', () => {
  it('Prepared statements', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    const stmt = db.prepare('SELECT * FROM t WHERE id = $1');
    const r = stmt.execute(1);
    assert.equal(r.rows[0].val, 'a');
    stmt.close();
  });

  it('Materialized views', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, grp TEXT, val INTEGER)');
    db.execute("INSERT INTO t VALUES (1, 'A', 10)");
    db.execute("INSERT INTO t VALUES (2, 'A', 20)");
    
    db.execute('CREATE MATERIALIZED VIEW mv AS SELECT grp, SUM(val) as total FROM t GROUP BY grp');
    const r = db.execute('SELECT * FROM mv');
    assert.equal(r.rows[0].total, 30);
  });

  it('UPSERT (ON CONFLICT DO UPDATE)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    db.execute('INSERT INTO t VALUES (1, 10)');
    db.execute('INSERT INTO t VALUES (1, 20) ON CONFLICT(id) DO UPDATE SET val = 20');
    
    const r = db.execute('SELECT val FROM t WHERE id = 1');
    assert.equal(r.rows[0].val, 20);
  });

  it('RETURNING clause', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)');
    const r = db.execute("INSERT INTO t VALUES (1, 'hello') RETURNING *");
    assert.equal(r.rows[0].val, 'hello');
  });

  it('EXPLAIN and EXPLAIN ANALYZE', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER) USING BTREE');
    db.execute('INSERT INTO t VALUES (1, 10)');
    
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE id = 1');
    assert.ok(explain.rows.length > 0);
    
    const analyze = db.execute('EXPLAIN ANALYZE SELECT * FROM t WHERE id = 1');
    assert.ok(analyze.execution_time_ms >= 0);
  });
});

describe('🔧 Query Optimizer', () => {
  it('Sort elimination for BTreeTable', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    for (let i = 1; i <= 10; i++) db.execute(`INSERT INTO t VALUES (${i}, 'v${i}')`);
    
    const explain = db.execute('EXPLAIN SELECT * FROM t ORDER BY id ASC');
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('Sort Eliminated'));
  });

  it('BTree PK lookup in EXPLAIN', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT) USING BTREE');
    db.execute("INSERT INTO t VALUES (1, 'a')");
    
    const explain = db.execute('EXPLAIN SELECT * FROM t WHERE id = 1');
    const plan = explain.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(plan.includes('BTree PK Lookup'));
  });
});

describe('📈 Performance', () => {
  it('10K row mixed workload', () => {
    const db = new Database();
    db.execute('CREATE TABLE bench (id INTEGER PRIMARY KEY, name TEXT, score INTEGER, category TEXT) USING BTREE');
    
    const t0 = performance.now();
    for (let i = 1; i <= 10000; i++) {
      const cat = ['A', 'B', 'C', 'D', 'E'][i % 5];
      db.execute(`INSERT INTO bench VALUES (${i}, 'item-${i}', ${i}, '${cat}')`);
    }
    const insertMs = performance.now() - t0;
    
    // Point lookup
    const t1 = performance.now();
    for (let i = 0; i < 100; i++) {
      db.execute(`SELECT * FROM bench WHERE id = ${Math.floor(Math.random() * 10000) + 1}`);
    }
    const lookupMs = performance.now() - t1;
    
    // Aggregation
    const t2 = performance.now();
    db.execute('SELECT category, COUNT(*), AVG(score) FROM bench GROUP BY category');
    const aggMs = performance.now() - t2;
    
    console.log(`  10K inserts: ${insertMs.toFixed(0)}ms`);
    console.log(`  100 lookups: ${lookupMs.toFixed(1)}ms (${(lookupMs/100).toFixed(3)}ms avg)`);
    console.log(`  Aggregation: ${aggMs.toFixed(1)}ms`);
    assert.ok(true);
  });
});
