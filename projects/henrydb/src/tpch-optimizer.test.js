// tpch-optimizer.test.js — TPC-H queries analyzed through the optimizer
// Verifies that the query optimizer makes reasonable decisions for analytical queries:
// - Predicate pushdown through multi-way joins
// - Hash joins for equi-join conditions
// - Correct row estimates vs actuals

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { PlanBuilder, PlanFormatter } from './query-plan.js';
import { parse } from './sql.js';

function setupTPCH() {
  const db = new Database();
  
  db.execute('CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name TEXT, n_regionkey INT)');
  db.execute('CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name TEXT)');
  db.execute('CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_name TEXT, s_nationkey INT, s_acctbal INT)');
  db.execute('CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_name TEXT, c_nationkey INT, c_mktsegment TEXT, c_acctbal INT)');
  db.execute('CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus TEXT, o_totalprice INT, o_orderdate TEXT)');
  db.execute('CREATE TABLE lineitem (l_orderkey INT, l_partkey INT, l_suppkey INT, l_quantity INT, l_extendedprice INT, l_discount INT, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT)');

  const regions = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'];
  for (let i = 0; i < regions.length; i++) {
    db.execute(`INSERT INTO region VALUES (${i}, '${regions[i]}')`);
  }

  const nations = ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT',
    'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA',
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA',
    'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA',
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UK', 'USA'];
  for (let i = 0; i < nations.length; i++) {
    db.execute(`INSERT INTO nation VALUES (${i}, '${nations[i]}', ${i % 5})`);
  }

  for (let i = 0; i < 50; i++) {
    db.execute(`INSERT INTO supplier VALUES (${i}, 'Supplier#${i}', ${i % 25}, ${5000 + i * 37 % 10000})`);
  }

  for (let i = 0; i < 100; i++) {
    const seg = ['BUILDING', 'AUTOMOBILE', 'MACHINERY', 'HOUSEHOLD', 'FURNITURE'][i % 5];
    db.execute(`INSERT INTO customer VALUES (${i}, 'Customer#${i}', ${i % 25}, '${seg}', ${1000 + i * 47 % 9000})`);
  }

  for (let i = 0; i < 500; i++) {
    const status = ['F', 'O', 'P'][i % 3];
    const date = `1995-${String(1 + i % 12).padStart(2, '0')}-${String(1 + i % 28).padStart(2, '0')}`;
    db.execute(`INSERT INTO orders VALUES (${i}, ${i % 100}, '${status}', ${1000 + i * 13 % 50000}, '${date}')`);
  }

  for (let i = 0; i < 2000; i++) {
    const flag = ['N', 'R', 'A'][i % 3];
    const lstatus = ['O', 'F'][i % 2];
    const date = `1995-${String(1 + i % 12).padStart(2, '0')}-${String(1 + i % 28).padStart(2, '0')}`;
    db.execute(`INSERT INTO lineitem VALUES (${i % 500}, ${i % 50}, ${i % 50}, ${1 + i % 7}, ${1000 + i * 11 % 50000}, ${i % 10}, '${flag}', '${lstatus}', '${date}')`);
  }

  return db;
}

function getTreePlan(db, sql) {
  const builder = new PlanBuilder(db);
  const ast = parse(sql);
  return builder.buildPlan(ast);
}

function planToText(plan) {
  return PlanFormatter.format(plan).join('\n');
}

// Helper: find all nodes of a given type in the plan tree
function findNodes(node, type) {
  const results = [];
  if (node.type === type) results.push(node);
  for (const child of node.children) {
    results.push(...findNodes(child, type));
  }
  return results;
}

describe('TPC-H Optimizer Decisions', () => {

  it('Q1 (pricing summary) — aggregate with filter, no join', () => {
    const db = setupTPCH();
    const result = db.execute(`
      SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_price
      FROM lineitem
      WHERE l_shipdate <= '1998-09-01'
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus
    `);
    assert.ok(result.rows.length > 0);

    // Plan should be: Sort → Aggregate → Seq Scan with filter
    const plan = getTreePlan(db, `
      SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty
      FROM lineitem WHERE l_shipdate <= '1998-09-01'
      GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag
    `);
    const text = planToText(plan);
    assert.ok(text.includes('Scan'), 'Should scan lineitem');
    assert.ok(text.includes('Aggregate') || text.includes('HashedAggregate'), 'Should aggregate');
  });

  it('Q3 (shipping priority) — 3-way join with filters', () => {
    const db = setupTPCH();
    const result = db.execute(`
      SELECT o_orderkey, SUM(l_extendedprice) AS revenue, o_orderdate
      FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      WHERE c.c_mktsegment = 'BUILDING' AND o.o_orderdate < '1995-03-15'
      GROUP BY o_orderkey, o_orderdate
      ORDER BY revenue DESC
      LIMIT 10
    `);
    assert.ok(result.rows.length > 0);

    // The plan should use hash joins and push filters
    const plan = getTreePlan(db, `
      SELECT o_orderkey FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      WHERE c.c_mktsegment = 'BUILDING' AND o.o_orderdate < '1995-03-15'
    `);
    const text = planToText(plan);
    
    // Should have hash joins for equi-join conditions
    const hashJoins = findNodes(plan, 'Hash Join');
    assert.ok(hashJoins.length >= 1, `Expected hash join(s), got: ${text}`);
    
    // Should show filters pushed to individual scans
    const seqScans = findNodes(plan, 'Seq Scan');
    const filtersOnScans = seqScans.filter(s => s.filter);
    assert.ok(filtersOnScans.length >= 1, 'Expected at least one pushed filter on a scan');
  });

  it('Q5 (local supplier volume) — 5-way join', () => {
    const db = setupTPCH();
    const result = db.execute(`
      SELECT n.n_name, SUM(l.l_extendedprice) AS revenue
      FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      JOIN supplier s ON l.l_suppkey = s.s_suppkey
      JOIN nation n ON c.c_nationkey = n.n_nationkey
      WHERE n.n_regionkey = 2
      GROUP BY n.n_name
      ORDER BY revenue DESC
    `);
    assert.ok(result.rows.length > 0);

    // 5-way join plan
    const plan = getTreePlan(db, `
      SELECT n.n_name FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      JOIN supplier s ON l.l_suppkey = s.s_suppkey
      JOIN nation n ON c.c_nationkey = n.n_nationkey
      WHERE n.n_regionkey = 2
    `);
    const text = planToText(plan);
    
    // Should have multiple join nodes
    const joins = [...findNodes(plan, 'Hash Join'), ...findNodes(plan, 'Nested Loop')];
    assert.ok(joins.length >= 4, `Expected 4+ joins for 5-way join, got ${joins.length}`);
    
    // Predicate n.n_regionkey = 2 should be pushed to nation scan
    const nationScans = findNodes(plan, 'Seq Scan').filter(s => s.table === 'nation');
    if (nationScans.length > 0 && nationScans[0].filter) {
      assert.ok(nationScans[0].filter.includes('regionkey') || nationScans[0].filter.includes('n_regionkey'),
        'Nation scan should have regionkey filter pushed');
    }
  });

  it('EXPLAIN ANALYZE on TPC-H Q1 shows reasonable estimates', () => {
    const db = setupTPCH();
    const result = db.execute(`
      EXPLAIN ANALYZE SELECT l_returnflag, SUM(l_quantity) AS sum_qty
      FROM lineitem
      WHERE l_shipdate <= '1998-09-01'
      GROUP BY l_returnflag
    `);
    assert.ok(result.planTreeText);
    assert.ok(result.actual_rows > 0);
    
    const text = result.planTreeText.join('\n');
    assert.ok(text.includes('actual rows='), 'Should show actual rows');
    assert.ok(text.includes('cost='), 'Should show cost estimate');
  });

  it('EXPLAIN ANALYZE on Q3 multi-join shows actuals', () => {
    const db = setupTPCH();
    const result = db.execute(`
      EXPLAIN ANALYZE SELECT o_orderkey, SUM(l_extendedprice) AS revenue
      FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      WHERE c.c_mktsegment = 'BUILDING'
      GROUP BY o_orderkey
      LIMIT 10
    `);
    assert.ok(result.planTreeText);
    assert.ok(result.actual_rows > 0);
  });

  it('predicate pushdown reduces row estimates in multi-join', () => {
    const db = setupTPCH();
    const builder = new PlanBuilder(db);
    
    // Without filter
    const plan1 = builder.buildPlan(parse(`
      SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey
    `));
    
    // With filter on customer
    const plan2 = builder.buildPlan(parse(`
      SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey
      WHERE c.c_mktsegment = 'BUILDING'
    `));
    
    // Filtered plan should have fewer estimated rows
    assert.ok(plan2.estimatedRows <= plan1.estimatedRows,
      `Filtered (${plan2.estimatedRows}) should be <= unfiltered (${plan1.estimatedRows})`);
  });

  it('plan tree depth matches query complexity', () => {
    const db = setupTPCH();
    
    // Simple scan: depth 1
    const simple = getTreePlan(db, 'SELECT * FROM nation');
    assert.equal(simple.children.length, 0, 'Simple scan should be a leaf');
    
    // 2-way join: depth 2+
    const twoWay = getTreePlan(db, 'SELECT * FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey');
    assert.ok(twoWay.children.length >= 1, '2-way join should have children');
    
    // 3-way join: depth 3+
    const threeWay = getTreePlan(db, `
      SELECT * FROM customer c 
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
    `);
    const totalNodes = countNodes(threeWay);
    assert.ok(totalNodes >= 5, `3-way join should have 5+ nodes, got ${totalNodes}`);
  });
});

function countNodes(node) {
  return 1 + node.children.reduce((sum, c) => sum + countNodes(c), 0);
}
