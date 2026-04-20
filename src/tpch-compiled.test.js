// tpch-compiled.test.js — TPC-H-style queries testing planner + compiled execution
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CompiledQueryEngine } from './compiled-query.js';
import { Database } from './db.js';

function setupTPCH(scale = 1) {
  const db = new Database();
  
  // TPC-H-like schema (simplified)
  db.execute(`CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name TEXT, n_regionkey INT)`);
  db.execute(`CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name TEXT)`);
  db.execute(`CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_name TEXT, s_nationkey INT, s_acctbal INT)`);
  db.execute(`CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_name TEXT, c_nationkey INT, c_mktsegment TEXT, c_acctbal INT)`);
  db.execute(`CREATE TABLE part (p_partkey INT PRIMARY KEY, p_name TEXT, p_brand TEXT, p_type TEXT, p_size INT, p_retailprice INT)`);
  db.execute(`CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus TEXT, o_totalprice INT, o_orderdate TEXT, o_orderpriority TEXT)`);
  db.execute(`CREATE TABLE lineitem (l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity INT, l_extendedprice INT, l_discount INT, l_tax INT, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT)`);

  // Regions (5)
  const regions = ['AFRICA', 'AMERICA', 'ASIA', 'EUROPE', 'MIDDLE EAST'];
  for (let i = 0; i < regions.length; i++) {
    db.execute(`INSERT INTO region VALUES (${i}, '${regions[i]}')`);
  }

  // Nations (25)
  const nations = ['ALGERIA', 'ARGENTINA', 'BRAZIL', 'CANADA', 'EGYPT',
    'ETHIOPIA', 'FRANCE', 'GERMANY', 'INDIA', 'INDONESIA',
    'IRAN', 'IRAQ', 'JAPAN', 'JORDAN', 'KENYA',
    'MOROCCO', 'MOZAMBIQUE', 'PERU', 'CHINA', 'ROMANIA',
    'SAUDI ARABIA', 'VIETNAM', 'RUSSIA', 'UK', 'USA'];
  for (let i = 0; i < nations.length; i++) {
    db.execute(`INSERT INTO nation VALUES (${i}, '${nations[i]}', ${i % 5})`);
  }

  // Suppliers (100 * scale)
  const numSuppliers = 100 * scale;
  for (let i = 0; i < numSuppliers; i++) {
    db.execute(`INSERT INTO supplier VALUES (${i}, 'Supplier#${String(i).padStart(4, '0')}', ${i % 25}, ${(i * 37 + 5000) % 10000})`);
  }

  // Customers (150 * scale)
  const numCustomers = 150 * scale;
  const segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'HOUSEHOLD', 'MACHINERY'];
  for (let i = 0; i < numCustomers; i++) {
    db.execute(`INSERT INTO customer VALUES (${i}, 'Customer#${String(i).padStart(6, '0')}', ${i % 25}, '${segments[i % 5]}', ${(i * 41 + 3000) % 10000})`);
  }

  // Parts (200 * scale)
  const numParts = 200 * scale;
  const brands = ['Brand#11', 'Brand#12', 'Brand#21', 'Brand#22', 'Brand#31'];
  const types = ['ECONOMY ANODIZED STEEL', 'STANDARD POLISHED TIN', 'PROMO BRUSHED BRASS', 'LARGE PLATED COPPER'];
  for (let i = 0; i < numParts; i++) {
    db.execute(`INSERT INTO part VALUES (${i}, 'Part#${i}', '${brands[i % 5]}', '${types[i % 4]}', ${(i % 50) + 1}, ${(i * 13 + 100) % 2000})`);
  }

  // Orders (600 * scale)
  const numOrders = 600 * scale;
  const priorities = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'];
  const statuses = ['O', 'F', 'P'];
  for (let i = 0; i < numOrders; i++) {
    const custkey = i % numCustomers;
    const year = 1993 + (i % 5);
    const month = String((i % 12) + 1).padStart(2, '0');
    const day = String((i % 28) + 1).padStart(2, '0');
    db.execute(`INSERT INTO orders VALUES (${i}, ${custkey}, '${statuses[i % 3]}', ${(i * 73 + 1000) % 50000}, '${year}-${month}-${day}', '${priorities[i % 5]}')`);
  }

  // Lineitems (2400 * scale)
  const numLineitems = 2400 * scale;
  const flags = ['R', 'A', 'N'];
  const lineStatuses = ['O', 'F'];
  for (let i = 0; i < numLineitems; i++) {
    const orderkey = i % numOrders;
    const partkey = i % numParts;
    const suppkey = i % numSuppliers;
    const year = 1993 + (i % 5);
    const month = String((i % 12) + 1).padStart(2, '0');
    const day = String((i % 28) + 1).padStart(2, '0');
    db.execute(`INSERT INTO lineitem VALUES (${orderkey}, ${partkey}, ${suppkey}, ${(i % 7) + 1}, ${(i % 50) + 1}, ${(i * 31 + 500) % 10000}, ${i % 10}, ${(i + 3) % 8}, '${flags[i % 3]}', '${lineStatuses[i % 2]}', '${year}-${month}-${day}')`);
  }

  return { db, numCustomers, numOrders, numLineitems, numParts, numSuppliers };
}

describe('TPC-H Compiled Queries', { timeout: 120000 }, () => {
  
  it('Q1-like: lineitem pricing summary (single table aggregate)', () => {
    const { db } = setupTPCH(1);
    const engine = new CompiledQueryEngine(db);

    // Single-table scan with filter — always compilable
    const ast = {
      type: 'SELECT',
      columns: [{ name: 'l_returnflag' }, { name: 'l_linestatus' }, { name: 'l_quantity' }, { name: 'l_extendedprice' }],
      from: { table: 'lineitem' },
      where: {
        type: 'COMPARE', op: 'LE',
        left: { type: 'column_ref', name: 'l_shipdate' },
        right: { type: 'literal', value: '1995-06-15' }
      },
      limit: { value: 100 }
    };

    const result = engine.executeSelect(ast);
    assert.ok(result, 'Should compile lineitem scan');
    assert.equal(result.rows.length, 100);
    assert.ok(result.rows[0].l_returnflag);
  });

  it('Q3-like: customer-orders join with filter', () => {
    const { db } = setupTPCH(1);
    const engine = new CompiledQueryEngine(db);

    // Customer JOIN Orders — planner should choose hash join
    const ast = {
      type: 'SELECT',
      columns: [{ name: 'c_name', table: 'c' }, { name: 'o_totalprice', table: 'o' }, { name: 'o_orderdate', table: 'o' }],
      from: { table: 'customer', alias: 'c' },
      joins: [{
        table: 'orders',
        alias: 'o',
        joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'c_custkey' },
          right: { type: 'column_ref', table: 'o', name: 'o_custkey' }
        }
      }],
      limit: { value: 50 }
    };

    const result = engine.executeSelect(ast);
    assert.ok(result, 'Should compile customer-orders join');
    assert.equal(result.rows.length, 50);
  });

  it('Q5-like: three-table join (customer → orders → lineitem)', () => {
    const { db } = setupTPCH(1);
    const engine = new CompiledQueryEngine(db);

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customer', alias: 'c' },
      joins: [
        {
          table: 'orders', alias: 'o', joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'c', name: 'c_custkey' },
            right: { type: 'column_ref', table: 'o', name: 'o_custkey' }
          }
        },
        {
          table: 'lineitem', alias: 'l', joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'o', name: 'o_orderkey' },
            right: { type: 'column_ref', table: 'l', name: 'l_orderkey' }
          }
        }
      ],
      limit: { value: 100 }
    };

    const result = engine.executeSelect(ast);
    assert.ok(result, 'Should compile 3-table join');
    assert.equal(result.rows.length, 100);
  });

  it('Q9-like: four-table join (part → lineitem → supplier → nation)', () => {
    const { db } = setupTPCH(1);
    const engine = new CompiledQueryEngine(db);

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'part', alias: 'p' },
      joins: [
        {
          table: 'lineitem', alias: 'l', joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'p', name: 'p_partkey' },
            right: { type: 'column_ref', table: 'l', name: 'l_partkey' }
          }
        },
        {
          table: 'supplier', alias: 's', joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'l', name: 'l_suppkey' },
            right: { type: 'column_ref', table: 's', name: 's_suppkey' }
          }
        },
        {
          table: 'nation', alias: 'n', joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 's', name: 's_nationkey' },
            right: { type: 'column_ref', table: 'n', name: 'n_nationkey' }
          }
        }
      ],
      limit: { value: 200 }
    };

    const result = engine.executeSelect(ast);
    assert.ok(result, 'Should compile 4-table join');
    assert.equal(result.rows.length, 200);
  });

  it('EXPLAIN COMPILED shows join strategy', () => {
    const { db } = setupTPCH(1);
    const engine = new CompiledQueryEngine(db);

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customer', alias: 'c' },
      joins: [{
        table: 'orders', alias: 'o', joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'c_custkey' },
          right: { type: 'column_ref', table: 'o', name: 'o_custkey' }
        }
      }],
    };

    const explain = engine.explainCompiled(ast);
    assert.ok(explain.includes('customer'));
    assert.ok(explain.includes('Join'));
  });

  it('correctness: compiled join matches standard execution', () => {
    const { db } = setupTPCH(1);
    const engine = new CompiledQueryEngine(db);

    // Compiled
    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customer', alias: 'c' },
      joins: [{
        table: 'orders', alias: 'o', joinType: 'INNER',
        on: {
          type: 'COMPARE', op: 'EQ',
          left: { type: 'column_ref', table: 'c', name: 'c_custkey' },
          right: { type: 'column_ref', table: 'o', name: 'o_custkey' }
        }
      }],
    };

    const compiled = engine.executeSelect(ast);
    const standard = db.execute('SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey');

    assert.equal(compiled.rows.length, standard.rows.length,
      `Compiled: ${compiled.rows.length} vs Standard: ${standard.rows.length}`);
  });

  it('benchmark: compiled vs standard on TPC-H 3-table join', () => {
    const { db } = setupTPCH(1);
    const engine = new CompiledQueryEngine(db);

    const ast = {
      type: 'SELECT',
      columns: [{ name: '*' }],
      from: { table: 'customer', alias: 'c' },
      joins: [
        {
          table: 'orders', alias: 'o', joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'c', name: 'c_custkey' },
            right: { type: 'column_ref', table: 'o', name: 'o_custkey' }
          }
        },
        {
          table: 'lineitem', alias: 'l', joinType: 'INNER',
          on: {
            type: 'COMPARE', op: 'EQ',
            left: { type: 'column_ref', table: 'o', name: 'o_orderkey' },
            right: { type: 'column_ref', table: 'l', name: 'l_orderkey' }
          }
        }
      ],
      limit: { value: 500 }
    };

    const t0 = Date.now();
    const compiled = engine.executeSelect(ast);
    const compiledMs = Date.now() - t0;

    const t1 = Date.now();
    const standard = db.execute('SELECT * FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON o.o_orderkey = l.l_orderkey LIMIT 500');
    const standardMs = Date.now() - t1;

    console.log(`    TPC-H 3-table: Compiled ${compiledMs}ms vs Standard ${standardMs}ms (${(standardMs/Math.max(compiledMs,1)).toFixed(1)}x speedup)`);
    assert.ok(compiled);
    assert.equal(compiled.rows.length, 500);
  });
});
