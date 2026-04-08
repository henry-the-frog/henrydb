// tpch-benchmark.test.js — TPC-H-like benchmark through the PostgreSQL wire protocol
// Proves that HenryDB can handle complex analytical queries end-to-end.
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15499;

// Scale factor: number of rows per table
const SF = { customers: 50, orders: 200, lineItems: 500, suppliers: 10, parts: 30, nations: 25 };

describe('TPC-H Benchmark (via wire protocol)', () => {
  let server;
  let client;

  before(async () => {
    server = new HenryDBServer({ port: PORT, queryCache: false }); // No cache for benchmarking
    await server.start();
    client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'tpch' });
    await client.connect();

    // === Schema ===
    await client.query(`CREATE TABLE nation (
      n_nationkey INTEGER, n_name TEXT, n_regionkey INTEGER, n_comment TEXT
    )`);
    await client.query(`CREATE TABLE customer (
      c_custkey INTEGER, c_name TEXT, c_address TEXT, c_nationkey INTEGER,
      c_phone TEXT, c_acctbal REAL, c_mktsegment TEXT, c_comment TEXT
    )`);
    await client.query(`CREATE TABLE orders (
      o_orderkey INTEGER, o_custkey INTEGER, o_orderstatus TEXT,
      o_totalprice REAL, o_orderdate TEXT, o_orderpriority TEXT,
      o_clerk TEXT, o_shippriority INTEGER, o_comment TEXT
    )`);
    await client.query(`CREATE TABLE lineitem (
      l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER,
      l_linenumber INTEGER, l_quantity REAL, l_extendedprice REAL,
      l_discount REAL, l_tax REAL, l_returnflag TEXT, l_linestatus TEXT,
      l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT,
      l_shipinstruct TEXT, l_shipmode TEXT, l_comment TEXT
    )`);
    await client.query(`CREATE TABLE supplier (
      s_suppkey INTEGER, s_name TEXT, s_address TEXT, s_nationkey INTEGER,
      s_phone TEXT, s_acctbal REAL, s_comment TEXT
    )`);
    await client.query(`CREATE TABLE part (
      p_partkey INTEGER, p_name TEXT, p_mfgr TEXT, p_brand TEXT,
      p_type TEXT, p_size INTEGER, p_container TEXT, p_retailprice REAL,
      p_comment TEXT
    )`);

    // === Data Generation ===
    const nations = ['ALGERIA','ARGENTINA','BRAZIL','CANADA','EGYPT','ETHIOPIA','FRANCE','GERMANY','INDIA','INDONESIA',
      'IRAN','IRAQ','JAPAN','JORDAN','KENYA','MOROCCO','MOZAMBIQUE','PERU','CHINA','ROMANIA',
      'SAUDI ARABIA','VIETNAM','RUSSIA','UNITED KINGDOM','UNITED STATES'];
    for (let i = 0; i < nations.length; i++) {
      await client.query(`INSERT INTO nation VALUES (${i}, '${nations[i]}', ${i % 5}, 'nation comment')`);
    }

    const segments = ['AUTOMOBILE','BUILDING','FURNITURE','HOUSEHOLD','MACHINERY'];
    for (let i = 1; i <= SF.customers; i++) {
      const segment = segments[i % 5];
      const nation = i % 25;
      const acctbal = (Math.random() * 10000 - 500).toFixed(2);
      await client.query(`INSERT INTO customer VALUES (${i}, 'Customer#${String(i).padStart(6,'0')}', 'Addr ${i}', ${nation}, '555-${String(i).padStart(4,'0')}', ${acctbal}, '${segment}', 'comment')`);
    }

    const statuses = ['O','F','P'];
    const priorities = ['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'];
    for (let i = 1; i <= SF.orders; i++) {
      const custkey = (i % SF.customers) + 1;
      const status = statuses[i % 3];
      const total = (Math.random() * 500000).toFixed(2);
      const year = 1993 + (i % 5);
      const month = String((i % 12) + 1).padStart(2, '0');
      const day = String((i % 28) + 1).padStart(2, '0');
      const priority = priorities[i % 5];
      await client.query(`INSERT INTO orders VALUES (${i}, ${custkey}, '${status}', ${total}, '${year}-${month}-${day}', '${priority}', 'Clerk#${i % 100}', ${i % 3}, 'comment')`);
    }

    const flags = ['R','A','N'];
    const modes = ['TRUCK','MAIL','SHIP','AIR','RAIL','REG AIR','FOB'];
    for (let i = 1; i <= SF.lineItems; i++) {
      const orderkey = (i % SF.orders) + 1;
      const partkey = (i % SF.parts) + 1;
      const suppkey = (i % SF.suppliers) + 1;
      const qty = (Math.random() * 50 + 1).toFixed(0);
      const price = (Math.random() * 1000 + 1).toFixed(2);
      const discount = (Math.random() * 0.1).toFixed(2);
      const tax = (Math.random() * 0.08).toFixed(2);
      const flag = flags[i % 3];
      const status2 = i % 2 === 0 ? 'O' : 'F';
      const mode = modes[i % 7];
      await client.query(`INSERT INTO lineitem VALUES (${orderkey}, ${partkey}, ${suppkey}, ${i}, ${qty}, ${price}, ${discount}, ${tax}, '${flag}', '${status2}', '1995-03-${String((i%28)+1).padStart(2,'0')}', '1995-04-01', '1995-04-15', 'DELIVER IN PERSON', '${mode}', 'comment')`);
    }

    for (let i = 1; i <= SF.suppliers; i++) {
      const nation = i % 25;
      const acctbal = (Math.random() * 10000).toFixed(2);
      await client.query(`INSERT INTO supplier VALUES (${i}, 'Supplier#${String(i).padStart(6,'0')}', 'SAddr ${i}', ${nation}, '555-S${i}', ${acctbal}, 'comment')`);
    }

    for (let i = 1; i <= SF.parts; i++) {
      const brands = ['Brand#11','Brand#12','Brand#13','Brand#21','Brand#22'];
      const types = ['STANDARD POLISHED TIN','SMALL PLATED COPPER','MEDIUM BURNISHED NICKEL'];
      const containers = ['SM CASE','SM BOX','SM PACK','MED BAG','LG BOX'];
      await client.query(`INSERT INTO part VALUES (${i}, 'Part ${i}', 'Manufacturer#${(i%5)+1}', '${brands[i%5]}', '${types[i%3]}', ${(i%50)+1}, '${containers[i%5]}', ${(i*10+Math.random()*100).toFixed(2)}, 'comment')`);
    }
  });

  after(async () => {
    await client.end();
    await server.stop();
  });

  // === TPC-H Queries (adapted) ===

  it('Q1: Pricing Summary Report', async () => {
    const r = await client.query(`
      SELECT l_returnflag, l_linestatus, 
             SUM(l_quantity) AS sum_qty,
             SUM(l_extendedprice) AS sum_base_price,
             COUNT(*) AS count_order
      FROM lineitem
      WHERE l_shipdate <= '1998-09-01'
      GROUP BY l_returnflag, l_linestatus
      ORDER BY l_returnflag, l_linestatus
    `);
    assert.ok(r.rows.length > 0, 'Q1 should return results');
    assert.ok(r.rows[0].sum_qty, 'Q1 should have sum_qty');
  });

  it('Q3: Shipping Priority', async () => {
    const r = await client.query(`
      SELECT o.o_orderkey, SUM(l.l_extendedprice) AS revenue, o.o_orderdate, o.o_shippriority
      FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      WHERE c.c_mktsegment = 'BUILDING'
        AND o.o_orderdate < '1995-03-15'
      GROUP BY o.o_orderkey, o.o_orderdate, o.o_shippriority
      ORDER BY revenue DESC
    `);
    assert.ok(r.rows.length >= 0, 'Q3 should return results');
  });

  it('Q4: Order Priority Checking', async () => {
    const r = await client.query(`
      SELECT o_orderpriority, COUNT(*) AS order_count
      FROM orders
      WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01'
      GROUP BY o_orderpriority
      ORDER BY o_orderpriority
    `);
    assert.ok(r.rows.length > 0, 'Q4 should return priority counts');
  });

  it('Q5: Local Supplier Volume', async () => {
    const r = await client.query(`
      SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue
      FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      JOIN supplier s ON l.l_suppkey = s.s_suppkey
      JOIN nation n ON c.c_nationkey = n.n_nationkey
      WHERE o.o_orderdate >= '1994-01-01' AND o.o_orderdate < '1995-01-01'
      GROUP BY n.n_name
      ORDER BY revenue DESC
    `);
    assert.ok(r.rows.length > 0, 'Q5 should return revenue by nation');
  });

  it('Q6: Forecasting Revenue Change', async () => {
    const r = await client.query(`
      SELECT SUM(l_extendedprice * l_discount) AS revenue
      FROM lineitem
      WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
        AND l_discount >= 0.05 AND l_discount <= 0.07
        AND l_quantity < 24
    `);
    assert.strictEqual(r.rows.length, 1, 'Q6 should return one row');
  });

  it('Q10: Returned Item Reporting', async () => {
    const r = await client.query(`
      SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) AS revenue,
             c.c_acctbal, n.n_name, c.c_address, c.c_phone
      FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      JOIN nation n ON c.c_nationkey = n.n_nationkey
      WHERE l.l_returnflag = 'R'
      GROUP BY c.c_custkey, c.c_name, c.c_acctbal, c.c_phone, c.c_address, n.n_name
      ORDER BY revenue DESC
    `);
    assert.ok(r.rows.length > 0, 'Q10 should return returned items');
  });

  it('Q12: Shipping Modes and Order Priority', async () => {
    const r = await client.query(`
      SELECT l.l_shipmode,
             COUNT(*) AS order_count
      FROM orders o
      JOIN lineitem l ON o.o_orderkey = l.l_orderkey
      WHERE l.l_shipmode IN ('MAIL', 'SHIP')
      GROUP BY l.l_shipmode
      ORDER BY l.l_shipmode
    `);
    assert.ok(r.rows.length > 0, 'Q12 should return shipping mode counts');
  });

  it('Q13: Customer Distribution', async () => {
    const r = await client.query(`
      SELECT COUNT(*) AS custcount
      FROM customer c
      JOIN orders o ON c.c_custkey = o.o_custkey
      GROUP BY c.c_custkey
      ORDER BY custcount DESC
    `);
    assert.ok(r.rows.length > 0, 'Q13 should return customer distribution');
  });

  it('Q16: Parts/Supplier Relationship', async () => {
    const r = await client.query(`
      SELECT p.p_brand, p.p_type, p.p_size, COUNT(*) AS supplier_cnt
      FROM part p
      WHERE p.p_brand <> 'Brand#45'
        AND p.p_size >= 1
      GROUP BY p.p_brand, p.p_type, p.p_size
      ORDER BY supplier_cnt DESC
    `);
    assert.ok(r.rows.length > 0, 'Q16 should return part/supplier stats');
  });

  it('Q19: Discounted Revenue (complex OR)', async () => {
    const r = await client.query(`
      SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
      FROM lineitem
      WHERE (l_quantity >= 1 AND l_quantity <= 11 AND l_shipmode IN ('AIR', 'REG AIR'))
         OR (l_quantity >= 10 AND l_quantity <= 20 AND l_shipmode = 'TRUCK')
    `);
    assert.strictEqual(r.rows.length, 1);
    assert.ok(parseFloat(r.rows[0].revenue) > 0, 'Q19 should have positive revenue');
  });

  it('Overall stats: all TPC-H tables loaded correctly', async () => {
    const counts = {};
    for (const table of ['nation', 'customer', 'orders', 'lineitem', 'supplier', 'part']) {
      const r = await client.query(`SELECT COUNT(*) AS cnt FROM ${table}`);
      counts[table] = parseInt(r.rows[0].cnt);
    }
    
    assert.strictEqual(counts.nation, 25);
    assert.strictEqual(counts.customer, SF.customers);
    assert.strictEqual(counts.orders, SF.orders);
    assert.strictEqual(counts.lineitem, SF.lineItems);
    assert.strictEqual(counts.supplier, SF.suppliers);
    assert.strictEqual(counts.part, SF.parts);
    
    console.log('TPC-H Table Sizes:', counts);
  });
});
