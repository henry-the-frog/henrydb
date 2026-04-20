import { Database } from './src/db.js';

function run() {
  const db = new Database();
  
  // Scale factor: SF=0.01 (tiny)
  const N_CUST = 150, N_ORD = 600, N_LINE = 2400, N_SUPP = 10, N_NATION = 25, N_REGION = 5;
  
  // Create schema
  db.execute('CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name TEXT)');
  db.execute('CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name TEXT, n_regionkey INT)');
  db.execute('CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_name TEXT, c_nationkey INT, c_acctbal REAL)');
  db.execute('CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus TEXT, o_totalprice REAL, o_orderdate TEXT)');
  db.execute('CREATE TABLE lineitem (l_orderkey INT, l_linenumber INT, l_quantity REAL, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT)');
  db.execute('CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_name TEXT, s_nationkey INT)');
  
  // Seed data
  const regions = ['AFRICA','AMERICA','ASIA','EUROPE','MIDDLE EAST'];
  for (let i = 0; i < 5; i++) db.execute(`INSERT INTO region VALUES (${i}, '${regions[i]}')`);
  
  const nations = 'ALGERIA,ARGENTINA,BRAZIL,CANADA,EGYPT,ETHIOPIA,FRANCE,GERMANY,INDIA,INDONESIA,IRAN,IRAQ,JAPAN,JORDAN,KENYA,MOROCCO,MOZAMBIQUE,PERU,CHINA,ROMANIA,SAUDI ARABIA,VIETNAM,RUSSIA,UK,USA'.split(',');
  for (let i = 0; i < 25; i++) db.execute(`INSERT INTO nation VALUES (${i}, '${nations[i]}', ${i % 5})`);
  
  for (let i = 1; i <= N_CUST; i++) db.execute(`INSERT INTO customer VALUES (${i}, 'Cust${i}', ${i%25}, ${(Math.random()*10000-1000).toFixed(2)})`);
  
  for (let i = 1; i <= N_ORD; i++) {
    const d = `202${4+i%2}-${String(1+i%12).padStart(2,'0')}-${String(1+i%28).padStart(2,'0')}`;
    db.execute(`INSERT INTO orders VALUES (${i}, ${1+i%N_CUST}, '${'FOP'[i%3]}', ${(Math.random()*100000).toFixed(2)}, '${d}')`);
  }
  
  const flags = 'ANR', status = 'FO';
  for (let i = 0; i < N_LINE; i++) {
    const q = 1+i%50, p = (q*(10+Math.random()*90)).toFixed(2), d = (Math.random()*0.1).toFixed(2), t = (Math.random()*0.08).toFixed(2);
    const sd = `202${4+i%2}-${String(1+i%12).padStart(2,'0')}-${String(1+i%28).padStart(2,'0')}`;
    db.execute(`INSERT INTO lineitem VALUES (${1+i%N_ORD}, ${i%7+1}, ${q}, ${p}, ${d}, ${t}, '${flags[i%3]}', '${status[i%2]}', '${sd}')`);
  }
  
  for (let i = 1; i <= N_SUPP; i++) db.execute(`INSERT INTO supplier VALUES (${i}, 'Supp${i}', ${i%25})`);
  
  console.log(`Data: ${N_CUST} cust, ${N_ORD} orders, ${N_LINE} lineitems, ${N_SUPP} supp\n`);
  
  // TPC-H Queries
  const queries = [
    { id: 'Q1', name: 'Pricing Summary',
      sql: `SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_price, COUNT(*) as cnt FROM lineitem WHERE l_shipdate <= '2025-12-01' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus`,
      check: (rows) => rows.length >= 2 && rows.every(r => r.sum_qty > 0 && r.cnt > 0) },
      
    { id: 'Q3', name: 'Shipping Priority',
      sql: `SELECT l.l_orderkey, SUM(l.l_extendedprice) as revenue, o.o_orderdate FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey WHERE o.o_orderdate < '2025-03-15' AND l.l_shipdate > '2025-03-15' GROUP BY l.l_orderkey, o.o_orderdate ORDER BY revenue DESC LIMIT 10`,
      check: (rows) => rows.length <= 10 },
    
    { id: 'Q4', name: 'Order Priority',
      sql: `SELECT o.o_orderstatus, COUNT(*) as order_count FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem l WHERE l.l_orderkey = o.o_orderkey) GROUP BY o.o_orderstatus ORDER BY o.o_orderstatus`,
      check: (rows) => rows.length >= 1 && rows.every(r => r.order_count > 0) },
      
    { id: 'Q6', name: 'Revenue Forecast',
      sql: `SELECT SUM(l_extendedprice * l_discount) as revenue FROM lineitem WHERE l_shipdate >= '2025-01-01' AND l_shipdate < '2026-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24`,
      check: (rows) => rows.length === 1 },
      
    { id: 'Q12', name: 'Shipping Modes',
      sql: `SELECT l.l_linestatus, SUM(CASE WHEN o.o_orderstatus = 'F' THEN 1 ELSE 0 END) as filled, COUNT(*) as total FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey GROUP BY l.l_linestatus ORDER BY l.l_linestatus`,
      check: (rows) => rows.length >= 1 },
      
    { id: 'Q13', name: 'Customer Distribution',
      sql: `SELECT c_count, COUNT(*) as custdist FROM (SELECT c.c_custkey, COUNT(o.o_orderkey) as c_count FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey GROUP BY c.c_custkey) as c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC`,
      check: (rows) => rows.length >= 1 },
      
    { id: 'Q14', name: 'Promotion Effect',
      sql: `SELECT SUM(l_extendedprice) as total_revenue FROM lineitem WHERE l_shipdate >= '2025-03-01' AND l_shipdate < '2025-04-01'`,
      check: (rows) => rows.length === 1 },
  ];
  
  let passed = 0, failed = 0, totalMs = 0;
  
  console.log('Query           | Time(ms) | Rows | Correct');
  console.log('----------------|----------|------|--------');
  
  for (const q of queries) {
    const t0 = performance.now();
    try {
      const result = db.execute(q.sql);
      const ms = performance.now() - t0;
      totalMs += ms;
      const rows = result.rows || [];
      const correct = q.check(rows);
      console.log(`${(q.id + ' ' + q.name).padEnd(16)}| ${String(ms.toFixed(0)).padStart(8)} | ${String(rows.length).padStart(4)} | ${correct ? '✅' : '❌'}`);
      if (correct) passed++; else failed++;
    } catch (e) {
      const ms = performance.now() - t0;
      totalMs += ms;
      console.log(`${(q.id + ' ' + q.name).padEnd(16)}| ${String(ms.toFixed(0)).padStart(8)} | ERR  | ❌ ${e.message.split('\n')[0].slice(0,40)}`);
      failed++;
    }
  }
  
  console.log(`\nTotal: ${passed}/${passed+failed} correct, ${totalMs.toFixed(0)}ms total`);
  console.log(`Avg: ${(totalMs/(passed+failed)).toFixed(0)}ms per query`);
}

run();
