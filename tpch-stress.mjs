import { Database } from './src/db.js';

async function run() {
  const db = new Database();
  
  // Create TPC-H-like schema
  const setup = [
    `CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name TEXT, n_regionkey INT)`,
    `CREATE TABLE region (r_regionkey INT PRIMARY KEY, r_name TEXT)`,
    `CREATE TABLE customer (c_custkey INT PRIMARY KEY, c_name TEXT, c_nationkey INT, c_acctbal REAL)`,
    `CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT, o_orderstatus TEXT, o_totalprice REAL, o_orderdate TEXT, o_orderpriority TEXT)`,
    `CREATE TABLE lineitem (l_orderkey INT, l_partkey INT, l_suppkey INT, l_linenumber INT, l_quantity REAL, l_extendedprice REAL, l_discount REAL, l_tax REAL, l_returnflag TEXT, l_linestatus TEXT, l_shipdate TEXT, l_commitdate TEXT, l_receiptdate TEXT, l_shipinstruct TEXT, l_shipmode TEXT)`,
    `CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_name TEXT, s_nationkey INT, s_acctbal REAL)`,
    `CREATE TABLE part (p_partkey INT PRIMARY KEY, p_name TEXT, p_brand TEXT, p_type TEXT, p_size INT, p_retailprice REAL)`,
    `CREATE TABLE partsupp (ps_partkey INT, ps_suppkey INT, ps_availqty INT, ps_supplycost REAL)`
  ];
  
  for (const sql of setup) db.execute(sql);
  
  // Insert data
  const regions = ['AFRICA','AMERICA','ASIA','EUROPE','MIDDLE EAST'];
  for (let i = 0; i < 5; i++) db.execute(`INSERT INTO region VALUES (${i}, '${regions[i]}')`);
  
  const nations = ['ALGERIA','ARGENTINA','BRAZIL','CANADA','EGYPT','ETHIOPIA','FRANCE','GERMANY','INDIA','INDONESIA','IRAN','IRAQ','JAPAN','JORDAN','KENYA','MOROCCO','MOZAMBIQUE','PERU','CHINA','ROMANIA','SAUDI ARABIA','VIETNAM','RUSSIA','UK','USA'];
  for (let i = 0; i < 25; i++) db.execute(`INSERT INTO nation VALUES (${i}, '${nations[i]}', ${i % 5})`);
  
  // 200 customers
  for (let i = 1; i <= 200; i++) {
    db.execute(`INSERT INTO customer VALUES (${i}, 'Customer#${String(i).padStart(6,'0')}', ${i % 25}, ${(Math.random() * 10000 - 1000).toFixed(2)})`);
  }
  
  // 500 orders
  const statuses = ['F','O','P'];
  const priorities = ['1-URGENT','2-HIGH','3-MEDIUM','4-NOT SPECIFIED','5-LOW'];
  for (let i = 1; i <= 500; i++) {
    const y = 2024 + (i % 3), m = String(1 + i % 12).padStart(2,'0'), d = String(1 + i % 28).padStart(2,'0');
    db.execute(`INSERT INTO orders VALUES (${i}, ${1 + i % 200}, '${statuses[i%3]}', ${(Math.random()*100000).toFixed(2)}, '${y}-${m}-${d}', '${priorities[i%5]}')`);
  }
  
  // 2000 lineitems
  const flags = ['A','N','R'];
  const lstatus = ['F','O'];
  const modes = ['AIR','FOB','MAIL','RAIL','REG AIR','SHIP','TRUCK'];
  for (let i = 0; i < 2000; i++) {
    const ok = 1 + i % 500, pk = 1 + i % 100, sk = 1 + i % 50;
    const qty = 1 + i % 50, price = (qty * (10 + Math.random()*90)).toFixed(2);
    const disc = (Math.random()*0.1).toFixed(2), tax = (Math.random()*0.08).toFixed(2);
    const y = 2024 + (i % 3), m = String(1 + i % 12).padStart(2,'0'), d = String(1 + i % 28).padStart(2,'0');
    db.execute(`INSERT INTO lineitem VALUES (${ok}, ${pk}, ${sk}, ${i % 7 + 1}, ${qty}, ${price}, ${disc}, ${tax}, '${flags[i%3]}', '${lstatus[i%2]}', '${y}-${m}-${d}', '${y}-${m}-${d}', '${y}-${m}-${d}', 'DELIVER IN PERSON', '${modes[i%7]}')`);
  }
  
  // 100 suppliers
  for (let i = 1; i <= 100; i++) {
    db.execute(`INSERT INTO supplier VALUES (${i}, 'Supplier#${String(i).padStart(6,'0')}', ${i % 25}, ${(Math.random()*10000).toFixed(2)})`);
  }
  
  // 200 parts
  for (let i = 1; i <= 200; i++) {
    db.execute(`INSERT INTO part VALUES (${i}, 'Part${i}', 'Brand#${1+i%5}${1+i%5}', 'ECONOMY POLISHED STEEL', ${1+i%50}, ${(Math.random()*2000).toFixed(2)})`);
  }
  
  // 400 partsupp
  for (let i = 0; i < 400; i++) {
    db.execute(`INSERT INTO partsupp VALUES (${1+i%200}, ${1+i%100}, ${100+i*10}, ${(Math.random()*1000).toFixed(2)})`);
  }
  
  console.log('Data loaded. Running TPC-H queries...\n');
  
  const queries = [
    { name: 'Q1 - Pricing Summary', sql: `SELECT l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price, SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price, AVG(l_quantity) as avg_qty, AVG(l_discount) as avg_disc, COUNT(*) as count_order FROM lineitem WHERE l_shipdate <= '2025-12-01' GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus` },
    
    { name: 'Q3 - Shipping Priority', sql: `SELECT l.l_orderkey, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue, o.o_orderdate, o.o_orderpriority FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey WHERE o.o_orderdate < '2025-03-15' AND l.l_shipdate > '2025-03-15' GROUP BY l.l_orderkey, o.o_orderdate, o.o_orderpriority ORDER BY revenue DESC LIMIT 10` },
    
    { name: 'Q4 - Order Priority Check', sql: `SELECT o.o_orderpriority, COUNT(*) as order_count FROM orders o WHERE o.o_orderdate >= '2025-01-01' AND o.o_orderdate < '2025-04-01' AND EXISTS (SELECT 1 FROM lineitem l WHERE l.l_orderkey = o.o_orderkey AND l.l_commitdate < l.l_receiptdate) GROUP BY o.o_orderpriority ORDER BY o.o_orderpriority` },
    
    { name: 'Q5 - Local Supplier Volume', sql: `SELECT n.n_name, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey JOIN supplier s ON l.l_suppkey = s.s_suppkey JOIN nation n ON c.c_nationkey = n.n_nationkey AND s.s_nationkey = n.n_nationkey WHERE n.n_regionkey = 1 AND o.o_orderdate >= '2025-01-01' AND o.o_orderdate < '2026-01-01' GROUP BY n.n_name ORDER BY revenue DESC` },
    
    { name: 'Q6 - Forecasting Revenue', sql: `SELECT SUM(l_extendedprice * l_discount) as revenue FROM lineitem WHERE l_shipdate >= '2025-01-01' AND l_shipdate < '2026-01-01' AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24` },
    
    { name: 'Q10 - Returned Item Report', sql: `SELECT c.c_custkey, c.c_name, SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue, c.c_acctbal, n.n_name FROM customer c JOIN orders o ON c.c_custkey = o.o_custkey JOIN lineitem l ON l.l_orderkey = o.o_orderkey JOIN nation n ON c.c_nationkey = n.n_nationkey WHERE o.o_orderdate >= '2025-01-01' AND o.o_orderdate < '2025-04-01' AND l.l_returnflag = 'R' GROUP BY c.c_custkey, c.c_name, c.c_acctbal, n.n_name ORDER BY revenue DESC LIMIT 20` },
    
    { name: 'Q12 - Shipping Modes', sql: `SELECT l.l_shipmode, SUM(CASE WHEN o.o_orderpriority = '1-URGENT' OR o.o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) as high_line_count, SUM(CASE WHEN o.o_orderpriority <> '1-URGENT' AND o.o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) as low_line_count FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey WHERE l.l_shipmode IN ('MAIL', 'SHIP') AND l.l_commitdate < l.l_receiptdate AND l.l_shipdate < l.l_commitdate AND l.l_receiptdate >= '2025-01-01' AND l.l_receiptdate < '2026-01-01' GROUP BY l.l_shipmode ORDER BY l.l_shipmode` },
    
    { name: 'Q13 - Customer Distribution', sql: `SELECT c_count, COUNT(*) as custdist FROM (SELECT c.c_custkey, COUNT(o.o_orderkey) as c_count FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey GROUP BY c.c_custkey) as c_orders GROUP BY c_count ORDER BY custdist DESC, c_count DESC` },
    
    { name: 'Q14 - Promotion Effect', sql: `SELECT 100.0 * SUM(CASE WHEN p.p_type LIKE 'PROMO%' THEN l.l_extendedprice * (1 - l.l_discount) ELSE 0 END) / SUM(l.l_extendedprice * (1 - l.l_discount)) as promo_revenue FROM lineitem l JOIN part p ON l.l_partkey = p.p_partkey WHERE l.l_shipdate >= '2025-03-01' AND l.l_shipdate < '2025-04-01'` },
    
    { name: 'Q16 - Parts/Supplier', sql: `SELECT p.p_brand, p.p_type, p.p_size, COUNT(DISTINCT ps.ps_suppkey) as supplier_cnt FROM partsupp ps JOIN part p ON p.p_partkey = ps.ps_partkey WHERE p.p_brand <> 'Brand#45' AND p.p_size IN (49, 14, 23, 45, 19, 3, 36, 9) GROUP BY p.p_brand, p.p_type, p.p_size ORDER BY supplier_cnt DESC, p.p_brand, p.p_type, p.p_size` },
    
    { name: 'Q19 - Discounted Revenue', sql: `SELECT SUM(l.l_extendedprice * (1 - l.l_discount)) as revenue FROM lineitem l JOIN part p ON p.p_partkey = l.l_partkey WHERE (p.p_brand = 'Brand#12' AND l.l_quantity >= 1 AND l.l_quantity <= 11 AND p.p_size >= 1 AND p.p_size <= 5) OR (p.p_brand = 'Brand#23' AND l.l_quantity >= 10 AND l.l_quantity <= 20 AND p.p_size >= 1 AND p.p_size <= 10) OR (p.p_brand = 'Brand#34' AND l.l_quantity >= 20 AND l.l_quantity <= 30 AND p.p_size >= 1 AND p.p_size <= 15)` },
    
    // Window functions
    { name: 'Window - Running Total', sql: `SELECT o_orderkey, o_totalprice, SUM(o_totalprice) OVER (ORDER BY o_orderkey) as running_total FROM orders LIMIT 20` },
    
    { name: 'Window - Rank per Customer', sql: `SELECT o_custkey, o_orderkey, o_totalprice, RANK() OVER (PARTITION BY o_custkey ORDER BY o_totalprice DESC) as price_rank FROM orders WHERE o_custkey <= 10 ORDER BY o_custkey, price_rank` },
    
    { name: 'Window - PERCENT_RANK', sql: `SELECT c_custkey, c_acctbal, PERCENT_RANK() OVER (ORDER BY c_acctbal) as pct_rank FROM customer WHERE c_nationkey = 0 ORDER BY c_acctbal` },
    
    // Recursive CTE
    { name: 'Recursive CTE - Hierarchy', sql: `WITH RECURSIVE region_chain(rkey, rname, depth) AS (SELECT r_regionkey, r_name, 1 FROM region WHERE r_regionkey = 0 UNION ALL SELECT r.r_regionkey, r.r_name, rc.depth + 1 FROM region r JOIN region_chain rc ON r.r_regionkey = rc.rkey + 1 WHERE rc.depth < 5) SELECT * FROM region_chain` },
    
    // Correlated subqueries
    { name: 'Correlated Subquery - Above Avg', sql: `SELECT c.c_name, c.c_acctbal FROM customer c WHERE c.c_acctbal > (SELECT AVG(c2.c_acctbal) FROM customer c2 WHERE c2.c_nationkey = c.c_nationkey) ORDER BY c.c_acctbal DESC LIMIT 10` },
    
    { name: 'NOT EXISTS', sql: `SELECT c.c_custkey, c.c_name FROM customer c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.o_custkey = c.c_custkey) LIMIT 10` },
    
    // ROLLUP
    { name: 'ROLLUP Aggregation', sql: `SELECT n.n_name, o.o_orderstatus, COUNT(*) as cnt, SUM(o.o_totalprice) as total FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey JOIN nation n ON c.c_nationkey = n.n_nationkey WHERE n.n_regionkey = 0 GROUP BY ROLLUP(n.n_name, o.o_orderstatus) ORDER BY n.n_name, o.o_orderstatus` },
    
    // Statistical aggregates
    { name: 'STDDEV/VARIANCE', sql: `SELECT n.n_name, STDDEV(c.c_acctbal) as std_bal, VARIANCE(c.c_acctbal) as var_bal FROM customer c JOIN nation n ON c.c_nationkey = n.n_nationkey GROUP BY n.n_name ORDER BY std_bal DESC LIMIT 5` },
    
    // FILTER clause
    { name: 'FILTER on Aggregate', sql: `SELECT o_orderstatus, COUNT(*) as total, COUNT(*) FILTER (WHERE o_totalprice > 50000) as high_value, AVG(o_totalprice) FILTER (WHERE o_orderpriority = '1-URGENT') as avg_urgent FROM orders GROUP BY o_orderstatus ORDER BY o_orderstatus` },
    
    // Array functions
    { name: 'ARRAY Functions', sql: `SELECT ARRAY[1, 2, 3, 4, 5] as arr, ARRAY_LENGTH(ARRAY[1,2,3]) as len, ARRAY_APPEND(ARRAY[1,2], 3) as appended` },
    
    // Date functions
    { name: 'Date Functions', sql: `SELECT EXTRACT(YEAR FROM '2025-06-15') as yr, EXTRACT(MONTH FROM '2025-06-15') as mo, DATE_TRUNC('month', '2025-06-15') as truncd` },
    
    // Math functions
    { name: 'Math Functions', sql: `SELECT MOD(17, 5) as m, SIGN(-42) as s, TRUNC(3.7) as t, PI() as pi, EXP(1) as e, LN(EXP(1)) as one, CBRT(27) as three` },
    
    // Complex CASE + GROUP BY expression
    { name: 'Complex CASE + Agg', sql: `SELECT CASE WHEN c_acctbal < 0 THEN 'negative' WHEN c_acctbal < 1000 THEN 'low' WHEN c_acctbal < 5000 THEN 'medium' ELSE 'high' END as tier, COUNT(*) as cnt, AVG(c_acctbal) as avg_bal FROM customer GROUP BY CASE WHEN c_acctbal < 0 THEN 'negative' WHEN c_acctbal < 1000 THEN 'low' WHEN c_acctbal < 5000 THEN 'medium' ELSE 'high' END ORDER BY avg_bal` },
    
    // Prepared statements
    { name: 'Prepared Statement', sql: `PREPARE q1 AS SELECT c_name FROM customer WHERE c_custkey = $1` },
    { name: 'Execute Prepared', sql: `EXECUTE q1(42)` },
    
    // MERGE
    { name: 'MERGE Setup', sql: `CREATE TABLE inv (item_id INT PRIMARY KEY, qty INT)` },
    { name: 'MERGE Insert', sql: `INSERT INTO inv VALUES (1, 100), (2, 200), (3, 300)` },
    { name: 'MERGE Execute', sql: `MERGE INTO inv t USING (SELECT 2 as item_id, 50 as qty UNION ALL SELECT 4, 400) AS s ON t.item_id = s.item_id WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty WHEN NOT MATCHED THEN INSERT (item_id, qty) VALUES (s.item_id, s.qty)` },
    { name: 'MERGE Verify', sql: `SELECT * FROM inv ORDER BY item_id` },
    
    // COPY TO
    { name: 'COPY TO CSV', sql: `COPY (SELECT c_custkey, c_name FROM customer LIMIT 5) TO STDOUT WITH (FORMAT CSV, HEADER true)` },
    
    // SHOW commands
    { name: 'SHOW TABLES', sql: `SHOW TABLES` },
    { name: 'SHOW COLUMNS', sql: `SHOW COLUMNS FROM customer` },
  ];
  
  let passed = 0, failed = 0, errors = [];
  let totalMs = 0;
  
  for (const q of queries) {
    const t0 = Date.now();
    try {
      const result = db.execute(q.sql);
      const ms = Date.now() - t0;
      totalMs += ms;
      const rows = Array.isArray(result) ? result.length : (result?.rowCount ?? (typeof result === 'string' ? 1 : 0));
      console.log(`✅ ${q.name}: ${rows} rows, ${ms}ms`);
      passed++;
    } catch (e) {
      const ms = Date.now() - t0;
      totalMs += ms;
      console.log(`❌ ${q.name}: ${e.message.split('\n')[0]} (${ms}ms)`);
      errors.push({ name: q.name, error: e.message.split('\n')[0] });
      failed++;
    }
  }
  
  console.log(`\n--- Results: ${passed}/${passed+failed} passed, ${failed} failed, total ${totalMs}ms ---`);
  if (errors.length) {
    console.log('\nFailures:');
    errors.forEach(e => console.log(`  ${e.name}: ${e.error}`));
  }
}

run().catch(e => console.error(e));
