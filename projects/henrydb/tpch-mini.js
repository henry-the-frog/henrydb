#!/usr/bin/env node
// tpch-mini.js — TPC-H-inspired analytical queries for HenryDB
import { Database } from './src/db.js';

const db = new Database();
const N = 1000; // scale factor

console.log(`🏆 TPC-H Mini Benchmark (SF=${N} rows)\n`);

// Schema
db.execute('CREATE TABLE nations (id INT PRIMARY KEY, name TEXT, region TEXT)');
db.execute('CREATE TABLE suppliers (id INT PRIMARY KEY, name TEXT, nation_id INT, acctbal INT)');
db.execute('CREATE TABLE parts (id INT PRIMARY KEY, name TEXT, brand TEXT, size INT, price INT)');
db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, status TEXT, total INT, order_date TEXT, priority TEXT)');
db.execute('CREATE TABLE lineitem (id INT PRIMARY KEY, order_id INT, part_id INT, supplier_id INT, qty INT, price INT, discount INT, tax INT)');

// Data generation
const regions = ['America', 'Europe', 'Asia', 'Africa', 'Oceania'];
const nations = ['USA', 'Canada', 'UK', 'Germany', 'France', 'China', 'Japan', 'India', 'Brazil', 'Australia'];
for (let i = 0; i < 10; i++) {
  db.execute(`INSERT INTO nations VALUES (${i}, '${nations[i]}', '${regions[i % 5]}')`);
}

for (let i = 1; i <= 50; i++) {
  db.execute(`INSERT INTO suppliers VALUES (${i}, 'Supplier ${i}', ${i % 10}, ${1000 + i * 73 % 10000})`);
}

const brands = ['Brand#1', 'Brand#2', 'Brand#3', 'Brand#4', 'Brand#5'];
for (let i = 1; i <= 200; i++) {
  db.execute(`INSERT INTO parts VALUES (${i}, 'Part ${i}', '${brands[i % 5]}', ${1 + i % 50}, ${100 + i * 13 % 900})`);
}

const statuses = ['F', 'O', 'P'];
const priorities = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'];
for (let i = 1; i <= N; i++) {
  const month = String(1 + (i % 12)).padStart(2, '0');
  const day = String(1 + (i % 28)).padStart(2, '0');
  const year = 2020 + (i % 4);
  db.execute(`INSERT INTO orders VALUES (${i}, ${1 + i % 100}, '${statuses[i % 3]}', ${100 + i * 7 % 5000}, '${year}-${month}-${day}', '${priorities[i % 5]}')`);
}

for (let i = 1; i <= N * 3; i++) {
  db.execute(`INSERT INTO lineitem VALUES (${i}, ${1 + (i % N)}, ${1 + (i % 200)}, ${1 + (i % 50)}, ${1 + i % 10}, ${100 + i * 11 % 900}, ${i % 10}, ${i % 8})`);
}

console.log(`Data: ${10} nations, ${50} suppliers, ${200} parts, ${N} orders, ${N * 3} lineitems\n`);

// Benchmark queries
function bench(name, sql) {
  const start = performance.now();
  const r = db.execute(sql);
  const ms = performance.now() - start;
  console.log(`Q: ${name}`);
  console.log(`   ${r.rows.length} rows, ${ms.toFixed(1)}ms`);
  if (r.rows.length > 0) console.log(`   Sample: ${JSON.stringify(r.rows[0])}`);
  console.log('');
  return ms;
}

let total = 0;

// Q1: Pricing summary (TPC-H Q1)
total += bench('Pricing Summary', `
  SELECT status, SUM(qty) as sum_qty, SUM(price) as sum_price, AVG(price) as avg_price, COUNT(*) as count_order
  FROM lineitem l JOIN orders o ON l.order_id = o.id
  GROUP BY status
  ORDER BY status
`);

// Q2: Minimum cost supplier
total += bench('Min Cost Supplier', `
  SELECT s.name, s.acctbal, n.name as nation, p.name as part
  FROM suppliers s
  JOIN nations n ON s.nation_id = n.id
  JOIN parts p ON p.id <= 10
  WHERE s.acctbal = (SELECT MIN(acctbal) FROM suppliers)
  ORDER BY s.acctbal
  LIMIT 5
`);

// Q3: Top revenue orders
total += bench('Top Revenue Orders', `
  SELECT o.id, o.total, o.order_date
  FROM orders o
  WHERE o.status = 'F' AND o.order_date > '2021-01-01'
  ORDER BY o.total DESC
  LIMIT 10
`);

// Q4: Order priority count
total += bench('Order Priority Count', `
  SELECT priority, COUNT(*) as order_count
  FROM orders
  WHERE order_date >= '2021-01-01' AND order_date < '2022-01-01'
  GROUP BY priority
  ORDER BY priority
`);

// Q5: Revenue by nation
total += bench('Revenue by Nation', `
  SELECT n.name, SUM(l.price * l.qty) as revenue
  FROM lineitem l
  JOIN orders o ON l.order_id = o.id
  JOIN suppliers s ON l.supplier_id = s.id
  JOIN nations n ON s.nation_id = n.id
  GROUP BY n.name
  ORDER BY revenue DESC
`);

// Q6: Forecasting revenue change
total += bench('Revenue Forecast', `
  SELECT SUM(price * qty) as revenue
  FROM lineitem
  WHERE discount BETWEEN 5 AND 7
    AND qty < 5
`);

// Q8: Market share with CTE
total += bench('Market Share (CTE)', `
  WITH regional_revenue AS (
    SELECT n.region, SUM(l.price * l.qty) as revenue
    FROM lineitem l
    JOIN suppliers s ON l.supplier_id = s.id
    JOIN nations n ON s.nation_id = n.id
    GROUP BY n.region
  )
  SELECT region, revenue
  FROM regional_revenue
  ORDER BY revenue DESC
`);

// Q10: Top customers with window
total += bench('Customer Ranking (Window)', `
  WITH customer_spend AS (
    SELECT customer_id, SUM(total) as spend
    FROM orders WHERE status = 'F'
    GROUP BY customer_id
  )
  SELECT customer_id, spend,
    RANK() OVER (ORDER BY spend DESC) as rank
  FROM customer_spend
  ORDER BY spend DESC
  LIMIT 10
`);

console.log(`Total benchmark time: ${total.toFixed(0)}ms`);
console.log(`Average per query: ${(total / 8).toFixed(0)}ms`);
