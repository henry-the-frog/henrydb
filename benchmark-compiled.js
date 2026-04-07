// benchmark-compiled.js — Compare interpreted vs compiled query execution
import { Database } from './src/db.js';
import { compileScanFilterProject, compileHashJoin } from './src/query-compiler.js';
import { parse } from './src/sql.js';

// Setup: create a database with realistic data
const db = new Database();
db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, amount INT, status TEXT, region TEXT)');
db.execute('CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, city TEXT, tier TEXT)');

// Insert 10K orders
console.log('Inserting 10K orders...');
for (let i = 0; i < 10000; i++) {
  const status = ['pending', 'shipped', 'delivered', 'cancelled'][i % 4];
  const region = ['US', 'EU', 'APAC', 'LATAM'][i % 4];
  db.execute(`INSERT INTO orders VALUES (${i}, ${i % 1000}, ${(i * 17) % 1000}, '${status}', '${region}')`);
}

// Insert 1K customers
console.log('Inserting 1K customers...');
for (let i = 0; i < 1000; i++) {
  const city = ['NYC', 'LA', 'London', 'Tokyo', 'Sydney'][i % 5];
  const tier = ['gold', 'silver', 'bronze'][i % 3];
  db.execute(`INSERT INTO customers VALUES (${i}, 'customer_${i}', '${city}', '${tier}')`);
}

console.log('\n=== Benchmark: Interpreted vs Compiled ===\n');

// --- Benchmark 1: Simple filter ---
{
  console.log('1. Simple filter: SELECT * FROM orders WHERE amount > 500 AND status = \'shipped\'');
  
  const runs = 5;
  
  // Interpreted
  const startI = performance.now();
  for (let j = 0; j < runs; j++) {
    db.execute("SELECT * FROM orders WHERE amount > 500 AND status = 'shipped'");
  }
  const timeI = (performance.now() - startI) / runs;
  
  // Compiled
  const ast = parse("SELECT * FROM orders WHERE amount > 500 AND status = 'shipped'");
  const schema = [{ name: 'id' }, { name: 'customer_id' }, { name: 'amount' }, { name: 'status' }, { name: 'region' }];
  const compiled = compileScanFilterProject(ast.where, ast.columns, schema);
  const heap = [...db.tables.get('orders').heap.scan()];
  
  const startC = performance.now();
  for (let j = 0; j < runs; j++) {
    compiled(heap);
  }
  const timeC = (performance.now() - startC) / runs;
  
  const interpResults = db.execute("SELECT COUNT(*) as cnt FROM orders WHERE amount > 500 AND status = 'shipped'");
  const compiledResults = compiled(heap);
  
  console.log(`   Interpreted: ${timeI.toFixed(1)}ms, Compiled: ${timeC.toFixed(1)}ms`);
  console.log(`   Speedup: ${(timeI / timeC).toFixed(1)}x`);
  console.log(`   Results: interpreted=${interpResults.rows[0].cnt}, compiled=${compiledResults.length}`);
  console.log();
}

// --- Benchmark 2: Complex filter ---
{
  console.log('2. Complex filter: amount > 200 AND amount < 800 AND region IN (\'US\', \'EU\') AND status != \'cancelled\'');
  
  const runs = 5;
  const sql = "SELECT id, amount, region FROM orders WHERE amount > 200 AND amount < 800 AND region IN ('US', 'EU') AND status != 'cancelled'";
  
  const startI = performance.now();
  for (let j = 0; j < runs; j++) db.execute(sql);
  const timeI = (performance.now() - startI) / runs;
  
  const ast = parse(sql);
  const schema = [{ name: 'id' }, { name: 'customer_id' }, { name: 'amount' }, { name: 'status' }, { name: 'region' }];
  const compiled = compileScanFilterProject(ast.where, ast.columns, schema);
  const heap = [...db.tables.get('orders').heap.scan()];
  
  const startC = performance.now();
  for (let j = 0; j < runs; j++) compiled(heap);
  const timeC = (performance.now() - startC) / runs;
  
  console.log(`   Interpreted: ${timeI.toFixed(1)}ms, Compiled: ${timeC.toFixed(1)}ms`);
  console.log(`   Speedup: ${(timeI / timeC).toFixed(1)}x`);
  console.log();
}

// --- Benchmark 3: Small join ---
{
  console.log('3. Join baseline (interpreted): 1K orders × 100 customers');
  
  // Create smaller tables for join benchmark
  db.execute('CREATE TABLE small_orders (id INT PRIMARY KEY, cid INT, amount INT)');
  db.execute('CREATE TABLE small_customers (id INT PRIMARY KEY, name TEXT)');
  for (let i = 0; i < 1000; i++) db.execute(`INSERT INTO small_orders VALUES (${i}, ${i % 100}, ${(i*17)%1000})`);
  for (let i = 0; i < 100; i++) db.execute(`INSERT INTO small_customers VALUES (${i}, 'cust_${i}')`);
  
  const runs = 10;
  const startI = performance.now();
  for (let j = 0; j < runs; j++) {
    db.execute('SELECT o.amount, c.name FROM small_orders o JOIN small_customers c ON c.id = o.cid WHERE o.amount > 500');
  }
  const timeI = (performance.now() - startI) / runs;
  
  console.log(`   Interpreted join: ${timeI.toFixed(1)}ms`);
  console.log();
}

// --- Benchmark 4: Selectivity estimation ---
{
  console.log('4. Highly selective: WHERE id = 42 (point query on 10K rows)');
  
  const runs = 20;
  
  const startI = performance.now();
  for (let j = 0; j < runs; j++) {
    db.execute('SELECT * FROM orders WHERE id = 42');
  }
  const timeI = (performance.now() - startI) / runs;
  
  const ast = parse('SELECT * FROM orders WHERE id = 42');
  const schema = [{ name: 'id' }, { name: 'customer_id' }, { name: 'amount' }, { name: 'status' }, { name: 'region' }];
  const compiled = compileScanFilterProject(ast.where, ast.columns, schema);
  const heap = [...db.tables.get('orders').heap.scan()];
  
  const startC = performance.now();
  for (let j = 0; j < runs; j++) compiled(heap);
  const timeC = (performance.now() - startC) / runs;
  
  console.log(`   Interpreted: ${timeI.toFixed(1)}ms, Compiled: ${timeC.toFixed(1)}ms`);
  console.log(`   Speedup: ${(timeI / timeC).toFixed(1)}x`);
}

console.log('\nDone!');
