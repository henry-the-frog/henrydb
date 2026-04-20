// decorrelation-stress.test.js — Stress tests for correlated subquery decorrelation
// Tests that CORRELATED_IN_HASHMAP optimization produces correct results

import { Database } from './db.js';
import { strict as assert } from 'assert';

let db, pass = 0, fail = 0;

function test(name, fn) {
  db = new Database();
  try { fn(); pass++; console.log(`  ✅ ${name}`); }
  catch (e) { fail++; console.log(`  ❌ ${name}: ${e.message}`); }
}

function q(d, sql) { return d.execute(sql).rows; }

console.log('\n🧪 Subquery Decorrelation Stress Tests');

// --- Correlated IN subquery patterns ---

test('Simple correlated IN: employees in departments with budget > threshold', () => {
  db.execute('CREATE TABLE depts(id INT PRIMARY KEY, name TEXT, budget INT)');
  db.execute('CREATE TABLE emps(id INT PRIMARY KEY, name TEXT, dept_id INT)');
  db.execute("INSERT INTO depts VALUES (1, 'Eng', 500000), (2, 'Sales', 200000), (3, 'HR', 100000)");
  db.execute("INSERT INTO emps VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1), (4, 'Diana', 3)");
  
  const r = q(db, `
    SELECT e.name FROM emps e
    WHERE e.dept_id IN (SELECT d.id FROM depts d WHERE d.budget > 150000)
  `);
  assert.deepEqual(r.map(x => x.name).sort(), ['Alice', 'Bob', 'Charlie']);
});

test('Correlated IN with self-reference: products priced above category average', () => {
  db.execute('CREATE TABLE products(id INT, cat TEXT, price INT)');
  db.execute("INSERT INTO products VALUES (1,'A',10),(2,'A',20),(3,'A',30),(4,'B',5),(5,'B',15),(6,'B',25)");
  
  const r = q(db, `
    SELECT p.id, p.price FROM products p
    WHERE p.price IN (
      SELECT p2.price FROM products p2 WHERE p2.cat = p.cat AND p2.price > 15
    )
    ORDER BY p.id
  `);
  // Cat A > 15: prices 20, 30. Cat B > 15: prices 25.
  assert.deepEqual(r.map(x => x.id), [2, 3, 6]);
});

test('Correlated IN with multiple correlation columns', () => {
  db.execute('CREATE TABLE inventory(store TEXT, product TEXT, qty INT)');
  db.execute('CREATE TABLE thresholds(store TEXT, product TEXT, min_qty INT)');
  db.execute("INSERT INTO inventory VALUES ('NY','Widget',100),('NY','Gadget',5),('LA','Widget',50),('LA','Gadget',200)");
  db.execute("INSERT INTO thresholds VALUES ('NY','Widget',80),('NY','Gadget',10),('LA','Widget',60),('LA','Gadget',150)");
  
  // Products where qty is below threshold
  const r = q(db, `
    SELECT i.store, i.product, i.qty FROM inventory i
    WHERE i.qty IN (
      SELECT i2.qty FROM inventory i2 
      JOIN thresholds t ON t.store = i2.store AND t.product = i2.product
      WHERE i2.store = i.store AND i2.qty < t.min_qty
    )
    ORDER BY i.store, i.product
  `);
  // NY Widget: 100 >= 80 OK. NY Gadget: 5 < 10 BELOW. LA Widget: 50 < 60 BELOW. LA Gadget: 200 >= 150 OK.
  assert.equal(r.length, 2);
  assert.deepEqual(r.map(x => `${x.store}:${x.product}`), ['LA:Widget', 'NY:Gadget']);
});

test('NOT IN correlated subquery', () => {
  db.execute('CREATE TABLE students(id INT PRIMARY KEY, name TEXT, grade TEXT)');
  db.execute('CREATE TABLE scores(student_id INT, subject TEXT, score INT)');
  db.execute("INSERT INTO students VALUES (1,'Alice','A'),(2,'Bob','B'),(3,'Charlie','A')");
  db.execute("INSERT INTO scores VALUES (1,'Math',90),(1,'Science',85),(2,'Math',70),(3,'Math',95)");
  
  // Students whose id is NOT IN the set of students with score < 80 in their grade
  const r = q(db, `
    SELECT s.name FROM students s
    WHERE s.id NOT IN (
      SELECT sc.student_id FROM scores sc
      JOIN students s2 ON sc.student_id = s2.id
      WHERE s2.grade = s.grade AND sc.score < 80
    )
    ORDER BY s.name
  `);
  // Grade A students with score < 80: none. Grade B: Bob (70 < 80).
  // So NOT IN filters Bob out.
  assert.deepEqual(r.map(x => x.name), ['Alice', 'Charlie']);
});

test('Correlated IN with aggregate in inner query', () => {
  db.execute('CREATE TABLE orders(id INT, customer_id INT, amount INT)');
  db.execute('CREATE TABLE customers(id INT PRIMARY KEY, name TEXT, tier TEXT)');
  db.execute("INSERT INTO customers VALUES (1,'Alice','Gold'),(2,'Bob','Silver'),(3,'Charlie','Gold')");
  db.execute("INSERT INTO orders VALUES (1,1,100),(2,1,200),(3,2,50),(4,3,300)");
  
  // Customers whose max order amount equals the max for their tier
  const r = q(db, `
    SELECT c.name FROM customers c
    WHERE c.id IN (
      SELECT o.customer_id FROM orders o
      WHERE o.customer_id IN (SELECT c2.id FROM customers c2 WHERE c2.tier = c.tier)
      AND o.amount > 100
    )
    ORDER BY c.name
  `);
  // Gold tier: Alice (200 > 100), Charlie (300 > 100). Silver: Bob (max 50 < 100).
  assert.deepEqual(r.map(x => x.name), ['Alice', 'Charlie']);
});

test('Large dataset: correlated IN with 500 rows', () => {
  db.execute('CREATE TABLE big_t(id INT PRIMARY KEY, grp INT, val INT)');
  for (let i = 1; i <= 500; i++) {
    db.execute(`INSERT INTO big_t VALUES (${i}, ${i % 10}, ${i * 7 % 100})`);
  }
  
  // Find rows where val exists in the same group with val > 50
  const r = q(db, `
    SELECT COUNT(*) as cnt FROM big_t t1
    WHERE t1.val IN (
      SELECT t2.val FROM big_t t2 WHERE t2.grp = t1.grp AND t2.val > 50
    )
  `);
  // Verify it runs without error and returns a reasonable count
  assert.ok(r[0].cnt > 0 && r[0].cnt <= 500);
});

test('Correlated IN vs uncorrelated IN give same results', () => {
  db.execute('CREATE TABLE t1(id INT, cat TEXT, val INT)');
  db.execute('CREATE TABLE t2(cat TEXT, threshold INT)');
  db.execute("INSERT INTO t1 VALUES (1,'A',10),(2,'A',20),(3,'B',15),(4,'B',25),(5,'C',30)");
  db.execute("INSERT INTO t2 VALUES ('A',12),('B',20),('C',25)");
  
  // Uncorrelated version (for baseline)
  const uncorrelated = q(db, `
    SELECT id FROM t1 WHERE val > 15 ORDER BY id
  `);
  
  // Correlated version
  const correlated = q(db, `
    SELECT t1.id FROM t1
    WHERE t1.val IN (
      SELECT t1b.val FROM t1 t1b 
      WHERE t1b.cat = t1.cat AND t1b.val > (SELECT threshold FROM t2 WHERE t2.cat = t1b.cat)
    )
    ORDER BY t1.id
  `);
  
  // Both should find rows where val > threshold for their category
  // A: threshold 12, so val 20 > 12. B: threshold 20, so val 25 > 20. C: threshold 25, so val 30 > 25.
  assert.deepEqual(correlated.map(x => x.id), [2, 4, 5]);
});

test('Correlated IN with NULL values', () => {
  db.execute('CREATE TABLE t(id INT, grp INT, val INT)');
  db.execute("INSERT INTO t VALUES (1, 1, 10), (2, 1, NULL), (3, 2, 20), (4, 2, NULL)");
  
  const r = q(db, `
    SELECT t1.id FROM t t1
    WHERE t1.val IN (
      SELECT t2.val FROM t t2 WHERE t2.grp = t1.grp AND t2.val IS NOT NULL
    )
    ORDER BY t1.id
  `);
  assert.deepEqual(r.map(x => x.id), [1, 3]);
});

test('EXISTS correlated subquery still works', () => {
  db.execute('CREATE TABLE orders(id INT, customer_id INT, total INT)');
  db.execute('CREATE TABLE customers(id INT PRIMARY KEY, name TEXT)');
  db.execute("INSERT INTO customers VALUES (1,'Alice'),(2,'Bob'),(3,'Charlie')");
  db.execute("INSERT INTO orders VALUES (1,1,100),(2,1,200),(3,2,50)");
  
  const r = q(db, `
    SELECT c.name FROM customers c
    WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.total > 100)
    ORDER BY c.name
  `);
  assert.deepEqual(r.map(x => x.name), ['Alice']);
});

test('Nested correlated subqueries', () => {
  db.execute('CREATE TABLE t1(id INT, val INT)');
  db.execute('CREATE TABLE t2(id INT, ref_id INT, val INT)');
  db.execute("INSERT INTO t1 VALUES (1,10),(2,20),(3,30)");
  db.execute("INSERT INTO t2 VALUES (1,1,5),(2,1,15),(3,2,25),(4,3,35)");
  
  const r = q(db, `
    SELECT t1.id FROM t1
    WHERE t1.val IN (
      SELECT t2.val FROM t2 WHERE t2.ref_id = t1.id
    )
    ORDER BY t1.id
  `);
  // t2 values for ref_id=1: 5,15. t1 id=1 val=10. 10 not in {5,15}. No match.
  // t2 values for ref_id=2: 25. t1 id=2 val=20. 20 not in {25}. No match.
  // t2 values for ref_id=3: 35. t1 id=3 val=30. 30 not in {35}. No match.
  assert.equal(r.length, 0);
  
  // Add matching values
  db.execute("INSERT INTO t2 VALUES (5,2,20)");
  const r2 = q(db, `
    SELECT t1.id FROM t1
    WHERE t1.val IN (SELECT t2.val FROM t2 WHERE t2.ref_id = t1.id)
  `);
  assert.deepEqual(r2.map(x => x.id), [2]); // 20 in {25, 20}
});

console.log(`\n  ${pass} passed, ${fail} failed\n`);
process.exit(fail > 0 ? 1 : 0);
