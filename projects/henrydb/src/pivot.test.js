import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

test('PIVOT basic crosstab', () => {
  const db = new Database();
  db.execute("CREATE TABLE sales (product TEXT, quarter TEXT, amount INT)");
  db.execute("INSERT INTO sales VALUES ('Widget', 'Q1', 100)");
  db.execute("INSERT INTO sales VALUES ('Widget', 'Q2', 150)");
  db.execute("INSERT INTO sales VALUES ('Widget', 'Q3', 200)");
  db.execute("INSERT INTO sales VALUES ('Gadget', 'Q1', 50)");
  db.execute("INSERT INTO sales VALUES ('Gadget', 'Q2', 75)");
  db.execute("INSERT INTO sales VALUES ('Gadget', 'Q3', 100)");
  
  const result = db.execute("SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2', 'Q3'))");
  assert.equal(result.rows.length, 2);
  const widget = result.rows.find(r => r.product === 'Widget');
  const gadget = result.rows.find(r => r.product === 'Gadget');
  assert.equal(widget['Q1'], 100);
  assert.equal(widget['Q2'], 150);
  assert.equal(widget['Q3'], 200);
  assert.equal(gadget['Q1'], 50);
  assert.equal(gadget['Q2'], 75);
  assert.equal(gadget['Q3'], 100);
});

test('PIVOT with COUNT', () => {
  const db = new Database();
  db.execute("CREATE TABLE orders (status TEXT, region TEXT, amount INT)");
  db.execute("INSERT INTO orders VALUES ('shipped', 'east', 1)");
  db.execute("INSERT INTO orders VALUES ('shipped', 'west', 1)");
  db.execute("INSERT INTO orders VALUES ('pending', 'east', 1)");
  db.execute("INSERT INTO orders VALUES ('shipped', 'east', 1)");
  db.execute("INSERT INTO orders VALUES ('pending', 'west', 1)");
  
  const result = db.execute("SELECT * FROM orders PIVOT (COUNT(amount) FOR region IN ('east', 'west'))");
  assert.equal(result.rows.length, 2);
  const shipped = result.rows.find(r => r.status === 'shipped');
  const pending = result.rows.find(r => r.status === 'pending');
  assert.equal(shipped['east'], 2);
  assert.equal(shipped['west'], 1);
  assert.equal(pending['east'], 1);
  assert.equal(pending['west'], 1);
});

test('PIVOT with AVG', () => {
  const db = new Database();
  db.execute("CREATE TABLE scores (student TEXT, subject TEXT, score INT)");
  db.execute("INSERT INTO scores VALUES ('Alice', 'math', 90)");
  db.execute("INSERT INTO scores VALUES ('Alice', 'science', 85)");
  db.execute("INSERT INTO scores VALUES ('Bob', 'math', 70)");
  db.execute("INSERT INTO scores VALUES ('Bob', 'science', 80)");
  
  const result = db.execute("SELECT * FROM scores PIVOT (AVG(score) FOR subject IN ('math', 'science'))");
  assert.equal(result.rows.length, 2);
  const alice = result.rows.find(r => r.student === 'Alice');
  assert.equal(alice['math'], 90);
  assert.equal(alice['science'], 85);
});

test('PIVOT with NULL values', () => {
  const db = new Database();
  db.execute("CREATE TABLE data (name TEXT, category TEXT, val INT)");
  db.execute("INSERT INTO data VALUES ('A', 'x', 10)");
  db.execute("INSERT INTO data VALUES ('A', 'y', 20)");
  db.execute("INSERT INTO data VALUES ('B', 'x', 30)");
  // B has no 'y' category
  
  const result = db.execute("SELECT * FROM data PIVOT (SUM(val) FOR category IN ('x', 'y'))");
  assert.equal(result.rows.length, 2);
  const b = result.rows.find(r => r.name === 'B');
  assert.equal(b['x'], 30);
  assert.equal(b['y'], null);
});

test('UNPIVOT basic', () => {
  const db = new Database();
  db.execute("CREATE TABLE quarterly (product TEXT, q1 INT, q2 INT, q3 INT)");
  db.execute("INSERT INTO quarterly VALUES ('Widget', 100, 150, 200)");
  db.execute("INSERT INTO quarterly VALUES ('Gadget', 50, 75, 100)");
  
  const result = db.execute("SELECT * FROM quarterly UNPIVOT (amount FOR quarter IN (q1, q2, q3))");
  assert.equal(result.rows.length, 6);
  const widgetQ1 = result.rows.find(r => r.product === 'Widget' && r.quarter === 'q1');
  assert.equal(widgetQ1.amount, 100);
  const gadgetQ3 = result.rows.find(r => r.product === 'Gadget' && r.quarter === 'q3');
  assert.equal(gadgetQ3.amount, 100);
});

test('UNPIVOT excludes NULL values', () => {
  const db = new Database();
  db.execute("CREATE TABLE data (id INT, a INT, b INT, c INT)");
  db.execute("INSERT INTO data VALUES (1, 10, NULL, 30)");
  
  const result = db.execute("SELECT * FROM data UNPIVOT (val FOR col IN (a, b, c))");
  // NULL in column 'b' should be excluded
  assert.equal(result.rows.length, 2);
  assert.ok(result.rows.every(r => r.val !== null));
  assert.equal(result.rows[0].col, 'a');
  assert.equal(result.rows[0].val, 10);
  assert.equal(result.rows[1].col, 'c');
  assert.equal(result.rows[1].val, 30);
});

test('PIVOT with alias', () => {
  const db = new Database();
  db.execute("CREATE TABLE sales (product TEXT, quarter TEXT, amount INT)");
  db.execute("INSERT INTO sales VALUES ('Widget', 'Q1', 100)");
  db.execute("INSERT INTO sales VALUES ('Widget', 'Q2', 200)");
  
  const result = db.execute("SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2')) AS p");
  assert.equal(result.rows.length, 1);
  assert.equal(result.rows[0]['Q1'], 100);
  assert.equal(result.rows[0]['Q2'], 200);
});

test('PIVOT with multiple grouping columns', () => {
  const db = new Database();
  db.execute("CREATE TABLE sales (region TEXT, product TEXT, quarter TEXT, amount INT)");
  db.execute("INSERT INTO sales VALUES ('east', 'Widget', 'Q1', 100)");
  db.execute("INSERT INTO sales VALUES ('east', 'Widget', 'Q2', 150)");
  db.execute("INSERT INTO sales VALUES ('west', 'Widget', 'Q1', 200)");
  db.execute("INSERT INTO sales VALUES ('east', 'Gadget', 'Q1', 50)");
  
  const result = db.execute("SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2'))");
  assert.equal(result.rows.length, 3); // east+Widget, west+Widget, east+Gadget
  const eastWidget = result.rows.find(r => r.region === 'east' && r.product === 'Widget');
  assert.equal(eastWidget['Q1'], 100);
  assert.equal(eastWidget['Q2'], 150);
});

test('PIVOT with MAX aggregate', () => {
  const db = new Database();
  db.execute("CREATE TABLE temps (city TEXT, month TEXT, temp INT)");
  db.execute("INSERT INTO temps VALUES ('NYC', 'Jan', 30)");
  db.execute("INSERT INTO temps VALUES ('NYC', 'Jan', 35)");
  db.execute("INSERT INTO temps VALUES ('NYC', 'Jul', 85)");
  db.execute("INSERT INTO temps VALUES ('LA', 'Jan', 60)");
  db.execute("INSERT INTO temps VALUES ('LA', 'Jul', 90)");
  
  const result = db.execute("SELECT * FROM temps PIVOT (MAX(temp) FOR month IN ('Jan', 'Jul'))");
  const nyc = result.rows.find(r => r.city === 'NYC');
  assert.equal(nyc['Jan'], 35); // MAX
  assert.equal(nyc['Jul'], 85);
});

test('PIVOT roundtrip: PIVOT then UNPIVOT', () => {
  const db = new Database();
  db.execute("CREATE TABLE sales (product TEXT, quarter TEXT, amount INT)");
  db.execute("INSERT INTO sales VALUES ('Widget', 'Q1', 100)");
  db.execute("INSERT INTO sales VALUES ('Widget', 'Q2', 200)");
  db.execute("INSERT INTO sales VALUES ('Gadget', 'Q1', 50)");
  db.execute("INSERT INTO sales VALUES ('Gadget', 'Q2', 75)");
  
  // PIVOT first
  const pivoted = db.execute("SELECT * FROM sales PIVOT (SUM(amount) FOR quarter IN ('Q1', 'Q2'))");
  assert.equal(pivoted.rows.length, 2);
  
  // Now store pivoted data and UNPIVOT
  db.execute("CREATE TABLE pivoted (product TEXT, Q1 INT, Q2 INT)");
  for (const row of pivoted.rows) {
    db.execute(`INSERT INTO pivoted VALUES ('${row.product}', ${row['Q1']}, ${row['Q2']})`);
  }
  
  const unpivoted = db.execute("SELECT * FROM pivoted UNPIVOT (amount FOR quarter IN (Q1, Q2))");
  assert.equal(unpivoted.rows.length, 4);
});

test('PIVOT with MIN aggregate', () => {
  const db = new Database();
  db.execute("CREATE TABLE prices (item TEXT, store TEXT, price INT)");
  db.execute("INSERT INTO prices VALUES ('Apple', 'StoreA', 3)");
  db.execute("INSERT INTO prices VALUES ('Apple', 'StoreA', 2)");
  db.execute("INSERT INTO prices VALUES ('Apple', 'StoreB', 4)");
  db.execute("INSERT INTO prices VALUES ('Banana', 'StoreA', 1)");
  db.execute("INSERT INTO prices VALUES ('Banana', 'StoreB', 2)");
  
  const result = db.execute("SELECT * FROM prices PIVOT (MIN(price) FOR store IN ('StoreA', 'StoreB'))");
  const apple = result.rows.find(r => r.item === 'Apple');
  assert.equal(apple['StoreA'], 2); // MIN
  assert.equal(apple['StoreB'], 4);
});
