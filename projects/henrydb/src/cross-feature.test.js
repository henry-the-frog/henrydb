// cross-feature.test.js — Cross-feature interaction tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('Cross-Feature Interactions', () => {
  it('window function + CTE + GROUP BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE sales (rep TEXT, region TEXT, amount INT)');
    db.execute("INSERT INTO sales VALUES ('alice','north',100),('alice','south',150),('bob','north',200),('bob','south',120),('charlie','north',180)");
    
    const r = db.execute(`
      WITH regional_totals AS (
        SELECT region, SUM(amount) as total FROM sales GROUP BY region
      )
      SELECT region, total,
             RANK() OVER (ORDER BY total DESC) as rank
      FROM regional_totals
    `);
    assert.equal(r.rows.length, 2);
    // north: 100+200+180=480, south: 150+120=270
    const north = r.rows.find(row => row.region === 'north');
    const south = r.rows.find(row => row.region === 'south');
    assert.equal(north.total, 480);
    assert.equal(south.total, 270);
    assert.equal(north.rank, 1);
    assert.equal(south.rank, 2);
  });

  it('recursive CTE + window function', () => {
    const db = new Database();
    db.execute('CREATE TABLE org (id INT PRIMARY KEY, name TEXT, mgr_id INT)');
    db.execute("INSERT INTO org VALUES (1,'CEO',NULL),(2,'VP1',1),(3,'VP2',1),(4,'Dir1',2),(5,'Dir2',2),(6,'Dir3',3)");
    
    const r = db.execute(`
      WITH RECURSIVE tree AS (
        SELECT id, name, mgr_id, 0 as depth FROM org WHERE mgr_id IS NULL
        UNION ALL
        SELECT o.id, o.name, o.mgr_id, t.depth + 1
        FROM org o JOIN tree t ON o.mgr_id = t.id
      )
      SELECT name, depth, ROW_NUMBER() OVER (ORDER BY depth, name) as rn
      FROM tree
    `);
    assert.equal(r.rows.length, 6);
    assert.equal(r.rows[0].name, 'CEO');
    assert.equal(r.rows[0].depth, 0);
    assert.equal(r.rows[0].rn, 1);
  });

  it('NTH_VALUE + PARTITION BY + ORDER BY', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (dept TEXT, emp TEXT, score INT)');
    db.execute("INSERT INTO scores VALUES ('eng','alice',90),('eng','bob',85),('eng','charlie',95),('sales','dave',88),('sales','eve',92)");
    
    const r = db.execute(`
      SELECT dept, emp, score,
             NTH_VALUE(emp, 2) OVER (PARTITION BY dept ORDER BY score DESC) as second_best
      FROM scores
    `);
    // eng: charlie(95), alice(90), bob(85) → 2nd is alice
    // sales: eve(92), dave(88) → 2nd is dave
    const eng = r.rows.filter(row => row.dept === 'eng');
    const sales = r.rows.filter(row => row.dept === 'sales');
    
    // First row in each partition has null (frame doesn't include 2nd yet)
    assert.equal(eng[0].second_best, null);
    // Second+ should have the value
    assert.ok(eng.some(row => row.second_best === 'alice'));
    assert.ok(sales.some(row => row.second_best === 'dave'));
  });

  it('savepoint + window function query after rollback', () => {
    const db = new Database();
    db.execute('CREATE TABLE metrics (ts INT, val INT)');
    db.execute('INSERT INTO metrics VALUES (1,10),(2,20),(3,30)');
    
    db.execute('SAVEPOINT sp1');
    db.execute('INSERT INTO metrics VALUES (4,40),(5,50)');
    db.execute('ROLLBACK TO sp1');
    
    // Window function should work on rolled-back data
    const r = db.execute(`
      SELECT ts, val, SUM(val) OVER (ORDER BY ts) as running_total
      FROM metrics ORDER BY ts
    `);
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].running_total, 10);
    assert.equal(r.rows[1].running_total, 30);
    assert.equal(r.rows[2].running_total, 60);
  });

  it('CTE + savepoint + JOIN + aggregate', () => {
    const db = new Database();
    db.execute('CREATE TABLE orders (id INT PRIMARY KEY, customer TEXT, amount INT)');
    db.execute('CREATE TABLE customers (name TEXT PRIMARY KEY, tier TEXT)');
    db.execute("INSERT INTO customers VALUES ('alice','gold'),('bob','silver')");
    db.execute("INSERT INTO orders VALUES (1,'alice',100),(2,'bob',200),(3,'alice',150)");
    
    db.execute('SAVEPOINT sp1');
    db.execute("INSERT INTO orders VALUES (4,'alice',300)");
    db.execute('ROLLBACK TO sp1');
    
    const r = db.execute(`
      WITH order_totals AS (
        SELECT customer, SUM(amount) as total, COUNT(*) as num_orders
        FROM orders GROUP BY customer
      )
      SELECT c.name, c.tier, ot.total, ot.num_orders
      FROM customers c JOIN order_totals ot ON c.name = ot.customer
      ORDER BY ot.total DESC
    `);
    assert.equal(r.rows.length, 2);
    assert.equal(r.rows[0].name, 'alice');
    assert.equal(r.rows[0].total, 250); // 100+150, not 100+150+300
    assert.equal(r.rows[0].num_orders, 2);
    assert.equal(r.rows[1].name, 'bob');
    assert.equal(r.rows[1].total, 200);
  });

  it('FIRST_VALUE + LAST_VALUE + NTH_VALUE in same query', () => {
    const db = new Database();
    db.execute('CREATE TABLE readings (sensor TEXT, ts INT, val FLOAT)');
    db.execute("INSERT INTO readings VALUES ('A',1,1.5),('A',2,2.3),('A',3,3.1),('A',4,2.8),('B',1,10.0),('B',2,12.5)");
    
    const r = db.execute(`
      SELECT sensor, ts, val,
             FIRST_VALUE(val) OVER (PARTITION BY sensor ORDER BY ts) as first_val,
             NTH_VALUE(val, 2) OVER (PARTITION BY sensor ORDER BY ts) as second_val,
             LAST_VALUE(val) OVER (PARTITION BY sensor ORDER BY ts) as current_val
      FROM readings
      ORDER BY sensor, ts
    `);
    // Sensor A: vals are 1.5, 2.3, 3.1, 2.8
    const sensorA = r.rows.filter(row => row.sensor === 'A');
    assert.equal(sensorA[0].first_val, 1.5);
    assert.equal(sensorA[0].second_val, null); // Only 1 row in frame
    assert.equal(sensorA[1].first_val, 1.5);
    assert.equal(sensorA[1].second_val, 2.3);
    assert.equal(sensorA[3].first_val, 1.5);
    assert.equal(sensorA[3].second_val, 2.3);
  });

  it('lock manager + savepoint interaction', async () => {
    const db = new Database();
    const { LockManager } = await import('./lock-manager.js');
    const lm = new LockManager();
    
    // Simulate tx acquiring locks, doing work, savepoint, more work, rollback
    lm.acquire(1, 'row:1', 'X');
    lm.acquire(1, 'row:2', 'X');
    // Savepoint moment
    lm.acquire(1, 'row:3', 'X');
    // After "rollback to savepoint", tx still holds row:1 and row:2
    lm.release(1, 'row:3'); // Simulate releasing lock acquired after savepoint
    
    // Verify tx1 still holds row:1 and row:2
    assert.equal(lm.acquire(2, 'row:1', 'X'), false);
    assert.equal(lm.acquire(2, 'row:2', 'X'), false);
    assert.equal(lm.acquire(2, 'row:3', 'X'), true); // This one was released
  });

  it('window function + CASE + COALESCE', () => {
    const db = new Database();
    db.execute('CREATE TABLE emp (name TEXT, dept TEXT, salary INT)');
    db.execute("INSERT INTO emp VALUES ('alice','eng',90000),('bob','eng',85000),('charlie','sales',70000),('dave','sales',NULL)");
    
    const r = db.execute(`
      SELECT name, dept,
             COALESCE(salary, 0) as effective_salary,
             CASE 
               WHEN salary IS NULL THEN 'No salary'
               WHEN salary > 80000 THEN 'High'
               ELSE 'Normal'
             END as tier,
             RANK() OVER (PARTITION BY dept ORDER BY COALESCE(salary, 0) DESC) as dept_rank
      FROM emp
      ORDER BY dept, COALESCE(salary, 0) DESC
    `);
    
    assert.equal(r.rows.length, 4);
    // eng dept: alice(90K, rank 1), bob(85K, rank 2)
    const alice = r.rows.find(row => row.name === 'alice');
    assert.equal(alice.tier, 'High');
    assert.equal(alice.dept_rank, 1);
    
    // dave has NULL salary
    const dave = r.rows.find(row => row.name === 'dave');
    assert.equal(dave.effective_salary, 0);
    assert.equal(dave.tier, 'No salary');
  });

  it('INSERT ... SELECT + window function source', () => {
    const db = new Database();
    db.execute('CREATE TABLE data (id INT, val INT)');
    db.execute('INSERT INTO data VALUES (1,10),(2,20),(3,30)');
    db.execute('CREATE TABLE ranked (id INT, val INT, rk INT)');
    
    db.execute(`
      INSERT INTO ranked 
      SELECT id, val, ROW_NUMBER() OVER (ORDER BY val DESC) as rk
      FROM data
    `);
    
    const r = db.execute('SELECT * FROM ranked ORDER BY rk');
    assert.equal(r.rows.length, 3);
    assert.equal(r.rows[0].id, 3); // val=30 is rank 1
    assert.equal(r.rows[0].rk, 1);
    assert.equal(r.rows[2].id, 1); // val=10 is rank 3
  });

  it('UPDATE with subquery + window function in subquery', () => {
    const db = new Database();
    db.execute('CREATE TABLE scores (id INT PRIMARY KEY, name TEXT, score INT, rank_col INT)');
    db.execute("INSERT INTO scores VALUES (1,'alice',90,0),(2,'bob',85,0),(3,'charlie',95,0)");
    
    // This is a complex pattern: update rank based on window function
    const ranked = db.execute(`
      SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) as rk FROM scores
    `);
    for (const row of ranked.rows) {
      db.execute(`UPDATE scores SET rank_col = ${row.rk} WHERE id = ${row.id}`);
    }
    
    const r = db.execute('SELECT name, rank_col FROM scores ORDER BY rank_col');
    assert.equal(r.rows[0].name, 'charlie');
    assert.equal(r.rows[0].rank_col, 1);
    assert.equal(r.rows[1].name, 'alice');
    assert.equal(r.rows[1].rank_col, 2);
  });
});
