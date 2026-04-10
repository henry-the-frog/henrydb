// window-functions-wire.test.js — Window functions through wire protocol
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;

describe('Window Functions (Wire Protocol)', () => {
  let server, port, c;
  
  before(async () => {
    port = 36100 + Math.floor(Math.random() * 2000);
    server = new HenryDBServer({ port });
    await server.start();
    c = new Client({ host: '127.0.0.1', port, user: 'test', database: 'testdb' });
    await c.connect();
    
    await c.query('CREATE TABLE emp (id INT, name TEXT, dept TEXT, salary INT)');
    await c.query("INSERT INTO emp VALUES (1, 'Alice', 'eng', 100)");
    await c.query("INSERT INTO emp VALUES (2, 'Bob', 'eng', 120)");
    await c.query("INSERT INTO emp VALUES (3, 'Charlie', 'sales', 80)");
    await c.query("INSERT INTO emp VALUES (4, 'Diana', 'sales', 90)");
    await c.query("INSERT INTO emp VALUES (5, 'Eve', 'eng', 100)"); // Tie with Alice
  });
  
  after(async () => {
    if (c) await c.end();
    if (server) await server.stop();
  });

  it('ROW_NUMBER() OVER (ORDER BY id)', async () => {
    const r = await c.query('SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM emp ORDER BY id');
    assert.equal(r.rows.length, 5);
    assert.equal(String(r.rows[0].rn), '1');
    assert.equal(String(r.rows[4].rn), '5');
  });

  it('ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC)', async () => {
    const r = await c.query('SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM emp');
    // eng: Bob(120)→1, Alice(100)→2, Eve(100)→3
    // sales: Diana(90)→1, Charlie(80)→2
    const eng = r.rows.filter(r => r.dept === 'eng');
    assert.equal(eng.length, 3);
    const bobRow = eng.find(r => r.name === 'Bob');
    assert.equal(String(bobRow.rn), '1'); // Highest salary
    
    const sales = r.rows.filter(r => r.dept === 'sales');
    assert.equal(sales.length, 2);
    const dianaRow = sales.find(r => r.name === 'Diana');
    assert.equal(String(dianaRow.rn), '1'); // Higher salary
  });

  it('RANK() OVER (ORDER BY salary DESC)', async () => {
    const r = await c.query('SELECT name, salary, RANK() OVER (ORDER BY salary DESC) as rnk FROM emp');
    const bobRow = r.rows.find(r => r.name === 'Bob');
    assert.equal(String(bobRow.rnk), '1'); // 120 is highest
    // Alice and Eve both rank 2 (tied at 100)
    const ties = r.rows.filter(r => String(r.rnk) === '2');
    assert.equal(ties.length, 2);
    // Diana is rank 4 (not 3, because of gap from ties)
    const diana = r.rows.find(r => r.name === 'Diana');
    assert.equal(String(diana.rnk), '4');
  });

  it('SUM() OVER (PARTITION BY dept)', async () => {
    const r = await c.query('SELECT name, dept, salary, SUM(salary) OVER (PARTITION BY dept) as dept_total FROM emp ORDER BY dept, name');
    const eng = r.rows.filter(r => r.dept === 'eng');
    assert.equal(String(eng[0].dept_total), '320'); // 100 + 120 + 100
    
    const sales = r.rows.filter(r => r.dept === 'sales');
    assert.equal(String(sales[0].dept_total), '170'); // 80 + 90
  });

  it('AVG() OVER (PARTITION BY dept)', async () => {
    const r = await c.query('SELECT name, dept, AVG(salary) OVER (PARTITION BY dept) as dept_avg FROM emp ORDER BY dept, name');
    const eng = r.rows.filter(r => r.dept === 'eng');
    // 320/3 ≈ 106.67
    const avg = parseFloat(String(eng[0].dept_avg));
    assert.ok(avg > 100 && avg < 120);
  });

  it('COUNT() OVER (PARTITION BY dept)', async () => {
    const r = await c.query('SELECT name, dept, COUNT(*) OVER (PARTITION BY dept) as dept_count FROM emp ORDER BY dept, name');
    const eng = r.rows.filter(r => r.dept === 'eng');
    assert.equal(String(eng[0].dept_count), '3');
    
    const sales = r.rows.filter(r => r.dept === 'sales');
    assert.equal(String(sales[0].dept_count), '2');
  });

  it('window function with WHERE', async () => {
    const r = await c.query("SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn FROM emp WHERE dept = 'eng'");
    assert.equal(r.rows.length, 3);
  });

  it('multiple window functions in one query', async () => {
    const r = await c.query('SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as rn, RANK() OVER (ORDER BY salary DESC) as rnk FROM emp ORDER BY rn');
    assert.equal(r.rows.length, 5);
    assert.ok(r.rows[0].rn !== undefined);
    assert.ok(r.rows[0].rnk !== undefined);
  });
});
