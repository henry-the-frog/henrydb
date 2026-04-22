// explain-volcano.test.js — Tests for EXPLAIN FORMAT VOLCANO
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';

describe('EXPLAIN FORMAT VOLCANO', () => {
  let db;
  
  function setup() {
    db = new Database();
    db.execute('CREATE TABLE emp (id INT, name TEXT, dept_id INT)');
    db.execute('CREATE TABLE dept (id INT, dname TEXT)');
    db.execute("INSERT INTO emp VALUES (1, 'Alice', 10), (2, 'Bob', 20), (3, 'Charlie', 10)");
    db.execute("INSERT INTO dept VALUES (10, 'Eng'), (20, 'Sales'), (30, 'HR')");
  }
  
  function planLines(sql) {
    const r = db.execute(sql);
    return r.rows.map(r => r['QUERY PLAN']);
  }
  
  it('shows SeqScan for simple table scan', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT * FROM emp');
    assert.ok(lines.some(l => l.includes('SeqScan')));
    assert.ok(lines.some(l => l.includes('table=emp')));
  });
  
  it('shows HashJoin for equi-join', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id');
    assert.ok(lines.some(l => l.includes('HashJoin')));
  });
  
  it('shows Filter for WHERE clause', () => {
    setup();
    const lines = planLines("EXPLAIN (FORMAT VOLCANO) SELECT * FROM emp WHERE name = 'Alice'");
    assert.ok(lines.some(l => l.includes('Filter')));
  });
  
  it('shows predicate pushdown — single table filter pushed to scan', () => {
    setup();
    const lines = planLines("EXPLAIN (FORMAT VOLCANO) SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept_id = d.id WHERE e.name = 'Alice' AND d.dname = 'Eng'");
    const text = lines.join('\n');
    
    // Filter should appear BELOW HashJoin (pushed to scans), not above it
    const hashJoinIdx = text.indexOf('HashJoin');
    const filter1Idx = text.indexOf('Filter');
    assert.ok(hashJoinIdx !== -1, 'Should have HashJoin');
    assert.ok(filter1Idx !== -1, 'Should have Filter');
    // Filter should be AFTER HashJoin in the tree (i.e., it's a child, not a parent)
    assert.ok(filter1Idx > hashJoinIdx, 'Filter should be below HashJoin (pushed down)');
  });
  
  it('shows HashAggregate for GROUP BY', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT dept_id, COUNT(*) FROM emp GROUP BY dept_id');
    assert.ok(lines.some(l => l.includes('HashAggregate')));
  });
  
  it('shows Sort for ORDER BY', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT * FROM emp ORDER BY name');
    assert.ok(lines.some(l => l.includes('Sort')));
  });
  
  it('shows Limit for LIMIT', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT * FROM emp LIMIT 2');
    assert.ok(lines.some(l => l.includes('Limit')));
  });
  
  it('shows Project for explicit column list', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT name FROM emp');
    assert.ok(lines.some(l => l.includes('Project')));
  });
  
  it('shows correct join type for LEFT JOIN', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT * FROM emp LEFT JOIN dept ON emp.dept_id = dept.id');
    assert.ok(lines.some(l => l.includes('left')));
  });
  
  it('shows correct join type for FULL JOIN', () => {
    setup();
    const lines = planLines('EXPLAIN (FORMAT VOLCANO) SELECT * FROM emp FULL JOIN dept ON emp.dept_id = dept.id');
    assert.ok(lines.some(l => l.includes('full')));
  });
});

describe('Default EXPLAIN includes Volcano plan', () => {
  it('shows Volcano Plan section for joins', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INT, name TEXT)');
    db.execute('CREATE TABLE b (id INT, val TEXT)');
    db.execute("INSERT INTO a VALUES (1, 'x')");
    db.execute("INSERT INTO b VALUES (1, 'y')");
    
    const r = db.execute('EXPLAIN SELECT a.name, b.val FROM a JOIN b ON a.id = b.id');
    const text = r.rows.map(r => r['QUERY PLAN']).join('\n');
    assert.ok(text.includes('Volcano Plan'), 'Default EXPLAIN should include Volcano Plan section');
    assert.ok(text.includes('HashJoin'), 'Should show HashJoin operator');
  });
});
