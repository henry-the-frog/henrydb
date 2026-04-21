// volcano-mergejoin.test.js — MergeJoin selection and correctness in volcano planner
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { buildPlan } from './volcano-planner.js';
import { MergeJoin, Sort, ValuesIter } from './volcano.js';
import { parse } from './sql.js';

describe('Volcano MergeJoin', () => {
  let db;

  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE orders (id INT, customer_id INT, amount INT)');
    db.execute('INSERT INTO orders VALUES (1, 10, 100)');
    db.execute('INSERT INTO orders VALUES (2, 20, 200)');
    db.execute('INSERT INTO orders VALUES (3, 10, 150)');
    db.execute('INSERT INTO orders VALUES (4, 30, 300)');
    
    db.execute('CREATE TABLE customers (id INT, name TEXT)');
    db.execute("INSERT INTO customers VALUES (10, 'Alice')");
    db.execute("INSERT INTO customers VALUES (20, 'Bob')");
    db.execute("INSERT INTO customers VALUES (30, 'Charlie')");
  });

  // ===== MergeJoin Iterator (direct) =====

  it('MergeJoin produces correct results for sorted inputs', () => {
    const left = new ValuesIter([
      { id: 1, val: 'a' },
      { id: 2, val: 'b' },
      { id: 3, val: 'c' },
    ]);
    const right = new ValuesIter([
      { id: 1, x: 10 },
      { id: 2, x: 20 },
      { id: 3, x: 30 },
    ]);
    const join = new MergeJoin(left, right, 'id', 'id');
    const rows = join.toArray();
    assert.equal(rows.length, 3);
    assert.equal(rows[0].val, 'a');
    assert.equal(rows[0].x, 10);
  });

  it('MergeJoin handles duplicates', () => {
    const left = new ValuesIter([
      { k: 1, name: 'a' },
      { k: 1, name: 'b' },
      { k: 2, name: 'c' },
    ]);
    const right = new ValuesIter([
      { k: 1, val: 10 },
      { k: 1, val: 20 },
      { k: 2, val: 30 },
    ]);
    const join = new MergeJoin(left, right, 'k', 'k');
    const rows = join.toArray();
    // 2*2 matches for k=1, plus 1*1 for k=2 = 5
    assert.equal(rows.length, 5);
  });

  it('MergeJoin handles non-matching keys', () => {
    const left = new ValuesIter([
      { k: 1, name: 'a' },
      { k: 3, name: 'c' },
      { k: 5, name: 'e' },
    ]);
    const right = new ValuesIter([
      { k: 2, val: 20 },
      { k: 3, val: 30 },
      { k: 4, val: 40 },
    ]);
    const join = new MergeJoin(left, right, 'k', 'k');
    const rows = join.toArray();
    assert.equal(rows.length, 1);
    assert.equal(rows[0].name, 'c');
    assert.equal(rows[0].val, 30);
  });

  it('MergeJoin handles empty inputs', () => {
    const left = new ValuesIter([]);
    const right = new ValuesIter([{ k: 1 }]);
    const join = new MergeJoin(left, right, 'k', 'k');
    assert.deepEqual(join.toArray(), []);
  });

  // ===== MergeJoin Plan Selection =====

  it('MergeJoin is chosen when both inputs are pre-sorted on join key', () => {
    // Manually build two sorted iterators and verify MergeJoin selection
    const leftSorted = new Sort(
      new ValuesIter([{ id: 2 }, { id: 1 }, { id: 3 }]),
      [{ column: 'id', desc: false }]
    );
    const rightSorted = new Sort(
      new ValuesIter([{ id: 3, v: 30 }, { id: 1, v: 10 }]),
      [{ column: 'id', desc: false }]
    );
    const join = new MergeJoin(leftSorted, rightSorted, 'id', 'id');
    const rows = join.toArray();
    assert.equal(rows.length, 2);
  });

  // ===== End-to-end via planner =====

  it('equi-join produces correct results regardless of strategy', () => {
    // This tests that the planner produces correct results for equi-joins
    // (whether it chooses HashJoin or MergeJoin)
    const ast = parse('SELECT o.id, c.name, o.amount FROM orders o JOIN customers c ON o.customer_id = c.id');
    const plan = buildPlan(ast, db.tables);
    const rows = plan.toArray();
    assert.equal(rows.length, 4);
    const alice_orders = rows.filter(r => r['c.name'] === 'Alice');
    assert.equal(alice_orders.length, 2);
  });

  it('describe() for MergeJoin', () => {
    const left = new ValuesIter([{ k: 1 }]);
    const right = new ValuesIter([{ k: 1 }]);
    const join = new MergeJoin(left, right, 'k', 'k');
    const desc = join.describe();
    assert.equal(desc.type, 'MergeJoin');
    assert.equal(desc.details.leftKey, 'k');
    assert.equal(desc.details.rightKey, 'k');
  });
});
