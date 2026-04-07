// merge-join.test.js — Merge join algorithm tests
import { describe, it, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { QueryPlanner } from './planner.js';

describe('Merge Join', () => {
  let db, planner;

  describe('Execution', () => {
    it('produces correct results for equijoin', () => {
      db = new Database();
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY, val TEXT)');
      db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT, data TEXT)');
      db.execute("INSERT INTO t1 VALUES (1, 'a')");
      db.execute("INSERT INTO t1 VALUES (2, 'b')");
      db.execute("INSERT INTO t1 VALUES (3, 'c')");
      db.execute("INSERT INTO t2 VALUES (1, 1, 'x')");
      db.execute("INSERT INTO t2 VALUES (2, 2, 'y')");
      db.execute("INSERT INTO t2 VALUES (3, 2, 'z')");
      db.execute("INSERT INTO t2 VALUES (4, 3, 'w')");

      planner = new QueryPlanner(db);
      const t2Table = db.tables.get('t2');
      const leftRows = [
        { id: 1, val: 'a' },
        { id: 2, val: 'b' },
        { id: 3, val: 'c' },
      ];
      const joinOn = {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'id' },
        right: { type: 'column_ref', name: 't1_id' },
      };

      const result = planner.executeMergeJoin(leftRows, t2Table, joinOn, t2Table.schema, 't2');
      assert.equal(result.length, 4); // 1→x, 2→y, 2→z, 3→w
    });

    it('handles no matches', () => {
      db = new Database();
      db.execute('CREATE TABLE a (id INT PRIMARY KEY, val INT)');
      db.execute('CREATE TABLE b (id INT PRIMARY KEY, a_id INT)');
      db.execute('INSERT INTO a VALUES (1, 10)');
      db.execute('INSERT INTO b VALUES (1, 99)'); // No matching a_id

      planner = new QueryPlanner(db);
      const bTable = db.tables.get('b');
      const leftRows = [{ id: 1, val: 10 }];
      const joinOn = {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'id' },
        right: { type: 'column_ref', name: 'a_id' },
      };

      const result = planner.executeMergeJoin(leftRows, bTable, joinOn, bTable.schema, 'b');
      assert.equal(result.length, 0);
    });

    it('handles many-to-many join', () => {
      db = new Database();
      db.execute('CREATE TABLE colors (id INT PRIMARY KEY, color TEXT)');
      db.execute('CREATE TABLE sizes (id INT PRIMARY KEY, color_id INT, size TEXT)');
      db.execute("INSERT INTO colors VALUES (1, 'red')");
      db.execute("INSERT INTO colors VALUES (2, 'blue')");
      db.execute("INSERT INTO sizes VALUES (1, 1, 'S')");
      db.execute("INSERT INTO sizes VALUES (2, 1, 'M')");
      db.execute("INSERT INTO sizes VALUES (3, 2, 'S')");
      db.execute("INSERT INTO sizes VALUES (4, 2, 'L')");

      planner = new QueryPlanner(db);
      const sTable = db.tables.get('sizes');
      const leftRows = [
        { id: 1, color: 'red' },
        { id: 2, color: 'blue' },
      ];
      const joinOn = {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'id' },
        right: { type: 'column_ref', name: 'color_id' },
      };

      const result = planner.executeMergeJoin(leftRows, sTable, joinOn, sTable.schema, 'sizes');
      assert.equal(result.length, 4); // 2 colors × 2 sizes each
    });

    it('handles unsorted input correctly', () => {
      db = new Database();
      db.execute('CREATE TABLE t1 (id INT PRIMARY KEY)');
      db.execute('CREATE TABLE t2 (id INT PRIMARY KEY, t1_id INT)');
      // Insert in reverse order
      db.execute('INSERT INTO t1 VALUES (3)');
      db.execute('INSERT INTO t1 VALUES (1)');
      db.execute('INSERT INTO t1 VALUES (2)');
      db.execute('INSERT INTO t2 VALUES (1, 2)');
      db.execute('INSERT INTO t2 VALUES (2, 3)');
      db.execute('INSERT INTO t2 VALUES (3, 1)');

      planner = new QueryPlanner(db);
      const t2Table = db.tables.get('t2');
      const leftRows = [{ id: 3 }, { id: 1 }, { id: 2 }]; // Unsorted
      const joinOn = {
        type: 'COMPARE', op: 'EQ',
        left: { type: 'column_ref', name: 'id' },
        right: { type: 'column_ref', name: 't1_id' },
      };

      const result = planner.executeMergeJoin(leftRows, t2Table, joinOn, t2Table.schema, 't2');
      assert.equal(result.length, 3); // All 3 match
    });
  });

  describe('Planner Integration', () => {
    it('planner considers merge join in cost model', () => {
      db = new Database();
      db.execute('CREATE TABLE employees (id INT PRIMARY KEY, name TEXT)');
      db.execute('CREATE TABLE tasks (id INT PRIMARY KEY, emp_id INT, title TEXT)');
      for (let i = 0; i < 100; i++) db.execute(`INSERT INTO employees VALUES (${i}, 'emp${i}')`);
      for (let i = 0; i < 100; i++) db.execute(`INSERT INTO tasks VALUES (${i}, ${i}, 'task${i}')`);

      planner = new QueryPlanner(db);
      const plan = planner.plan({
        type: 'SELECT', from: { table: 'employees' }, columns: [{ type: 'star' }],
        joins: [{
          joinType: 'INNER', table: 'tasks',
          on: { type: 'COMPARE', op: 'EQ', left: { type: 'column_ref', name: 'id' }, right: { type: 'column_ref', name: 'emp_id' } }
        }],
      });
      // Should have a join step (any type)
      assert.ok(plan.joins.length > 0);
      assert.ok(['HASH_JOIN', 'MERGE_JOIN', 'NESTED_LOOP_JOIN'].includes(plan.joins[0].type));
    });
  });
});
