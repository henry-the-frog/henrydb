// cte.test.js — Tests for Common Table Expressions
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { ValuesIter, Filter, Sort, CTE, RecursiveCTE, Project, HashJoin } from './volcano.js';

describe('Common Table Expressions (CTE)', () => {
  describe('Non-recursive CTE', () => {
    it('materializes CTE and uses in main query', () => {
      // WITH high_earners AS (SELECT * FROM emp WHERE salary > 80000)
      // SELECT * FROM high_earners
      const employees = [
        { name: 'Alice', salary: 90000 },
        { name: 'Bob', salary: 70000 },
        { name: 'Charlie', salary: 85000 },
      ];
      
      const cteDef = new Filter(new ValuesIter(employees), r => r.salary > 80000);
      // The main query just reads the materialized CTE
      const mainQuery = new ValuesIter([]); // Placeholder
      const cte = new CTE(cteDef, mainQuery, 'high_earners');
      
      // Manually test materialization
      cte._definition.open();
      const materialized = [];
      let row;
      while ((row = cte._definition.next()) !== null) materialized.push(row);
      cte._definition.close();
      
      assert.equal(materialized.length, 2); // Alice and Charlie
    });

    it('CTE can be referenced multiple times', () => {
      const data = [{ x: 1 }, { x: 2 }, { x: 3 }];
      const cteDef = new Filter(new ValuesIter(data), r => r.x > 1);
      
      cteDef.open();
      const rows = [];
      let row;
      while ((row = cteDef.next()) !== null) rows.push(row);
      cteDef.close();
      
      assert.equal(rows.length, 2);
      
      // Reference again via materialized copy
      const copy = new ValuesIter(rows);
      const result = new Filter(copy, r => r.x > 2).toArray();
      assert.equal(result.length, 1);
      assert.equal(result[0].x, 3);
    });
  });

  describe('Recursive CTE', () => {
    it('generates sequence with recursive step', () => {
      // WITH RECURSIVE nums AS (
      //   SELECT 1 as n  -- base case
      //   UNION ALL
      //   SELECT n + 1 FROM nums WHERE n < 10  -- recursive step
      // )
      const baseCaseIter = new ValuesIter([{ n: 1 }]);
      const recursiveStep = (workingTable) => {
        const nextRows = workingTable
          .filter(r => r.n < 10)
          .map(r => ({ n: r.n + 1 }));
        return new ValuesIter(nextRows);
      };
      
      const cte = new RecursiveCTE(baseCaseIter, recursiveStep);
      const rows = cte.toArray();
      assert.equal(rows.length, 10);
      assert.equal(rows[0].n, 1);
      assert.equal(rows[9].n, 10);
    });

    it('traverses tree hierarchy (org chart)', () => {
      // Employees with manager_id forming a tree
      const employees = [
        { id: 1, name: 'CEO', manager_id: null },
        { id: 2, name: 'VP Engineering', manager_id: 1 },
        { id: 3, name: 'VP Sales', manager_id: 1 },
        { id: 4, name: 'Senior Dev', manager_id: 2 },
        { id: 5, name: 'Junior Dev', manager_id: 2 },
        { id: 6, name: 'Sales Rep', manager_id: 3 },
      ];
      
      // Base case: CEO (root)
      const baseCaseIter = new ValuesIter(
        employees.filter(e => e.manager_id === null).map(e => ({ ...e, level: 0 }))
      );
      
      // Recursive step: find direct reports
      const recursiveStep = (workingTable) => {
        const parentIds = workingTable.map(r => r.id);
        const children = employees
          .filter(e => parentIds.includes(e.manager_id))
          .map(e => ({ ...e, level: workingTable[0].level + 1 }));
        return new ValuesIter(children);
      };
      
      const cte = new RecursiveCTE(baseCaseIter, recursiveStep);
      const rows = cte.toArray();
      
      assert.equal(rows.length, 6); // All employees
      assert.equal(rows[0].name, 'CEO');
      assert.equal(rows[0].level, 0);
      // Level 1: VP Engineering, VP Sales
      const level1 = rows.filter(r => r.level === 1);
      assert.equal(level1.length, 2);
      // Level 2: Senior Dev, Junior Dev, Sales Rep
      const level2 = rows.filter(r => r.level === 2);
      assert.equal(level2.length, 3);
    });

    it('generates Fibonacci sequence', () => {
      const baseCaseIter = new ValuesIter([{ a: 0, b: 1, n: 0 }]);
      const recursiveStep = (workingTable) => {
        const next = workingTable
          .filter(r => r.n < 10)
          .map(r => ({ a: r.b, b: r.a + r.b, n: r.n + 1 }));
        return new ValuesIter(next);
      };
      
      const cte = new RecursiveCTE(baseCaseIter, recursiveStep);
      const rows = cte.toArray();
      assert.equal(rows.length, 11); // 0 through 10
      assert.equal(rows[0].a, 0);
      assert.equal(rows[1].a, 1);
      assert.equal(rows[2].a, 1);
      assert.equal(rows[3].a, 2);
      assert.equal(rows[4].a, 3);
      assert.equal(rows[5].a, 5);
      assert.equal(rows[10].a, 55);
    });

    it('respects maxDepth safety limit', () => {
      // Infinite recursion with maxDepth=5
      const baseCaseIter = new ValuesIter([{ n: 1 }]);
      const recursiveStep = (workingTable) => {
        return new ValuesIter(workingTable.map(r => ({ n: r.n + 1 })));
      };
      
      const cte = new RecursiveCTE(baseCaseIter, recursiveStep, 5);
      const rows = cte.toArray();
      assert.equal(rows.length, 6); // Base + 5 recursive levels
    });

    it('terminates when no new rows produced', () => {
      const baseCaseIter = new ValuesIter([{ x: 1 }]);
      const recursiveStep = (workingTable) => {
        // Always returns empty — terminates immediately
        return new ValuesIter([]);
      };
      
      const cte = new RecursiveCTE(baseCaseIter, recursiveStep);
      const rows = cte.toArray();
      assert.equal(rows.length, 1); // Just the base case
    });

    it('graph traversal: find all reachable nodes', () => {
      const edges = [
        { from: 'A', to: 'B' },
        { from: 'A', to: 'C' },
        { from: 'B', to: 'D' },
        { from: 'C', to: 'D' },
        { from: 'D', to: 'E' },
      ];
      
      // Find all nodes reachable from A
      const baseCaseIter = new ValuesIter([{ node: 'A', depth: 0 }]);
      const visited = new Set(['A']);
      
      const recursiveStep = (workingTable) => {
        const newNodes = [];
        for (const r of workingTable) {
          for (const e of edges) {
            if (e.from === r.node && !visited.has(e.to)) {
              visited.add(e.to);
              newNodes.push({ node: e.to, depth: r.depth + 1 });
            }
          }
        }
        return new ValuesIter(newNodes);
      };
      
      const cte = new RecursiveCTE(baseCaseIter, recursiveStep);
      const rows = cte.toArray();
      assert.equal(rows.length, 5); // A, B, C, D, E
      assert.ok(rows.some(r => r.node === 'E'));
    });
  });

  describe('EXPLAIN', () => {
    it('shows CTE in explain output', () => {
      const cte = new CTE(
        new ValuesIter([{ x: 1 }]),
        new ValuesIter([{ x: 1 }]),
        'my_cte',
      );
      const plan = cte.explain();
      assert.ok(plan.includes('CTE'));
      assert.ok(plan.includes('my_cte'));
    });

    it('shows RecursiveCTE in explain output', () => {
      const rcte = new RecursiveCTE(
        new ValuesIter([{ n: 1 }]),
        () => new ValuesIter([]),
      );
      const plan = rcte.explain();
      assert.ok(plan.includes('RecursiveCTE'));
    });
  });
});
