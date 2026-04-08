// plan-viz.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PlanVisualizer } from './plan-viz.js';

describe('PlanVisualizer', () => {
  const samplePlan = {
    type: 'Projection',
    columns: ['name', 'salary'],
    cost: 150,
    rows: 50,
    children: [{
      type: 'HashJoin',
      joinType: 'inner',
      joinKey: 'dept_id',
      cost: 120,
      rows: 50,
      children: [
        {
          type: 'Filter',
          predicate: 'salary > 100000',
          cost: 80,
          rows: 30,
          children: [{
            type: 'SeqScan',
            table: 'employees',
            cost: 50,
            rows: 1000,
          }],
        },
        {
          type: 'IndexScan',
          table: 'departments',
          predicate: 'active = true',
          cost: 10,
          rows: 5,
        },
      ],
    }],
  };

  it('generates valid DOT output', () => {
    const viz = new PlanVisualizer();
    const dot = viz.toDot(samplePlan);
    
    assert.ok(dot.startsWith('digraph QueryPlan'));
    assert.ok(dot.includes('Projection'));
    assert.ok(dot.includes('HashJoin'));
    assert.ok(dot.includes('SeqScan'));
    assert.ok(dot.includes('IndexScan'));
    assert.ok(dot.includes('->'));
    assert.ok(dot.endsWith('}'));
  });

  it('generates text tree', () => {
    const viz = new PlanVisualizer();
    const text = viz.toText(samplePlan);
    
    assert.ok(text.includes('Projection'));
    assert.ok(text.includes('HashJoin'));
    assert.ok(text.includes('SeqScan'));
    assert.ok(text.includes('salary > 100000'));
    assert.ok(text.includes('cost='));
    assert.ok(text.includes('rows='));
  });

  it('generates JSON summary', () => {
    const viz = new PlanVisualizer();
    const json = viz.toJSON(samplePlan);
    
    assert.equal(json.type, 'Projection');
    assert.equal(json.children.length, 1);
    assert.equal(json.children[0].type, 'HashJoin');
    assert.equal(json.children[0].children.length, 2);
  });

  it('handles single-node plan', () => {
    const viz = new PlanVisualizer();
    const dot = viz.toDot({ type: 'SeqScan', table: 'users', cost: 10, rows: 100 });
    assert.ok(dot.includes('SeqScan'));
    assert.ok(!dot.includes('->'));
  });

  it('DOT includes node colors', () => {
    const viz = new PlanVisualizer();
    const dot = viz.toDot(samplePlan);
    assert.ok(dot.includes('fillcolor'));
    assert.ok(dot.includes('#FFE0B2')); // SeqScan color
  });

  it('text tree shows indentation', () => {
    const viz = new PlanVisualizer();
    const text = viz.toText(samplePlan);
    assert.ok(text.includes('└─'));
  });

  it('handles plan with sort and limit', () => {
    const plan = {
      type: 'Limit',
      limit: 10,
      children: [{
        type: 'Sort',
        sortKey: 'salary DESC',
        children: [{
          type: 'SeqScan',
          table: 'employees',
        }],
      }],
    };
    const viz = new PlanVisualizer();
    const dot = viz.toDot(plan);
    assert.ok(dot.includes('limit: 10'));
    assert.ok(dot.includes('sort: salary DESC'));
  });
});
