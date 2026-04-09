// projection-pushdown.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { pushdownProjection } from './projection-pushdown.js';

describe('Projection Pushdown', () => {
  it('reduces scan columns', () => {
    const plan = {
      type: 'project',
      columns: ['name', 'age'],
      child: {
        type: 'scan',
        columns: ['id', 'name', 'age', 'email'],
      },
    };
    
    const optimized = pushdownProjection(plan, new Set(['name']));
    assert.equal(optimized.child.columns.length, 1);
    assert.deepEqual(optimized.child.columns, ['name']);
  });
});
