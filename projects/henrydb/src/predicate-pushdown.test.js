// predicate-pushdown.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { pushdownPredicate } from './predicate-pushdown.js';

describe('PredicatePushdown', () => {
  it('pushes filter below join to left side', () => {
    const plan = {
      type: 'filter',
      predicates: [{ columns: ['age'], fn: 'gt', value: 25 }],
      child: {
        type: 'join',
        left: { type: 'scan', columns: ['id', 'age'] },
        right: { type: 'scan', columns: ['order_id', 'user_id'] },
      },
    };
    
    const optimized = pushdownPredicate(plan);
    // Filter should be pushed to left side
    assert.equal(optimized.type, 'join');
    assert.equal(optimized.left.type, 'filter');
  });
});
