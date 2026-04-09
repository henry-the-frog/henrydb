// explain-pretty.test.js — Tests for EXPLAIN pretty-printer
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { prettyExplain, explainAsTable, explainAsJSON } from './explain-pretty.js';

describe('EXPLAIN Pretty Printer', () => {
  const plan = [
    { operation: 'Seq Scan', table: 'users', estimated_rows: 100 },
    { operation: 'Index Scan', table: 'orders', index: 'idx_user_id', estimated_rows: 5, condition: 'user_id = ?' },
    { operation: 'Hash Join', estimated_rows: 5, condition: 'users.id = orders.user_id' },
  ];

  it('prettyExplain generates ASCII tree', () => {
    const result = prettyExplain(plan);
    assert.ok(result.includes('Seq Scan'));
    assert.ok(result.includes('Index Scan'));
    assert.ok(result.includes('Hash Join'));
    assert.ok(result.includes('users'));
    assert.ok(result.includes('idx_user_id'));
  });

  it('handles empty plan', () => {
    assert.ok(prettyExplain([]).includes('empty'));
    assert.ok(prettyExplain(null).includes('empty'));
  });

  it('shows estimated rows', () => {
    const result = prettyExplain(plan);
    assert.ok(result.includes('~100'));
  });

  it('explainAsTable generates markdown', () => {
    const result = explainAsTable(plan);
    assert.ok(result.includes('|'));
    assert.ok(result.includes('Seq Scan'));
    assert.ok(result.includes('---'));
  });

  it('explainAsJSON generates valid JSON', () => {
    const result = explainAsJSON(plan);
    const parsed = JSON.parse(result);
    assert.equal(parsed.length, 3);
  });

  it('handles plan with optional fields', () => {
    const simple = [{ operation: 'Scan', table: 'x' }];
    assert.ok(prettyExplain(simple).includes('Scan'));
  });
});
