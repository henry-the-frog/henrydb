// plan-html.test.js — Tests for HTML/SVG plan visualization
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { planToHTML } from './plan-html.js';
import { SeqScanNode, IndexScanNode, HashJoinNode, HashNode, SortNode, LimitNode, AggregateNode, PlanBuilder, PlanFormatter } from './query-plan.js';
import { Database } from './db.js';

describe('planToHTML', () => {
  it('generates valid HTML for simple scan', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 1000, estimatedCost: 35.0 });
    scan.filter = 'age > 21';
    
    const html = planToHTML(scan);
    assert.ok(html.includes('<!DOCTYPE html>'));
    assert.ok(html.includes('<svg'));
    assert.ok(html.includes('</svg>'));
    assert.ok(html.includes('HenryDB Query Plan'));
    assert.ok(html.includes('Seq Scan'));
    assert.ok(html.includes('est rows: 1000'));
  });

  it('generates SVG with join tree nodes and edges', () => {
    const left = new SeqScanNode('orders', { estimatedRows: 5000, estimatedCost: 85 });
    const right = new SeqScanNode('users', { estimatedRows: 500, estimatedCost: 25 });
    const hash = new HashNode({ estimatedRows: 500 });
    hash.addChild(right);
    const join = new HashJoinNode('INNER', 'orders.user_id = users.id', { estimatedRows: 5000, estimatedCost: 120 });
    join.addChild(left);
    join.addChild(hash);
    
    const html = planToHTML(join);
    assert.ok(html.includes('Hash Join'));
    assert.ok(html.includes('orders'));
    assert.ok(html.includes('users'));
    assert.ok(html.includes('<path')); // edges
    assert.ok(html.includes('<rect')); // nodes
  });

  it('includes legend with color-coded node types', () => {
    const scan = new SeqScanNode('t', { estimatedRows: 10 });
    const html = planToHTML(scan);
    assert.ok(html.includes('legend'));
    assert.ok(html.includes('Seq Scan'));
    assert.ok(html.includes('Index Scan'));
    assert.ok(html.includes('Hash Join'));
  });

  it('includes text plan alongside SVG', () => {
    const scan = new SeqScanNode('items', { estimatedRows: 42, estimatedCost: 5.0 });
    const html = planToHTML(scan);
    assert.ok(html.includes('Text Plan'));
    assert.ok(html.includes('text-plan'));
    assert.ok(html.includes('Seq Scan on items'));
  });

  it('shows actual vs estimated in ANALYZE mode', () => {
    const scan = new SeqScanNode('users', { estimatedRows: 100, estimatedCost: 10 });
    scan.setActuals(95, 2.5);
    
    const html = planToHTML(scan, { analyze: true });
    assert.ok(html.includes('actual rows: 95'));
    assert.ok(html.includes('ANALYZE'));
  });

  it('shows accuracy bar for overestimates', () => {
    const scan = new SeqScanNode('t', { estimatedRows: 100, estimatedCost: 10 });
    scan.setActuals(500, 5.0); // 5x more actual than estimated
    
    const html = planToHTML(scan, { analyze: true });
    // Red bar for large overestimate
    assert.ok(html.includes('#F44336') || html.includes('#FF9800'), 'Should show warning color for bad estimate');
  });

  it('shows accuracy bar for good estimates', () => {
    const scan = new SeqScanNode('t', { estimatedRows: 100, estimatedCost: 10 });
    scan.setActuals(98, 5.0); // Very close
    
    const html = planToHTML(scan, { analyze: true });
    assert.ok(html.includes('#4CAF50'), 'Should show green for good estimate');
  });

  it('generates HTML for complex multi-level plan', () => {
    const ordersScan = new SeqScanNode('orders', { estimatedRows: 5000, estimatedCost: 85 });
    const usersScan = new SeqScanNode('users', { estimatedRows: 500, estimatedCost: 25 });
    usersScan.filter = 'active = 1';
    
    const hash = new HashNode({ estimatedRows: 350 });
    hash.addChild(usersScan);
    
    const join = new HashJoinNode('INNER', 'o.user_id = u.id', { estimatedRows: 3500, estimatedCost: 120 });
    join.addChild(ordersScan);
    join.addChild(hash);
    
    const sort = new SortNode([{ column: 'total', direction: 'DESC' }], { estimatedRows: 3500, estimatedCost: 160 });
    sort.addChild(join);
    
    const limit = new LimitNode(10, { estimatedRows: 10, estimatedCost: 160.1 });
    limit.addChild(sort);
    
    const html = planToHTML(limit);
    assert.ok(html.includes('Limit'));
    assert.ok(html.includes('Sort'));
    assert.ok(html.includes('Hash Join'));
    assert.ok(html.includes('orders'));
    assert.ok(html.includes('users'));
    assert.ok(html.includes('active = 1'));
    // Multiple path elements for edges
    const pathCount = (html.match(/<path/g) || []).length;
    assert.ok(pathCount >= 3, `Expected 3+ edges, got ${pathCount}`);
  });

  it('escapes XML special characters', () => {
    const scan = new SeqScanNode('t', { estimatedRows: 10 });
    scan.filter = 'val > 5 AND val < 10';
    const html = planToHTML(scan);
    assert.ok(html.includes('&gt;') || html.includes('&lt;'), 'Should escape angle brackets');
    assert.ok(!html.includes('< 10') || html.includes('&lt;'), 'Should not have unescaped < in SVG');
  });
});

describe('EXPLAIN (FORMAT HTML) integration', () => {
  it('returns HTML from EXPLAIN (FORMAT HTML)', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)');
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO t VALUES (${i}, ${i})`);
    
    const result = db.execute('EXPLAIN (FORMAT HTML) SELECT * FROM t WHERE val > 25');
    assert.ok(result.html);
    assert.ok(result.html.includes('<!DOCTYPE html>'));
    assert.ok(result.html.includes('<svg'));
    assert.ok(result.html.includes('Seq Scan'));
  });

  it('HTML plan for JOIN query', () => {
    const db = new Database();
    db.execute('CREATE TABLE a (id INTEGER PRIMARY KEY, val TEXT)');
    db.execute('CREATE TABLE b (id INTEGER PRIMARY KEY, a_id INTEGER)');
    for (let i = 1; i <= 20; i++) db.execute(`INSERT INTO a VALUES (${i}, 'v${i}')`);
    for (let i = 1; i <= 50; i++) db.execute(`INSERT INTO b VALUES (${i}, ${1 + i % 20})`);
    
    const result = db.execute('EXPLAIN (FORMAT HTML) SELECT * FROM a JOIN b ON a.id = b.a_id');
    assert.ok(result.html);
    assert.ok(result.html.includes('Hash Join') || result.html.includes('Nested Loop'));
    assert.ok(result.html.includes('Seq Scan'));
  });
});
