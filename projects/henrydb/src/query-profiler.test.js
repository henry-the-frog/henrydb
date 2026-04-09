// query-profiler.test.js — Tests for query profiling engine
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { QueryProfiler, profileQuery } from './query-profiler.js';
import { Database } from './db.js';

let db;

describe('QueryProfiler', () => {
  test('basic profiling: begin/end node', () => {
    const profiler = new QueryProfiler();
    
    const node = profiler.beginNode('SEQ_SCAN', { table: 'users' });
    node.addRows(100);
    node.addPageRead(5);
    profiler.endNode();
    
    const profile = profiler.getProfile();
    assert.equal(profile.plan.operation, 'SEQ_SCAN');
    assert.equal(profile.plan.stats.rowsActual, 100);
    assert.equal(profile.plan.stats.pagesRead, 5);
    assert.ok(profile.plan.stats.totalTimeMs >= 0);
  });

  test('nested nodes track parent-child relationship', () => {
    const profiler = new QueryProfiler();
    
    profiler.beginNode('SORT', { key: 'name' });
    profiler.beginNode('SEQ_SCAN', { table: 'users' });
    profiler.currentNode().addRows(50);
    profiler.endNode();
    profiler.endNode();
    
    const profile = profiler.getProfile();
    assert.equal(profile.plan.operation, 'SORT');
    assert.equal(profile.plan.children.length, 1);
    assert.equal(profile.plan.children[0].operation, 'SEQ_SCAN');
    assert.equal(profile.plan.children[0].stats.rowsActual, 50);
  });

  test('self time excludes children time', async () => {
    const profiler = new QueryProfiler();
    
    profiler.beginNode('HASH_JOIN');
    
    profiler.beginNode('SEQ_SCAN', { table: 'left' });
    await new Promise(r => setTimeout(r, 10));
    profiler.endNode();
    
    profiler.beginNode('SEQ_SCAN', { table: 'right' });
    await new Promise(r => setTimeout(r, 10));
    profiler.endNode();
    
    profiler.endNode();
    
    const profile = profiler.getProfile();
    const join = profile.plan;
    assert.ok(join.stats.selfTimeMs < join.stats.totalTimeMs);
    assert.equal(join.children.length, 2);
  });

  test('I/O counters', () => {
    const profiler = new QueryProfiler();
    
    const node = profiler.beginNode('INDEX_SCAN', { index: 'idx_users_email' });
    node.addPageRead(3);
    node.addPageRead(2);
    node.addIndexLookup(5);
    node.addPageWrite(1);
    profiler.endNode();
    
    const profile = profiler.getProfile();
    assert.equal(profile.plan.stats.pagesRead, 5);
    assert.equal(profile.plan.stats.indexLookups, 5);
    assert.equal(profile.plan.stats.pagesWritten, 1);
  });

  test('memory tracking', () => {
    const profiler = new QueryProfiler();
    
    const node = profiler.beginNode('HASH_AGGREGATE');
    node.setMemory(1024);
    node.setMemory(2048);
    node.setMemory(512); // Peak should remain 2048
    profiler.endNode();
    
    const profile = profiler.getProfile();
    assert.equal(profile.plan.stats.peakMemoryBytes, 2048);
  });

  test('loop counting', () => {
    const profiler = new QueryProfiler();
    
    const outer = profiler.beginNode('NESTED_LOOP');
    const inner = profiler.beginNode('INDEX_SCAN');
    
    // Simulate multiple loop iterations
    for (let i = 0; i < 5; i++) {
      inner.start();
      inner.addRows(10);
      inner.end();
    }
    
    profiler.endNode();
    profiler.endNode();
    
    const profile = profiler.getProfile();
    const scan = profile.plan.children[0];
    assert.equal(scan.stats.loops, 6); // 1 initial + 5 additional
    assert.equal(scan.stats.rowsActual, 50);
    assert.ok(scan.stats.rowsPerLoop > 0);
  });

  test('summary aggregates across all nodes', () => {
    const profiler = new QueryProfiler();
    
    profiler.beginNode('HASH_JOIN');
    
    const left = profiler.beginNode('SEQ_SCAN');
    left.addRows(100);
    left.addPageRead(10);
    profiler.endNode();
    
    const right = profiler.beginNode('INDEX_SCAN');
    right.addRows(50);
    right.addPageRead(5);
    right.addIndexLookup(50);
    profiler.endNode();
    
    profiler.endNode();
    
    const profile = profiler.getProfile();
    assert.equal(profile.summary.totalRows, 150);
    assert.equal(profile.summary.totalPagesRead, 15);
    assert.equal(profile.summary.totalIndexLookups, 50);
    assert.equal(profile.summary.nodeCount, 3);
  });

  test('formatReport produces readable output', () => {
    const profiler = new QueryProfiler();
    
    const root = profiler.beginNode('SORT', { key: 'name', estimatedRows: 100 });
    root.addRows(95);
    
    const scan = profiler.beginNode('SEQ_SCAN', { table: 'users', estimatedRows: 100 });
    scan.addRows(95);
    scan.addPageRead(10);
    profiler.endNode();
    
    profiler.endNode();
    
    const report = profiler.formatReport({ showEstimates: true, showHotPath: true });
    assert.ok(report.includes('SORT'));
    assert.ok(report.includes('SEQ_SCAN'));
    assert.ok(report.includes('rows=95'));
    assert.ok(report.includes('Summary'));
    assert.ok(report.includes('Hot Path'));
  });

  test('hot path identifies most expensive path', () => {
    const profiler = new QueryProfiler();
    
    profiler.beginNode('MERGE_JOIN');
    
    const left = profiler.beginNode('SORT');
    left.stats.selfTimeMs = 50; // Manually set for test
    profiler.endNode();
    
    const right = profiler.beginNode('INDEX_SCAN');
    right.stats.selfTimeMs = 10;
    profiler.endNode();
    
    profiler.endNode();
    
    const report = profiler.formatReport({ showHotPath: true });
    assert.ok(report.includes('Hot Path'));
    assert.ok(report.includes('SORT'));
  });

  test('estimate accuracy ratio', () => {
    const profiler = new QueryProfiler();
    
    const node = profiler.beginNode('SEQ_SCAN', { estimatedRows: 100 });
    node.addRows(250);
    profiler.endNode();
    
    const report = profiler.formatReport({ showEstimates: true });
    assert.ok(report.includes('estimated=100'));
    assert.ok(report.includes('ratio=2.50'));
  });

  test('empty profiler returns no data', () => {
    const profiler = new QueryProfiler();
    const profile = profiler.getProfile();
    assert.equal(profile.plan, null);
    assert.equal(profile.summary.nodeCount, 0);
  });
});

describe('profileQuery (integrated)', () => {
  beforeEach(() => {
    db = new Database();
    db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)');
    for (let i = 1; i <= 100; i++) {
      db.execute(`INSERT INTO products VALUES (${i}, 'Product ${i}', ${i * 10})`);
    }
  });

  test('profiles a simple SELECT', () => {
    const { result, profile, report } = profileQuery(db, 'SELECT * FROM products WHERE price > 500');
    assert.ok(result.rows.length > 0);
    assert.ok(profile.plan);
    assert.ok(profile.plan.stats.rowsActual > 0);
    assert.ok(report.includes('SCAN'));
  });

  test('profiles a GROUP BY query', () => {
    const { profile } = profileQuery(db, 'SELECT COUNT(*) FROM products GROUP BY price > 500');
    assert.ok(profile.plan);
    assert.equal(profile.plan.operation, 'AGGREGATE');
  });

  test('profiles an ORDER BY query', () => {
    const { profile } = profileQuery(db, 'SELECT * FROM products ORDER BY price DESC');
    assert.ok(profile.plan);
    assert.equal(profile.plan.operation, 'SORT');
  });

  test('profile report includes timing', () => {
    const { report } = profileQuery(db, 'SELECT * FROM products');
    assert.ok(report.includes('ms'));
    assert.ok(report.includes('Total time'));
  });
});
