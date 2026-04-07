// composite-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Database } from './db.js';
import { CompositeKey, makeCompositeKey } from './composite-key.js';

describe('Composite Indexes', () => {
  it('creates multi-column index', () => {
    const db = new Database();
    db.execute('CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b INT)');
    db.execute("INSERT INTO t VALUES (1, 'x', 10)");
    db.execute("INSERT INTO t VALUES (2, 'y', 20)");
    db.execute('CREATE INDEX idx_ab ON t (a, b)');
    
    const table = db.tables.get('t');
    assert.ok(table.indexes.has('a,b'));
  });

  it('CompositeKey preserves sort order', () => {
    const k1 = makeCompositeKey([1, 'a']);
    const k2 = makeCompositeKey([1, 'b']);
    const k3 = makeCompositeKey([2, 'a']);
    
    assert.ok(k1 < k2, '(1,a) < (1,b)');
    assert.ok(k2 < k3, '(1,b) < (2,a)');
    assert.ok(k1 < k3, '(1,a) < (2,a)');
  });

  it('CompositeKey handles negative numbers', () => {
    const k1 = makeCompositeKey([-5, 'a']);
    const k2 = makeCompositeKey([0, 'a']);
    const k3 = makeCompositeKey([5, 'a']);
    
    assert.ok(k1 < k2, '-5 < 0');
    assert.ok(k2 < k3, '0 < 5');
  });

  it('prefix matching works', () => {
    const k = makeCompositeKey([1, 'hello', 42]);
    assert.ok(k.startsWith([1]));
    assert.ok(k.startsWith([1, 'hello']));
    assert.ok(k.startsWith([1, 'hello', 42]));
    assert.ok(!k.startsWith([2]));
    assert.ok(!k.startsWith([1, 'world']));
  });
});
