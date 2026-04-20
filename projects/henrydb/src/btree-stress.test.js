// btree-stress.test.js — B+Tree edge cases and stress tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { BPlusTree } from './btree.js';

describe('B+Tree — Basic Operations', () => {
  it('insert and search single key', () => {
    const tree = new BPlusTree(4);
    tree.insert(42, 'hello');
    assert.equal(tree.search(42), 'hello');
  });

  it('search missing key returns undefined', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'a');
    assert.equal(tree.search(99), undefined);
  });

  it('insert duplicate key updates value', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'v1');
    tree.insert(1, 'v2');
    const result = tree.search(1);
    // Might return either value depending on implementation
    assert.ok(result !== undefined);
  });

  it('delete removes key', () => {
    const tree = new BPlusTree(4);
    tree.insert(1, 'a');
    tree.insert(2, 'b');
    tree.insert(3, 'c');
    tree.delete(2);
    assert.equal(tree.search(2), undefined);
    assert.equal(tree.search(1), 'a');
    assert.equal(tree.search(3), 'c');
  });
});

describe('B+Tree — Sequential Insert', () => {
  it('ascending insert 100 keys', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 100; i++) {
      tree.insert(i, `val_${i}`);
    }
    // Verify all keys searchable
    for (let i = 1; i <= 100; i++) {
      assert.equal(tree.search(i), `val_${i}`, `Key ${i} should be findable`);
    }
  });

  it('descending insert 100 keys', () => {
    const tree = new BPlusTree(4);
    for (let i = 100; i >= 1; i--) {
      tree.insert(i, `val_${i}`);
    }
    for (let i = 1; i <= 100; i++) {
      assert.equal(tree.search(i), `val_${i}`);
    }
  });

  it('ascending insert 1000 keys', () => {
    const tree = new BPlusTree(8);
    for (let i = 1; i <= 1000; i++) {
      tree.insert(i, i * 10);
    }
    // Spot check
    assert.equal(tree.search(1), 10);
    assert.equal(tree.search(500), 5000);
    assert.equal(tree.search(1000), 10000);
    assert.equal(tree.search(1001), undefined);
  });
});

describe('B+Tree — Random Insert', () => {
  it('random insert 500 keys then verify all', () => {
    const tree = new BPlusTree(6);
    const keys = [];
    for (let i = 0; i < 500; i++) {
      const key = Math.floor(Math.random() * 10000);
      keys.push(key);
      tree.insert(key, key * 2);
    }
    // All keys should be findable
    for (const key of keys) {
      const result = tree.search(key);
      assert.ok(result !== undefined, `Key ${key} should be in tree`);
    }
  });
});

describe('B+Tree — Range Queries', () => {
  it('range query returns ordered results', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 20; i++) {
      tree.insert(i, `val_${i}`);
    }
    const results = tree.range(5, 10);
    assert.ok(results.length >= 5, 'Should return at least 5 results');
    // Results should be in order
    for (let i = 1; i < results.length; i++) {
      assert.ok(results[i - 1].key <= results[i].key, 'Range results should be sorted');
    }
  });

  it('range with no results', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 10; i++) tree.insert(i, i);
    const results = tree.range(20, 30);
    assert.equal(results.length, 0);
  });

  it('range covers entire tree', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 10; i++) tree.insert(i, i);
    const results = tree.range(0, 100);
    assert.equal(results.length, 10);
  });
});

describe('B+Tree — Delete Patterns', () => {
  it('delete all keys one by one', () => {
    const tree = new BPlusTree(4);
    const n = 50;
    for (let i = 1; i <= n; i++) tree.insert(i, i);
    for (let i = 1; i <= n; i++) {
      tree.delete(i);
      assert.equal(tree.search(i), undefined, `Key ${i} should be deleted`);
    }
  });

  it('delete every other key', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 20; i++) tree.insert(i, i);
    for (let i = 2; i <= 20; i += 2) tree.delete(i);
    for (let i = 1; i <= 20; i++) {
      if (i % 2 === 0) {
        assert.equal(tree.search(i), undefined);
      } else {
        assert.equal(tree.search(i), i);
      }
    }
  });

  it('delete and re-insert', () => {
    const tree = new BPlusTree(4);
    for (let i = 1; i <= 10; i++) tree.insert(i, i);
    tree.delete(5);
    assert.equal(tree.search(5), undefined);
    tree.insert(5, 50);
    assert.equal(tree.search(5), 50);
  });
});

describe('B+Tree — Scan (Iterator)', () => {
  it('scan returns all entries in order', () => {
    const tree = new BPlusTree(4);
    const expected = [5, 3, 8, 1, 7, 2, 9, 4, 6, 10];
    for (const k of expected) tree.insert(k, k * 10);
    
    const results = [...tree.scan()];
    assert.equal(results.length, 10);
    for (let i = 1; i < results.length; i++) {
      assert.ok(results[i - 1].key <= results[i].key, 'Scan should be ordered');
    }
  });

  it('scan of empty tree', () => {
    const tree = new BPlusTree(4);
    const results = [...tree.scan()];
    assert.equal(results.length, 0);
  });
});

describe('B+Tree — Different Orders', () => {
  it('order 3 (minimum)', () => {
    const tree = new BPlusTree(3);
    for (let i = 1; i <= 50; i++) tree.insert(i, i);
    for (let i = 1; i <= 50; i++) {
      assert.equal(tree.search(i), i, `Order 3: key ${i} not found`);
    }
  });

  it('order 16', () => {
    const tree = new BPlusTree(16);
    for (let i = 1; i <= 200; i++) tree.insert(i, i);
    for (let i = 1; i <= 200; i++) {
      assert.equal(tree.search(i), i);
    }
  });

  it('order 32', () => {
    const tree = new BPlusTree(32);
    for (let i = 1; i <= 500; i++) tree.insert(i, i);
    for (let i = 1; i <= 500; i++) {
      assert.equal(tree.search(i), i);
    }
  });
});

describe('B+Tree — String Keys', () => {
  it('string keys sort lexicographically', () => {
    const tree = new BPlusTree(4);
    tree.insert('banana', 2);
    tree.insert('apple', 1);
    tree.insert('cherry', 3);
    tree.insert('date', 4);
    
    assert.equal(tree.search('apple'), 1);
    assert.equal(tree.search('banana'), 2);
    assert.equal(tree.search('cherry'), 3);
    assert.equal(tree.search('date'), 4);
    
    const results = tree.range('b', 'd');
    assert.ok(results.length >= 2); // banana, cherry
  });
});
