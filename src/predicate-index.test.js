// predicate-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { PredicateIndex, PersistentBPTree } from './predicate-index.js';

describe('PredicateIndex', () => {
  it('basic predicate query', () => {
    const idx = new PredicateIndex();
    idx.addPredicate('isAdult', r => r.age >= 18);
    idx.indexRow(0, { name: 'Alice', age: 25 });
    idx.indexRow(1, { name: 'Bob', age: 15 });
    idx.indexRow(2, { name: 'Charlie', age: 30 });
    
    const adults = idx.query('isAdult');
    assert.deepEqual(adults, new Set([0, 2]));
  });

  it('AND predicates', () => {
    const idx = new PredicateIndex();
    idx.addPredicate('isAdult', r => r.age >= 18);
    idx.addPredicate('isEng', r => r.dept === 'eng');
    
    idx.indexRow(0, { age: 25, dept: 'eng' });
    idx.indexRow(1, { age: 15, dept: 'eng' });
    idx.indexRow(2, { age: 30, dept: 'hr' });
    
    const result = idx.and('isAdult', 'isEng');
    assert.deepEqual(result, new Set([0]));
  });

  it('OR predicates', () => {
    const idx = new PredicateIndex();
    idx.addPredicate('isAdult', r => r.age >= 18);
    idx.addPredicate('isEng', r => r.dept === 'eng');
    
    idx.indexRow(0, { age: 25, dept: 'eng' });
    idx.indexRow(1, { age: 15, dept: 'hr' });
    
    const result = idx.or('isAdult', 'isEng');
    assert.deepEqual(result, new Set([0])); // Only 0 matches either
  });

  it('negation query', () => {
    const idx = new PredicateIndex();
    idx.addPredicate('expensive', r => r.price > 100);
    idx.indexRow(0, { price: 150 });
    idx.indexRow(1, { price: 50 });
    
    const cheap = idx.query('expensive', false);
    assert.deepEqual(cheap, new Set([1]));
  });
});

describe('PersistentBPTree', () => {
  it('insert and search', () => {
    const tree = new PersistentBPTree(4);
    tree.insert(5, 'five').insert(3, 'three').insert(7, 'seven');
    assert.equal(tree.search(5), 'five');
    assert.equal(tree.search(3), 'three');
  });

  it('versions preserved', () => {
    const tree = new PersistentBPTree(4);
    tree.insert(1, 'one');
    const v1 = tree.version;
    tree.insert(2, 'two');
    const v2 = tree.version;
    
    assert.equal(tree.searchAt(1, v1), 'one');
    assert.equal(tree.searchAt(2, v1), undefined); // Not in v1
    assert.equal(tree.searchAt(2, v2), 'two'); // In v2
  });

  it('many inserts', () => {
    const tree = new PersistentBPTree(4);
    for (let i = 0; i < 50; i++) tree.insert(i, `v${i}`);
    for (let i = 0; i < 50; i++) assert.equal(tree.search(i), `v${i}`);
  });

  it('update preserves old version', () => {
    const tree = new PersistentBPTree(4);
    tree.insert(1, 'old');
    const oldVersion = tree.version;
    tree.insert(1, 'new');
    
    assert.equal(tree.searchAt(1, oldVersion), 'old');
    assert.equal(tree.search(1), 'new');
  });

  it('version count grows', () => {
    const tree = new PersistentBPTree(4);
    tree.insert(1, 'a').insert(2, 'b').insert(3, 'c');
    assert.ok(tree.versionCount >= 3);
  });
});
