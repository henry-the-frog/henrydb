// vector.test.js — Vector similarity search tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { VectorStore, cosineSimilarity, euclideanDistance } from './vector.js';

describe('Vector Math', () => {
  it('cosine similarity of identical vectors is 1', () => {
    assert.ok(Math.abs(cosineSimilarity([1, 2, 3], [1, 2, 3]) - 1) < 0.001);
  });

  it('cosine similarity of orthogonal vectors is 0', () => {
    assert.ok(Math.abs(cosineSimilarity([1, 0], [0, 1])) < 0.001);
  });

  it('euclidean distance of same point is 0', () => {
    assert.equal(euclideanDistance([1, 2], [1, 2]), 0);
  });

  it('euclidean distance is correct', () => {
    assert.ok(Math.abs(euclideanDistance([0, 0], [3, 4]) - 5) < 0.001);
  });
});

describe('VectorStore', () => {
  it('stores and retrieves nearest neighbors', () => {
    const store = new VectorStore(3);
    store.add('a', [1, 0, 0], { label: 'x-axis' });
    store.add('b', [0, 1, 0], { label: 'y-axis' });
    store.add('c', [0, 0, 1], { label: 'z-axis' });
    store.add('d', [0.9, 0.1, 0], { label: 'near-x' });
    
    const results = store.nearestCosine([1, 0, 0], 2);
    assert.equal(results.length, 2);
    assert.equal(results[0].id, 'a'); // Exact match
    assert.equal(results[1].id, 'd'); // Close to x-axis
  });

  it('euclidean nearest neighbor', () => {
    const store = new VectorStore(2);
    store.add('origin', [0, 0]);
    store.add('close', [1, 0]);
    store.add('far', [10, 10]);
    
    const results = store.nearestEuclidean([0.9, 0], 2);
    assert.equal(results[0].id, 'close'); // 0.1 distance
  });

  it('filtered search', () => {
    const store = new VectorStore(2);
    store.add('a', [1, 0], { category: 'x' });
    store.add('b', [0, 1], { category: 'y' });
    store.add('c', [0.9, 0], { category: 'x' });
    
    const results = store.filteredSearch([1, 0], m => m.category === 'x', 5);
    assert.equal(results.length, 2);
    assert.ok(results.every(r => r.metadata.category === 'x'));
  });

  it('high-dimensional vectors', () => {
    const store = new VectorStore(128);
    // Add random vectors
    for (let i = 0; i < 100; i++) {
      const vec = Array.from({ length: 128 }, () => Math.random());
      store.add(`doc_${i}`, vec, { index: i });
    }
    
    const query = Array.from({ length: 128 }, () => Math.random());
    const results = store.nearestCosine(query, 5);
    assert.equal(results.length, 5);
    // Results should be sorted by similarity descending
    for (let i = 1; i < results.length; i++) {
      assert.ok(results[i - 1].similarity >= results[i].similarity);
    }
  });

  it('semantic search simulation', () => {
    const store = new VectorStore(4);
    // Simulate word embeddings
    store.add('king', [0.9, 0.1, 0.8, 0.2], { word: 'king' });
    store.add('queen', [0.9, 0.1, 0.2, 0.8], { word: 'queen' });
    store.add('man', [0.1, 0.9, 0.8, 0.2], { word: 'man' });
    store.add('woman', [0.1, 0.9, 0.2, 0.8], { word: 'woman' });
    store.add('apple', [0.0, 0.0, 0.5, 0.5], { word: 'apple' });
    
    // "king" should be most similar to "queen" (both royal)
    const results = store.nearestCosine([0.9, 0.1, 0.8, 0.2], 2);
    assert.equal(results[0].id, 'king');
  });
});
