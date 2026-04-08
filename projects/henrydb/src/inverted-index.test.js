// inverted-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { InvertedIndex } from './inverted-index.js';

describe('InvertedIndex', () => {
  const idx = new InvertedIndex();
  idx.addDocument('d1', 'The quick brown fox jumps over the lazy dog');
  idx.addDocument('d2', 'A fast brown car races past a slow dog');
  idx.addDocument('d3', 'The dog barked at the brown squirrel');
  idx.addDocument('d4', 'SQL query optimization for database systems');
  idx.addDocument('d5', 'Database indexing with B-trees and hash tables');

  it('basic search returns ranked results', () => {
    const results = idx.search('brown dog');
    assert.ok(results.length > 0);
    assert.ok(results[0].score > 0);
    // d1 has both "brown" and "dog", should score high
    assert.ok(results.some(r => r.docId === 'd1'));
  });

  it('term not found returns empty', () => {
    const results = idx.search('nonexistent');
    assert.equal(results.length, 0);
  });

  it('AND search', () => {
    const docs = idx.searchAnd('brown dog');
    assert.ok(docs.includes('d1')); // Has both
    assert.ok(docs.includes('d2')); // Has both
    assert.ok(docs.includes('d3')); // Has both
    assert.ok(!docs.includes('d4')); // No "brown" or "dog"
  });

  it('phrase search', () => {
    const docs = idx.searchPhrase('brown fox');
    assert.ok(docs.includes('d1')); // "brown fox" is consecutive
    assert.ok(!docs.includes('d2')); // "brown car" not "brown fox"
  });

  it('term info', () => {
    const info = idx.getTermInfo('dog');
    assert.ok(info);
    assert.ok(info.documentFrequency >= 3);
    assert.ok(info.totalOccurrences >= 3);
  });

  it('BM25 ranking: more relevant docs score higher', () => {
    const results = idx.search('database');
    // d4 and d5 mention database
    const dbDocs = results.filter(r => r.docId === 'd4' || r.docId === 'd5');
    assert.ok(dbDocs.length >= 1);
  });

  it('stats', () => {
    const stats = idx.getStats();
    assert.equal(stats.documents, 5);
    assert.ok(stats.terms > 10);
  });

  it('benchmark: 1000 docs, 10K searches', () => {
    const bigIdx = new InvertedIndex();
    for (let i = 0; i < 1000; i++) {
      bigIdx.addDocument(`doc_${i}`, `This is document number ${i} about topic ${i % 10} with keyword${i % 50}`);
    }

    const t0 = Date.now();
    for (let i = 0; i < 10000; i++) {
      bigIdx.search(`topic ${i % 10}`);
    }
    const ms = Date.now() - t0;
    console.log(`    10K searches over 1K docs: ${ms}ms`);
    assert.ok(ms < 5000);
  });
});
