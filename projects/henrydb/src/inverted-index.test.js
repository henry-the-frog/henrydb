// inverted-index.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { InvertedIndex, tokenize } from './inverted-index.js';

describe('Tokenizer', () => {
  it('lowercases and splits', () => {
    const tokens = tokenize('Hello World Database');
    assert.ok(tokens.includes('hello'));
    assert.ok(tokens.includes('world'));
    assert.ok(tokens.includes('database'));
  });

  it('removes stop words', () => {
    const tokens = tokenize('the quick brown fox is a great animal');
    assert.ok(!tokens.includes('the'));
    assert.ok(!tokens.includes('is'));
    assert.ok(!tokens.includes('a'));
    assert.ok(tokens.includes('quick'));
  });
});

describe('InvertedIndex — Basic', () => {
  it('indexes and searches documents', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'The quick brown fox jumps over the lazy dog');
    idx.addDocument(2, 'The quick brown fox is very fast');
    idx.addDocument(3, 'The lazy dog sleeps all day');
    
    const results = idx.searchTerm('fox');
    assert.equal(results.length, 2);
    assert.ok(results.some(r => r.docId === 1));
    assert.ok(results.some(r => r.docId === 2));
  });

  it('returns empty for missing terms', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'hello world');
    assert.equal(idx.searchTerm('missing').length, 0);
  });

  it('TF-IDF scoring ranks relevant docs higher', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'database database database indexing');
    idx.addDocument(2, 'database web application');
    idx.addDocument(3, 'cooking recipes food');
    
    const results = idx.searchTerm('database');
    assert.equal(results[0].docId, 1); // Doc 1 has highest TF for "database"
  });
});

describe('InvertedIndex — Boolean Queries', () => {
  const idx = new InvertedIndex();
  idx.addDocument(1, 'database indexing performance optimization');
  idx.addDocument(2, 'database query optimization SQL');
  idx.addDocument(3, 'web application performance monitoring');
  idx.addDocument(4, 'machine learning database analytics');

  it('AND query', () => {
    const results = idx.search('database AND optimization');
    assert.equal(results.length, 2); // docs 1 and 2
    assert.ok(results.every(r => [1, 2].includes(r.docId)));
  });

  it('OR query', () => {
    const results = idx.search('indexing OR analytics');
    assert.equal(results.length, 2); // docs 1 and 4
  });

  it('NOT query', () => {
    const results = idx.search('NOT database');
    assert.equal(results.length, 1); // Only doc 3
    assert.equal(results[0].docId, 3);
  });

  it('multi-word defaults to AND', () => {
    const results = idx.search('database optimization');
    assert.equal(results.length, 2);
  });
});

describe('InvertedIndex — Maintenance', () => {
  it('removeDocument updates index', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'hello world');
    idx.addDocument(2, 'hello universe');
    
    idx.removeDocument(1);
    
    const results = idx.searchTerm('hello');
    assert.equal(results.length, 1);
    assert.equal(results[0].docId, 2);
  });

  it('getStats reports correct values', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'database indexing');
    idx.addDocument(2, 'query optimization');
    
    const stats = idx.getStats();
    assert.equal(stats.documents, 2);
    assert.ok(stats.terms > 0);
  });
});

describe('InvertedIndex — Performance', () => {
  it('indexes and searches 10K documents', () => {
    const idx = new InvertedIndex();
    const words = ['database', 'query', 'index', 'tree', 'hash', 'sort', 'join', 
                   'table', 'column', 'row', 'page', 'buffer', 'cache', 'lock',
                   'transaction', 'commit', 'rollback', 'log', 'recovery', 'checkpoint'];
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) {
      const numWords = 5 + Math.floor(Math.random() * 10);
      const text = Array.from({ length: numWords }, () => words[Math.random() * words.length | 0]).join(' ');
      idx.addDocument(i, text);
    }
    const indexMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 1000; i++) {
      idx.search('database AND query');
    }
    const searchMs = performance.now() - t1;
    
    console.log(`    10K docs indexed: ${indexMs.toFixed(1)}ms`);
    console.log(`    1K searches: ${searchMs.toFixed(1)}ms (${(1000 / searchMs * 1000) | 0} queries/sec)`);
    console.log(`    Stats:`, idx.getStats());
  });
});
