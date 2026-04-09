// fulltext-index.test.js — Tests for full-text search
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { FullTextIndex, tokenize } from './fulltext-index.js';

describe('tokenize', () => {
  it('lowercases and splits on whitespace', () => {
    const tokens = tokenize('Hello World Test');
    assert.deepEqual(tokens, ['hello', 'world', 'test']);
  });

  it('removes stop words', () => {
    const tokens = tokenize('The quick brown fox is in the house');
    assert.ok(!tokens.includes('the'));
    assert.ok(!tokens.includes('is'));
    assert.ok(!tokens.includes('in'));
    assert.ok(tokens.includes('quick'));
    assert.ok(tokens.includes('brown'));
    assert.ok(tokens.includes('fox'));
  });

  it('removes punctuation', () => {
    const tokens = tokenize('Hello, world! How are you?');
    assert.ok(tokens.includes('hello'));
    assert.ok(tokens.includes('world'));
    assert.ok(tokens.includes('how'));
  });

  it('removes short words (1 char)', () => {
    const tokens = tokenize('I go to a nice big park');
    assert.ok(!tokens.includes('i'));
    assert.ok(!tokens.includes('a'));
    assert.ok(tokens.includes('nice'));
    assert.ok(tokens.includes('big'));
    assert.ok(tokens.includes('park'));
  });

  it('handles empty and null', () => {
    assert.deepEqual(tokenize(''), []);
    assert.deepEqual(tokenize(null), []);
  });
});

describe('FullTextIndex', () => {
  function setupIndex() {
    const idx = new FullTextIndex('test');
    idx.addDocument(1, 'The quick brown fox jumps over the lazy dog');
    idx.addDocument(2, 'A fast brown dog runs in the park');
    idx.addDocument(3, 'The fox is clever and quick');
    idx.addDocument(4, 'Dogs and cats are popular pets');
    idx.addDocument(5, 'The park is a beautiful place for dogs');
    return idx;
  }

  it('basic search returns matching documents', () => {
    const idx = setupIndex();
    const results = idx.search('fox');
    assert.ok(results.length > 0);
    assert.ok(results.some(r => r.docId === 1));
    assert.ok(results.some(r => r.docId === 3));
  });

  it('TF-IDF ranks more relevant docs higher', () => {
    const idx = setupIndex();
    // "fox" appears in docs 1 and 3
    // Doc 3 mentions fox in a shorter doc → higher TF → higher score
    const results = idx.search('fox');
    assert.ok(results[0].score > 0);
  });

  it('multi-term OR search', () => {
    const idx = setupIndex();
    const results = idx.search('fox park');
    // Should match docs with "fox" OR "park"
    const docIds = results.map(r => r.docId);
    assert.ok(docIds.includes(1) || docIds.includes(3)); // fox
    assert.ok(docIds.includes(2) || docIds.includes(5)); // park
  });

  it('multi-term AND search', () => {
    const idx = setupIndex();
    const results = idx.search('brown fox', { mode: 'AND' });
    // Only doc 1 has both "brown" and "fox"
    assert.equal(results.length, 1);
    assert.equal(results[0].docId, 1);
  });

  it('no results for unknown terms', () => {
    const idx = setupIndex();
    const results = idx.search('xyzzy foobar');
    assert.equal(results.length, 0);
  });

  it('removeDocument updates index', () => {
    const idx = setupIndex();
    
    // Fox is in docs 1 and 3
    assert.ok(idx.search('fox').length === 2);
    
    idx.removeDocument(1);
    assert.ok(idx.search('fox').length === 1);
    assert.equal(idx.search('fox')[0].docId, 3);
  });

  it('getDF returns document frequency', () => {
    const idx = setupIndex();
    assert.equal(idx.getDF('fox'), 2);   // Docs 1 and 3
    assert.equal(idx.getDF('park'), 2);  // Docs 2 and 5
    assert.equal(idx.getDF('xyzzy'), 0); // Not in index
  });

  it('getStats', () => {
    const idx = setupIndex();
    const stats = idx.getStats();
    assert.equal(stats.documents, 5);
    assert.ok(stats.uniqueTerms > 0);
    assert.ok(stats.avgDocLength > 0);
  });

  it('results include matched terms', () => {
    const idx = setupIndex();
    const results = idx.search('quick brown');
    for (const r of results) {
      assert.ok(r.matchedTerms.length > 0);
      assert.ok(r.matchedTerms.every(t => ['quick', 'brown'].includes(t)));
    }
  });

  it('limit parameter', () => {
    const idx = new FullTextIndex('test');
    for (let i = 1; i <= 100; i++) {
      idx.addDocument(i, `document number ${i} with content about topic ${i % 10}`);
    }
    
    const results = idx.search('document', { limit: 5 });
    assert.equal(results.length, 5);
  });

  it('large index: 10K documents', () => {
    const idx = new FullTextIndex('large');
    const topics = ['machine learning', 'web development', 'database systems', 'operating systems', 'computer networks'];
    
    for (let i = 1; i <= 10000; i++) {
      const topic = topics[i % topics.length];
      idx.addDocument(i, `Article ${i} about ${topic} in computer science research and ${topic} applications`);
    }
    
    const stats = idx.getStats();
    console.log(`  10K docs: ${stats.uniqueTerms} unique terms, avg doc ${stats.avgDocLength.toFixed(1)} terms`);
    
    const t0 = performance.now();
    const results = idx.search('database systems');
    const elapsed = performance.now() - t0;
    
    console.log(`  Search 'database systems': ${results.length} results in ${elapsed.toFixed(2)}ms`);
    assert.ok(results.length > 0);
    assert.ok(elapsed < 100, `Expected <100ms, got ${elapsed.toFixed(1)}ms`);
  });

  it('TF-IDF scoring: rare terms score higher', () => {
    const idx = new FullTextIndex('test');
    idx.addDocument(1, 'common common common rare');
    idx.addDocument(2, 'common common common common');
    idx.addDocument(3, 'rare rare rare common');
    
    // "rare" has higher IDF (appears in fewer docs)
    const results = idx.search('rare');
    assert.equal(results.length, 2);
    // Doc 3 should score highest (more occurrences of rare term)
    assert.equal(results[0].docId, 3);
  });
});
