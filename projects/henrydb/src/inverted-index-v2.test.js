// inverted-index-v2.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { InvertedIndex } from './inverted-index-v2.js';

describe('InvertedIndex v2', () => {
  it('term search', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'the quick brown fox');
    idx.addDocument(2, 'the lazy dog');
    
    const results = idx.search('the');
    assert.equal(results.length, 2);
  });

  it('phrase search', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'hello world foo bar');
    idx.addDocument(2, 'world hello');
    
    const results = idx.searchPhrase('hello world');
    assert.deepEqual(results, [1]);
  });

  it('term frequency', () => {
    const idx = new InvertedIndex();
    idx.addDocument(1, 'the the the cat');
    const postings = idx.search('the');
    assert.equal(postings[0].tf, 3);
  });
});
