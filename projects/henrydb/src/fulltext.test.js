// fulltext.test.js — Full-text search tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { tokenize, tokenizeWithPositions, InvertedIndex, STOP_WORDS } from './fulltext.js';

describe('Tokenizer', () => {
  it('lowercases and splits on non-alphanumeric', () => {
    const tokens = tokenize('Hello, World! This is a TEST.');
    assert.ok(tokens.includes('hello'));
    assert.ok(tokens.includes('world'));
    assert.ok(tokens.includes('test'));
  });

  it('removes stop words', () => {
    const tokens = tokenize('the quick brown fox is a very fast animal');
    assert.ok(!tokens.includes('the'));
    assert.ok(!tokens.includes('is'));
    assert.ok(!tokens.includes('a'));
    assert.ok(tokens.includes('quick'));
    assert.ok(tokens.includes('brown'));
    assert.ok(tokens.includes('fox'));
  });

  it('removes short tokens (< 2 chars)', () => {
    const tokens = tokenize('I am a b c go run');
    assert.ok(!tokens.includes('i'));
    assert.ok(!tokens.includes('a'));
    assert.ok(tokens.includes('go'));
    assert.ok(tokens.includes('run'));
  });

  it('tokenizeWithPositions tracks word positions', () => {
    const tokens = tokenizeWithPositions('the quick brown fox');
    assert.equal(tokens[0].term, 'quick');
    assert.equal(tokens[0].position, 1);
    assert.equal(tokens[1].term, 'brown');
    assert.equal(tokens[1].position, 2);
  });

  it('handles null and empty input', () => {
    assert.deepEqual(tokenize(null), []);
    assert.deepEqual(tokenize(''), []);
    assert.deepEqual(tokenize(42), []);
  });
});

describe('Inverted Index', () => {
  it('indexes and searches single term', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'database systems are fascinating');
    idx.addDocument(2, 'web development with javascript');
    idx.addDocument(3, 'database design patterns');

    const results = idx.search('database');
    assert.equal(results.length, 2);
    assert.ok(results.some(p => p.docId === 1));
    assert.ok(results.some(p => p.docId === 3));
  });

  it('returns empty for unknown terms', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'hello world');
    assert.deepEqual(idx.search('nonexistent'), []);
  });

  it('boolean AND search requires all terms', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'fast database systems');
    idx.addDocument(2, 'fast web servers');
    idx.addDocument(3, 'database web applications');

    const results = idx.searchAnd('fast database');
    assert.equal(results.length, 1);
    assert.equal(results[0], 1);
  });

  it('boolean OR search returns any match', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'fast database systems');
    idx.addDocument(2, 'fast web servers');
    idx.addDocument(3, 'slow applications');

    const results = idx.searchOr('fast database');
    assert.equal(results.length, 2); // 1 and 2 (fast), 1 (database)
  });

  it('TF-IDF scoring ranks relevant docs higher', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'database database database systems'); // High TF for "database"
    idx.addDocument(2, 'database systems overview');
    idx.addDocument(3, 'web development guide');

    const results = idx.searchWithScore('database');
    assert.ok(results.length >= 2);
    // Doc 1 should rank higher (more occurrences of "database")
    assert.equal(results[0].docId, 1);
  });

  it('removeDocument updates the index', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'hello world');
    idx.addDocument(2, 'hello there');

    idx.removeDocument(1);
    const results = idx.search('hello');
    assert.equal(results.length, 1);
    assert.equal(results[0].docId, 2);
  });

  it('stats returns index statistics', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'hello world');
    idx.addDocument(2, 'world peace');

    const stats = idx.stats();
    assert.equal(stats.documents, 2);
    assert.ok(stats.terms >= 2); // hello, world, peace
  });

  it('handles positions for phrase proximity', () => {
    const idx = new InvertedIndex('test_idx', 'docs', 'content');
    idx.addDocument(1, 'the quick brown fox jumps over the lazy dog');

    const results = idx.search('quick');
    assert.equal(results.length, 1);
    assert.ok(results[0].positions.includes(1)); // position of "quick"
  });
});
