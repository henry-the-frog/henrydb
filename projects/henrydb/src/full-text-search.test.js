// full-text-search.test.js
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { TSVector, TSQuery, tsRank, TextSearchIndex } from './full-text-search.js';

describe('TSVector', () => {
  test('creates vector from text', () => {
    const vec = TSVector.fromText('The quick brown fox jumps over the lazy dog');
    assert.ok(vec.lexemes.has('quick'));
    assert.ok(vec.lexemes.has('fox'));
    assert.ok(!vec.lexemes.has('the')); // stop word
    assert.ok(!vec.lexemes.has('over')); // stop word
  });

  test('stems words', () => {
    const vec = TSVector.fromText('running quickly beautiful happiness');
    assert.ok(vec.lexemes.has('runn')); // running → runn
    assert.ok(vec.lexemes.has('quick')); // quickly → quick
    assert.ok(vec.lexemes.has('beauti')); // beautiful → beauti
    assert.ok(vec.lexemes.has('happi')); // happiness → happi
  });

  test('tracks positions', () => {
    const vec = TSVector.fromText('cat sat on the mat cat');
    const catData = vec.lexemes.get('cat');
    assert.ok(catData.positions.length >= 2);
  });

  test('concat merges vectors', () => {
    const v1 = TSVector.fromText('hello world');
    const v2 = TSVector.fromText('goodbye world');
    const merged = v1.concat(v2);
    assert.ok(merged.lexemes.has('hello'));
    assert.ok(merged.lexemes.has('goodbye'));
    // 'world' should have positions from both
    assert.ok(merged.lexemes.get('world').positions.length >= 2);
  });

  test('toString format', () => {
    const vec = TSVector.fromText('cat dog');
    const str = vec.toString();
    assert.ok(str.includes("'cat'"));
    assert.ok(str.includes("'dog'"));
  });
});

describe('TSQuery', () => {
  test('simple term match', () => {
    const vec = TSVector.fromText('The quick brown fox');
    const query = TSQuery.parse('fox');
    assert.ok(vec.matches(query));
  });

  test('AND query', () => {
    const vec = TSVector.fromText('The quick brown fox');
    assert.ok(vec.matches(TSQuery.parse('quick & fox')));
    assert.ok(!vec.matches(TSQuery.parse('quick & cat')));
  });

  test('OR query', () => {
    const vec = TSVector.fromText('The quick brown fox');
    assert.ok(vec.matches(TSQuery.parse('fox | cat')));
    assert.ok(!vec.matches(TSQuery.parse('cat | dog')));
  });

  test('NOT query', () => {
    const vec = TSVector.fromText('The quick brown fox');
    assert.ok(vec.matches(TSQuery.parse('fox & !cat')));
    assert.ok(!vec.matches(TSQuery.parse('fox & !quick')));
  });

  test('phrase query (<->)', () => {
    const vec = TSVector.fromText('big brown cat sat down');
    assert.ok(vec.matches(TSQuery.parse('brown <-> cat')));
    assert.ok(!vec.matches(TSQuery.parse('big <-> cat'))); // Not adjacent
  });

  test('fromPlainText ANDs all words', () => {
    const vec = TSVector.fromText('The database handles queries efficiently');
    const query = TSQuery.fromPlainText('database queries');
    assert.ok(vec.matches(query));

    const query2 = TSQuery.fromPlainText('database missing');
    assert.ok(!vec.matches(query2));
  });

  test('complex boolean query', () => {
    const vec = TSVector.fromText('PostgreSQL is a powerful relational database');
    assert.ok(vec.matches(TSQuery.parse('(postgresql | mysql) & database')));
    assert.ok(!vec.matches(TSQuery.parse('mysql & !postgresql')));
  });
});

describe('tsRank', () => {
  test('higher rank for more term occurrences', () => {
    const vec1 = TSVector.fromText('cat');
    const vec2 = TSVector.fromText('cat cat cat cat cat');
    const query = TSQuery.parse('cat');
    
    assert.ok(tsRank(vec2, query) > tsRank(vec1, query));
  });

  test('matching docs rank higher than zero', () => {
    const vec = TSVector.fromText('database query optimization');
    const query = TSQuery.parse('database');
    assert.ok(tsRank(vec, query) > 0);
  });

  test('non-matching docs rank zero', () => {
    const vec = TSVector.fromText('database query optimization');
    const query = TSQuery.parse('missing');
    assert.equal(tsRank(vec, query), 0);
  });

  test('AND query ranks sum of terms', () => {
    const vec = TSVector.fromText('fast database query');
    const q1 = TSQuery.parse('database');
    const q2 = TSQuery.parse('database & query');
    assert.ok(tsRank(vec, q2) > tsRank(vec, q1));
  });
});

describe('TextSearchIndex', () => {
  test('add and search documents', () => {
    const idx = new TextSearchIndex();
    idx.addDocument(1, TSVector.fromText('PostgreSQL is a powerful relational database'));
    idx.addDocument(2, TSVector.fromText('MongoDB is a document database'));
    idx.addDocument(3, TSVector.fromText('Redis is an in-memory cache'));

    const results = idx.search(TSQuery.parse('database'));
    assert.equal(results.length, 2);
    assert.ok(results.some(r => r.docId === 1));
    assert.ok(results.some(r => r.docId === 2));
  });

  test('AND search narrows results', () => {
    const idx = new TextSearchIndex();
    idx.addDocument(1, TSVector.fromText('relational database management system'));
    idx.addDocument(2, TSVector.fromText('document database with JSON'));
    idx.addDocument(3, TSVector.fromText('relational algebra theory'));

    const results = idx.search(TSQuery.parse('relational & database'));
    assert.equal(results.length, 1);
    assert.equal(results[0].docId, 1);
  });

  test('OR search widens results', () => {
    const idx = new TextSearchIndex();
    idx.addDocument(1, TSVector.fromText('PostgreSQL database'));
    idx.addDocument(2, TSVector.fromText('Redis cache'));
    idx.addDocument(3, TSVector.fromText('MongoDB database'));

    const results = idx.search(TSQuery.parse('postgresql | redis'));
    assert.equal(results.length, 2);
  });

  test('results sorted by rank', () => {
    const idx = new TextSearchIndex();
    idx.addDocument(1, TSVector.fromText('database'));
    idx.addDocument(2, TSVector.fromText('database database database performance'));
    idx.addDocument(3, TSVector.fromText('database management'));

    const results = idx.search(TSQuery.parse('database'));
    assert.equal(results[0].docId, 2); // Most occurrences
    assert.ok(results[0].rank >= results[1].rank);
  });

  test('remove document', () => {
    const idx = new TextSearchIndex();
    idx.addDocument(1, TSVector.fromText('cat'));
    idx.addDocument(2, TSVector.fromText('dog'));
    
    idx.removeDocument(1);
    const results = idx.search(TSQuery.parse('cat'));
    assert.equal(results.length, 0);
  });

  test('search with limit', () => {
    const idx = new TextSearchIndex();
    for (let i = 0; i < 20; i++) {
      idx.addDocument(i, TSVector.fromText(`document number ${i} about database systems`));
    }

    const results = idx.search(TSQuery.parse('database'), { limit: 5 });
    assert.equal(results.length, 5);
  });

  test('getStats', () => {
    const idx = new TextSearchIndex();
    idx.addDocument(1, TSVector.fromText('hello world'));
    idx.addDocument(2, TSVector.fromText('goodbye world'));

    const stats = idx.getStats();
    assert.equal(stats.documents, 2);
    assert.ok(stats.uniqueLexemes >= 2);
  });

  test('empty query returns no results', () => {
    const idx = new TextSearchIndex();
    idx.addDocument(1, TSVector.fromText('hello world'));
    const results = idx.search(TSQuery.parse(''));
    assert.equal(results.length, 0);
  });
});
