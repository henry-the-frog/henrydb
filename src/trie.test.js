// trie.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Trie } from './trie.js';

describe('Trie', () => {
  it('insert/get', () => {
    const t = new Trie();
    t.insert('hello', 1); t.insert('help', 2); t.insert('world', 3);
    assert.equal(t.get('hello'), 1);
    assert.equal(t.get('help'), 2);
    assert.equal(t.get('hel'), undefined);
  });

  it('prefix search', () => {
    const t = new Trie();
    t.insert('apple', 1); t.insert('app', 2); t.insert('application', 3); t.insert('banana', 4);
    const results = t.prefixSearch('app');
    assert.equal(results.length, 3);
    assert.ok(results.some(r => r.key === 'app'));
  });

  it('autocomplete', () => {
    const t = new Trie();
    ['database', 'data', 'datastore', 'datum', 'dart'].forEach((w, i) => t.insert(w, i));
    const suggestions = t.autocomplete('dat');
    assert.ok(suggestions.includes('data'));
    assert.ok(suggestions.includes('database'));
    assert.ok(!suggestions.includes('dart'));
  });

  it('delete', () => {
    const t = new Trie();
    t.insert('test', 1);
    assert.ok(t.delete('test'));
    assert.equal(t.get('test'), undefined);
  });

  it('has', () => {
    const t = new Trie();
    t.insert('abc', 1);
    assert.ok(t.has('abc'));
    assert.ok(!t.has('ab'));
  });

  it('1000 keys', () => {
    const t = new Trie();
    for (let i = 0; i < 1000; i++) t.insert(`key_${i}`, i);
    assert.equal(t.size, 1000);
    assert.equal(t.get('key_500'), 500);
  });
});
