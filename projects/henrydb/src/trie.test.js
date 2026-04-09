// trie.test.js — Tests for Trie prefix tree
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Trie } from './trie.js';

describe('Trie', () => {
  it('insert and get', () => {
    const t = new Trie();
    t.insert('hello', 1);
    t.insert('world', 2);
    assert.equal(t.get('hello'), 1);
    assert.equal(t.get('world'), 2);
    assert.equal(t.get('missing'), undefined);
    assert.equal(t.size, 2);
  });

  it('has and hasPrefix', () => {
    const t = new Trie();
    t.insert('apple');
    t.insert('application');
    
    assert.equal(t.has('apple'), true);
    assert.equal(t.has('app'), false);
    assert.equal(t.hasPrefix('app'), true);
    assert.equal(t.hasPrefix('apx'), false);
  });

  it('prefixSearch', () => {
    const t = new Trie();
    t.insert('apple', 1);
    t.insert('application', 2);
    t.insert('apply', 3);
    t.insert('banana', 4);
    
    const results = t.prefixSearch('app');
    assert.equal(results.length, 3);
    const keys = results.map(r => r.key).sort();
    assert.deepEqual(keys, ['apple', 'application', 'apply']);
  });

  it('autocomplete', () => {
    const t = new Trie();
    const words = ['database', 'data', 'datum', 'date', 'day', 'dog'];
    for (const w of words) t.insert(w);
    
    const completions = t.autocomplete('dat');
    assert.ok(completions.includes('data'));
    assert.ok(completions.includes('database'));
    assert.ok(completions.includes('datum'));
    assert.ok(completions.includes('date'));
    assert.ok(!completions.includes('dog'));
  });

  it('delete', () => {
    const t = new Trie();
    t.insert('test');
    t.insert('testing');
    
    assert.equal(t.delete('test'), true);
    assert.equal(t.has('test'), false);
    assert.equal(t.has('testing'), true);
  });

  it('countPrefix', () => {
    const t = new Trie();
    t.insert('abc');
    t.insert('abd');
    t.insert('xyz');
    
    assert.equal(t.countPrefix('ab'), 2);
    assert.equal(t.countPrefix('a'), 2);
    assert.equal(t.countPrefix('x'), 1);
    assert.equal(t.countPrefix('z'), 0);
  });

  it('longestCommonPrefix', () => {
    const t = new Trie();
    t.insert('prefix_a');
    t.insert('prefix_b');
    t.insert('prefix_c');
    
    assert.equal(t.longestCommonPrefix(), 'prefix_');
  });

  it('stress: 10K words', () => {
    const t = new Trie();
    const words = [];
    for (let i = 0; i < 10000; i++) {
      const word = `word_${String(i).padStart(5, '0')}`;
      words.push(word);
      t.insert(word, i);
    }
    
    assert.equal(t.size, 10000);
    
    // All findable
    for (const w of words.slice(0, 100)) {
      assert.ok(t.has(w));
    }
    
    // Prefix search
    const r = t.prefixSearch('word_000', 20);
    assert.equal(r.length, 10); // word_00000 through word_00009
    
    console.log(`  10K words, prefix 'word_000': ${r.length} results`);
  });

  it('performance: 10K insert + 10K lookup', () => {
    const t = new Trie();
    
    const t0 = performance.now();
    for (let i = 0; i < 10000; i++) t.insert(`key-${i}`, i);
    const insertMs = performance.now() - t0;
    
    const t1 = performance.now();
    for (let i = 0; i < 10000; i++) t.get(`key-${i}`);
    const lookupMs = performance.now() - t1;
    
    console.log(`  10K insert: ${insertMs.toFixed(1)}ms, 10K lookup: ${lookupMs.toFixed(1)}ms`);
    assert.ok(insertMs < 500);
    assert.ok(lookupMs < 500);
  });

  it('empty trie', () => {
    const t = new Trie();
    assert.equal(t.size, 0);
    assert.equal(t.get('anything'), undefined);
    assert.equal(t.has('anything'), false);
    assert.deepEqual(t.prefixSearch('a'), []);
    assert.equal(t.longestCommonPrefix(), '');
  });
});
