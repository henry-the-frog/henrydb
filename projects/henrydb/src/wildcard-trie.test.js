// wildcard-trie.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WildcardTrie } from './wildcard-trie.js';

describe('WildcardTrie', () => {
  it('exact match', () => {
    const wt = new WildcardTrie();
    wt.insert('hello', 1);
    wt.insert('world', 2);
    assert.equal(wt.match('hello').length, 1);
    assert.equal(wt.match('xyz').length, 0);
  });

  it('? matches single char', () => {
    const wt = new WildcardTrie();
    wt.insert('cat', 1);
    wt.insert('car', 2);
    wt.insert('cap', 3);
    
    const matches = wt.match('ca?');
    assert.equal(matches.length, 3);
  });

  it('* matches any sequence', () => {
    const wt = new WildcardTrie();
    wt.insert('abc', 1);
    wt.insert('axyzc', 2);
    wt.insert('ac', 3);
    
    const matches = wt.match('a*c');
    assert.ok(matches.length >= 2); // abc and ac at minimum
  });

  it('use case: LIKE query simulation', () => {
    const wt = new WildcardTrie();
    const names = ['Alice', 'Alex', 'Bob', 'Alice2', 'Alfred'];
    names.forEach(n => wt.insert(n, n));
    
    // LIKE 'Al%' → Al*
    const alMatches = wt.match('Al*');
    assert.ok(alMatches.length >= 3); // Alice, Alex, Alfred
  });

  it('matchText: stored patterns match text', () => {
    const wt = new WildcardTrie();
    wt.insert('user.*', true); // Topic subscription
    wt.insert('order.?', true);
    
    assert.equal(wt.matchText('user.created'), true);
    assert.equal(wt.matchText('order.A'), true);
    assert.equal(wt.matchText('other.thing'), false);
  });
});
