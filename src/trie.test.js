// trie.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Trie, RingBuffer } from './trie.js';

describe('Trie', () => {
  it('insert and exact search', () => {
    const trie = new Trie();
    trie.insert('hello', 1);
    trie.insert('world', 2);
    assert.equal(trie.search('hello'), 1);
    assert.equal(trie.search('world'), 2);
    assert.equal(trie.search('nope'), undefined);
  });

  it('prefix queries', () => {
    const trie = new Trie();
    trie.insert('apple', 1);
    trie.insert('application', 2);
    trie.insert('apply', 3);
    trie.insert('banana', 4);
    
    const results = trie.findByPrefix('app');
    assert.equal(results.length, 3);
    assert.ok(results.some(r => r.key === 'apple'));
    assert.ok(results.some(r => r.key === 'application'));
  });

  it('hasPrefix checks prefix existence', () => {
    const trie = new Trie();
    trie.insert('hello');
    assert.equal(trie.hasPrefix('hel'), true);
    assert.equal(trie.hasPrefix('xyz'), false);
  });

  it('delete removes keys', () => {
    const trie = new Trie();
    trie.insert('hello');
    trie.insert('help');
    
    trie.delete('hello');
    assert.equal(trie.search('hello'), undefined);
    assert.equal(trie.search('help'), true);
    assert.equal(trie.size, 1);
  });

  it('findByPrefix with limit', () => {
    const trie = new Trie();
    for (let i = 0; i < 100; i++) trie.insert(`test${i}`);
    
    const results = trie.findByPrefix('test', 5);
    assert.equal(results.length, 5);
  });

  it('handles empty string', () => {
    const trie = new Trie();
    trie.insert('', 'empty');
    assert.equal(trie.search(''), 'empty');
  });

  it('autocomplete use case', () => {
    const trie = new Trie();
    const words = ['database', 'data', 'datum', 'debug', 'delete', 'design'];
    words.forEach(w => trie.insert(w));
    
    assert.equal(trie.findByPrefix('da').length, 3);
    assert.equal(trie.findByPrefix('de').length, 3);
    assert.equal(trie.findByPrefix('dat').length, 3);
  });
});

describe('RingBuffer', () => {
  it('push and get', () => {
    const rb = new RingBuffer(5);
    rb.push('a');
    rb.push('b');
    rb.push('c');
    
    assert.equal(rb.get(0), 'a');
    assert.equal(rb.get(2), 'c');
    assert.equal(rb.size, 3);
  });

  it('overwrites oldest when full', () => {
    const rb = new RingBuffer(3);
    rb.push('a');
    rb.push('b');
    rb.push('c');
    rb.push('d'); // Overwrites 'a'
    
    assert.equal(rb.size, 3);
    assert.equal(rb.get(0), 'b'); // 'a' was overwritten
    assert.equal(rb.get(2), 'd');
  });

  it('latest returns newest item', () => {
    const rb = new RingBuffer(5);
    rb.push(1);
    rb.push(2);
    rb.push(3);
    assert.equal(rb.latest(), 3);
  });

  it('toArray returns ordered items', () => {
    const rb = new RingBuffer(3);
    rb.push('x');
    rb.push('y');
    rb.push('z');
    rb.push('w'); // overwrites 'x'
    
    assert.deepEqual(rb.toArray(), ['y', 'z', 'w']);
  });

  it('handles empty buffer', () => {
    const rb = new RingBuffer(5);
    assert.equal(rb.size, 0);
    assert.equal(rb.latest(), undefined);
    assert.deepEqual(rb.toArray(), []);
  });

  it('isFull property', () => {
    const rb = new RingBuffer(2);
    assert.equal(rb.isFull, false);
    rb.push(1);
    rb.push(2);
    assert.equal(rb.isFull, true);
  });
});
