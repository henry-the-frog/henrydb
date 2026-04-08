// art.test.js — Tests for Adaptive Radix Tree
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { AdaptiveRadixTree } from './art.js';

describe('AdaptiveRadixTree', () => {

  it('insert and search: integers', () => {
    const art = new AdaptiveRadixTree();
    art.insert(42, 'hello');
    art.insert(100, 'world');
    art.insert(1, 'first');

    assert.equal(art.search(42), 'hello');
    assert.equal(art.search(100), 'world');
    assert.equal(art.search(1), 'first');
    assert.equal(art.search(999), undefined);
    assert.equal(art.size, 3);
  });

  it('insert and search: strings', () => {
    const art = new AdaptiveRadixTree();
    art.insert('alice', 1);
    art.insert('bob', 2);
    art.insert('charlie', 3);

    assert.equal(art.search('alice'), 1);
    assert.equal(art.search('bob'), 2);
    assert.equal(art.search('charlie'), 3);
    assert.equal(art.search('dave'), undefined);
  });

  it('update existing key', () => {
    const art = new AdaptiveRadixTree();
    art.insert(5, 'old');
    art.insert(5, 'new');
    assert.equal(art.search(5), 'new');
    assert.equal(art.size, 1);
  });

  it('has() method', () => {
    const art = new AdaptiveRadixTree();
    art.insert(10, 'val');
    assert.ok(art.has(10));
    assert.ok(!art.has(20));
  });

  it('grows Node4 → Node16', () => {
    const art = new AdaptiveRadixTree();
    for (let i = 0; i < 10; i++) art.insert(i * 256, i);

    // Should have grown past Node4
    for (let i = 0; i < 10; i++) {
      assert.equal(art.search(i * 256), i);
    }
    assert.equal(art.size, 10);
  });

  it('grows Node16 → Node48', () => {
    const art = new AdaptiveRadixTree();
    for (let i = 0; i < 30; i++) art.insert(i * 256, i);

    for (let i = 0; i < 30; i++) {
      assert.equal(art.search(i * 256), i);
    }
  });

  it('grows to Node256', () => {
    const art = new AdaptiveRadixTree();
    for (let i = 0; i < 100; i++) art.insert(i * 256, i);

    for (let i = 0; i < 100; i++) {
      assert.equal(art.search(i * 256), i);
    }
  });

  it('handles negative integers', () => {
    const art = new AdaptiveRadixTree();
    art.insert(-100, 'negative');
    art.insert(0, 'zero');
    art.insert(100, 'positive');

    assert.equal(art.search(-100), 'negative');
    assert.equal(art.search(0), 'zero');
    assert.equal(art.search(100), 'positive');
  });

  it('sequential insert: 10K keys', () => {
    const art = new AdaptiveRadixTree();
    for (let i = 0; i < 10000; i++) art.insert(i, i * 10);

    assert.equal(art.size, 10000);
    assert.equal(art.search(0), 0);
    assert.equal(art.search(5000), 50000);
    assert.equal(art.search(9999), 99990);
    assert.equal(art.search(10000), undefined);
  });

  it('random insert: 5K keys', () => {
    const art = new AdaptiveRadixTree();
    const keys = [];
    for (let i = 0; i < 5000; i++) {
      const key = (i * 2654435761) & 0xFFFFFF; // Hash for pseudo-random
      keys.push(key);
      art.insert(key, i);
    }

    for (let i = 0; i < keys.length; i++) {
      assert.equal(art.search(keys[i]), i);
    }
  });

  it('string keys with common prefixes', () => {
    const art = new AdaptiveRadixTree();
    art.insert('prefix_a', 1);
    art.insert('prefix_ab', 2);
    art.insert('prefix_abc', 3);
    art.insert('prefix_b', 4);

    assert.equal(art.search('prefix_a'), 1);
    assert.equal(art.search('prefix_ab'), 2);
    assert.equal(art.search('prefix_abc'), 3);
    assert.equal(art.search('prefix_b'), 4);
  });

  it('benchmark: ART vs Map on 100K point lookups', () => {
    const n = 100000;
    
    // Build ART
    const art = new AdaptiveRadixTree();
    const t0 = Date.now();
    for (let i = 0; i < n; i++) art.insert(i, i);
    const artBuildMs = Date.now() - t0;

    // Build Map
    const map = new Map();
    const t1 = Date.now();
    for (let i = 0; i < n; i++) map.set(i, i);
    const mapBuildMs = Date.now() - t1;

    // Lookup ART
    const t2 = Date.now();
    for (let i = 0; i < n; i++) art.search(i);
    const artLookupMs = Date.now() - t2;

    // Lookup Map
    const t3 = Date.now();
    for (let i = 0; i < n; i++) map.get(i);
    const mapLookupMs = Date.now() - t3;

    console.log(`    Build: ART ${artBuildMs}ms vs Map ${mapBuildMs}ms | Lookup: ART ${artLookupMs}ms vs Map ${mapLookupMs}ms`);
    assert.equal(art.size, n);
  });

  it('entries returns key-value pairs', () => {
    const art = new AdaptiveRadixTree();
    art.insert(3, 'c');
    art.insert(1, 'a');
    art.insert(2, 'b');

    const entries = art.entries();
    assert.equal(entries.length, 3);
    // Should be sorted by key (integer encoding preserves order)
    assert.equal(entries[0][1], 'a'); // key 1
    assert.equal(entries[1][1], 'b'); // key 2
    assert.equal(entries[2][1], 'c'); // key 3
  });
});
