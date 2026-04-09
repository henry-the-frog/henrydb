// grand-benchmark.test.js — Compare ALL index structures
// The definitive HenryDB data structure shootout
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

const N = 5000; // Elements to benchmark

describe('🏆 Grand Data Structure Benchmark', () => {
  it('INSERT + LOOKUP shootout (5K elements)', async () => {
    const structures = [];
    
    // B+Tree
    const { BPlusTree } = await import('./bplus-tree.js');
    structures.push({
      name: 'B+Tree',
      create: () => new BPlusTree(64),
      insert: (s, k, v) => s.insert(k, v),
      get: (s, k) => s.get(k),
    });

    // SkipList
    const { SkipList } = await import('./skip-list.js');
    structures.push({
      name: 'SkipList',
      create: () => new SkipList(),
      insert: (s, k, v) => s.insert(k, v),
      get: (s, k) => s.get(k),
    });

    // ART
    const { AdaptiveRadixTree } = await import('./art.js');
    structures.push({
      name: 'ART',
      create: () => new AdaptiveRadixTree(),
      insert: (s, k, v) => s.insert(String(k), v),
      get: (s, k) => s.get(String(k)),
    });

    // Trie
    const { Trie } = await import('./trie.js');
    structures.push({
      name: 'Trie',
      create: () => new Trie(),
      insert: (s, k, v) => s.insert(String(k), v),
      get: (s, k) => s.get(String(k)),
    });

    // ExtendibleHash
    const { ExtendibleHashTable } = await import('./extendible-hash.js');
    structures.push({
      name: 'ExtHash',
      create: () => new ExtendibleHashTable(16),
      insert: (s, k, v) => s.insert(k, v),
      get: (s, k) => s.get(k),
    });

    // RobinHood
    const { RobinHoodHashMap } = await import('./robin-hood-hash.js');
    structures.push({
      name: 'RobinHood',
      create: () => new RobinHoodHashMap(),
      insert: (s, k, v) => s.set(k, v),
      get: (s, k) => s.get(k),
    });

    // SortedArray
    const { SortedArray } = await import('./sorted-array.js');
    structures.push({
      name: 'SortedArr',
      create: () => new SortedArray(),
      insert: (s, k, v) => s.insert(k, v),
      get: (s, k) => s.get(k),
    });

    // LSM-Tree
    const { LSMTree } = await import('./lsm-tree.js');
    structures.push({
      name: 'LSM-Tree',
      create: () => new LSMTree({ memtableSize: 500 }),
      insert: (s, k, v) => s.put(k, v),
      get: (s, k) => s.get(k),
    });

    // B-epsilon Tree
    const { BEpsilonTree } = await import('./b-epsilon-tree.js');
    structures.push({
      name: 'Bε-Tree',
      create: () => new BEpsilonTree(64, 32),
      insert: (s, k, v) => s.put(k, v),
      get: (s, k) => s.get(k),
    });

    console.log(`\n  ╔══════════════════════════════════════════════════════════════╗`);
    console.log(`  ║  🏆 Grand Data Structure Benchmark (${N} elements)           ║`);
    console.log(`  ╠════════════╦══════════╦══════════╦════════════╦═════════════╣`);
    console.log(`  ║ Structure  ║ Insert   ║ Lookup   ║ Insert/op  ║ Lookup/op   ║`);
    console.log(`  ╠════════════╬══════════╬══════════╬════════════╬═════════════╣`);

    for (const s of structures) {
      const inst = s.create();
      
      const t0 = performance.now();
      for (let i = 0; i < N; i++) s.insert(inst, i, `val-${i}`);
      const insertMs = performance.now() - t0;
      
      const t1 = performance.now();
      for (let i = 0; i < N; i++) s.get(inst, i);
      const lookupMs = performance.now() - t1;
      
      console.log(`  ║ ${s.name.padEnd(10)} ║ ${insertMs.toFixed(1).padStart(6)}ms ║ ${lookupMs.toFixed(1).padStart(6)}ms ║ ${(insertMs/N*1000).toFixed(2).padStart(8)}µs ║ ${(lookupMs/N*1000).toFixed(2).padStart(9)}µs ║`);
    }

    console.log(`  ╚════════════╩══════════╩══════════╩════════════╩═════════════╝`);
    
    console.log(`\n  Legend:`);
    console.log(`    B+Tree    — Balanced tree, O(log n), range queries`);
    console.log(`    SkipList  — Probabilistic, O(log n), Redis-style`);
    console.log(`    ART       — Adaptive Radix Tree, O(k), DuckDB-style`);
    console.log(`    Trie      — Prefix tree, O(k), autocomplete`);
    console.log(`    ExtHash   — Extendible hashing, O(1) avg, dynamic`);
    console.log(`    RobinHood — Open-addressing, O(1) avg, low variance`);
    console.log(`    SortedArr — Simple array, O(log n) read, O(n) write`);
    console.log(`    LSM-Tree  — Write-optimized, O(1) write, O(log n) read`);
    console.log(`    Bε-Tree   — Buffered B-tree, batch I/O optimization`);
    
    assert.ok(true);
  });
});
