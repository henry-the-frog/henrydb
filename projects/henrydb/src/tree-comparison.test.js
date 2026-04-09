// tree-comparison.test.js — Head-to-head comparison of ALL tree implementations
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

const N = 10000;

describe('🌳 Tree Comparison (10K elements)', () => {
  it('head-to-head: insert + lookup + height', async () => {
    const trees = [];
    
    const { BPlusTree } = await import('./bplus-tree.js');
    trees.push({ name: 'B+Tree(64)', create: () => new BPlusTree(64),
      insert: (t, k, v) => t.insert(k, v), get: (t, k) => t.get(k), height: () => 'N/A' });

    const { AVLTree } = await import('./avl-tree.js');
    trees.push({ name: 'AVL', create: () => new AVLTree(),
      insert: (t, k, v) => t.insert(k, v), get: (t, k) => t.get(k), height: t => t.height });

    const { RedBlackTree } = await import('./red-black-tree.js');
    trees.push({ name: 'Red-Black', create: () => new RedBlackTree(),
      insert: (t, k, v) => t.insert(k, v), get: (t, k) => t.get(k), height: t => t.height() });

    const { SplayTree } = await import('./splay-tree.js');
    trees.push({ name: 'Splay', create: () => new SplayTree(),
      insert: (t, k, v) => t.insert(k, v), get: (t, k) => t.get(k), height: () => 'N/A' });

    const { Treap } = await import('./treap.js');
    trees.push({ name: 'Treap', create: () => new Treap(),
      insert: (t, k, v) => t.insert(k, v), get: (t, k) => t.get(k), height: () => 'N/A' });

    const { SkipList } = await import('./skip-list.js');
    trees.push({ name: 'SkipList', create: () => new SkipList(),
      insert: (t, k, v) => t.insert(k, v), get: (t, k) => t.get(k), height: () => 'N/A' });

    console.log(`\n  ╔══════════════════════════════════════════════════════════╗`);
    console.log(`  ║  🌳 Tree Structure Comparison (${N} sorted inserts)     ║`);
    console.log(`  ╠════════════╦══════════╦══════════╦════════╦══════════════╣`);
    console.log(`  ║ Tree       ║ Insert   ║ Lookup   ║ Height ║ Guarantee    ║`);
    console.log(`  ╠════════════╬══════════╬══════════╬════════╬══════════════╣`);

    const guarantees = {
      'B+Tree(64)': 'O(log_B n)',
      'AVL': 'O(log n) strict',
      'Red-Black': 'O(log n)',
      'Splay': 'O(log n) amort',
      'Treap': 'O(log n) expect',
      'SkipList': 'O(log n) expect',
    };

    for (const spec of trees) {
      const t = spec.create();
      const t0 = performance.now();
      for (let i = 0; i < N; i++) spec.insert(t, i, i);
      const insertMs = performance.now() - t0;
      
      const t1 = performance.now();
      for (let i = 0; i < N; i++) spec.get(t, i);
      const lookupMs = performance.now() - t1;
      
      const h = typeof spec.height === 'function' ? spec.height(t) : 'N/A';
      const g = guarantees[spec.name] || '';
      
      console.log(`  ║ ${spec.name.padEnd(10)} ║ ${insertMs.toFixed(1).padStart(6)}ms ║ ${lookupMs.toFixed(1).padStart(6)}ms ║ ${String(h).padStart(6)} ║ ${g.padEnd(12)} ║`);
    }

    console.log(`  ╚════════════╩══════════╩══════════╩════════╩══════════════╝`);
    console.log(`\n  Theoretical minimum height: ${Math.ceil(Math.log2(N + 1))}`);
    assert.ok(true);
  });
});
