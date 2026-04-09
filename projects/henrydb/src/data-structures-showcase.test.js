// data-structures-showcase.test.js — HenryDB Data Structures Library
// Comprehensive demonstration and benchmark of all data structures.
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

describe('📚 HenryDB Data Structures Library', () => {
  describe('🌳 Tree Structures', () => {
    it('B+Tree: balanced tree for sorted data + range queries', async () => {
      const { BPlusTree } = await import('./bplus-tree.js');
      const bt = new BPlusTree(64);
      for (let i = 0; i < 10000; i++) bt.insert(i, `val-${i}`);
      
      assert.equal(bt.get(5000), 'val-5000');
      const range = bt.range(100, 200);
      assert.equal(range.length, 101);
    });

    it('BTreeTable: clustered storage engine (rows sorted by PK)', async () => {
      const { BTreeTable } = await import('./btree-table.js');
      const table = new BTreeTable('test');
      table.insert([3, 'Charlie']);
      table.insert([1, 'Alice']);
      table.insert([2, 'Bob']);
      
      const rows = [...table.scan()].map(r => r.values[1]);
      assert.deepEqual(rows, ['Alice', 'Bob', 'Charlie']);
    });

    it('Trie: prefix tree for string keys + autocomplete', async () => {
      const { Trie } = await import('./trie.js');
      const t = new Trie();
      ['database', 'data', 'datum', 'date', 'dog'].forEach(w => t.insert(w));
      
      const completions = t.autocomplete('dat');
      assert.ok(completions.length === 4);
    });

    it('SkipList: probabilistic ordered structure (Redis-style)', async () => {
      const { SkipList } = await import('./skip-list.js');
      const sl = new SkipList();
      for (let i = 1000; i >= 1; i--) sl.insert(i, i);
      
      assert.equal(sl.min().key, 1);
      assert.equal(sl.max().key, 1000);
    });

    it('LSM-Tree: write-optimized storage (LevelDB/RocksDB-style)', async () => {
      const { LSMTree } = await import('./lsm-tree.js');
      const lsm = new LSMTree({ memtableSize: 100 });
      for (let i = 0; i < 500; i++) lsm.put(i, i * 2);
      
      assert.equal(lsm.get(250), 500);
      const stats = lsm.getStats();
      assert.ok(stats.flushes >= 1);
    });
  });

  describe('🗄️ Hash Structures', () => {
    it('ExtendibleHashTable: dynamic hash with bucket splitting', async () => {
      const { ExtendibleHashTable } = await import('./extendible-hash.js');
      const ht = new ExtendibleHashTable(16);
      for (let i = 0; i < 1000; i++) ht.insert(i, `val-${i}`);
      
      assert.equal(ht.get(500), 'val-500');
      assert.equal(ht.size, 1000);
    });

    it('RobinHoodHashMap: low-variance open addressing', async () => {
      const { RobinHoodHashMap } = await import('./robin-hood-hash.js');
      const m = new RobinHoodHashMap();
      for (let i = 0; i < 1000; i++) m.set(i, i * 3);
      
      assert.equal(m.get(500), 1500);
      const stats = m.getStats();
      assert.ok(stats.avgProbeDistance < 2);
    });
  });

  describe('🎲 Probabilistic Structures', () => {
    it('BloomFilter: set membership (no false negatives)', async () => {
      const { BloomFilter } = await import('./bloom-filter.js');
      const bf = new BloomFilter(10000, 0.01);
      for (let i = 0; i < 10000; i++) bf.add(i);
      
      // No false negatives
      for (let i = 0; i < 100; i++) assert.equal(bf.mightContain(i), true);
      // Low false positives
      let fp = 0;
      for (let i = 10000; i < 11000; i++) if (bf.mightContain(i)) fp++;
      assert.ok(fp < 50, `FPR too high: ${fp}/1000`);
    });

    it('CuckooFilter: membership with deletion support', async () => {
      const { CuckooFilter } = await import('./cuckoo-filter.js');
      const cf = new CuckooFilter(1000);
      cf.insert('test');
      assert.equal(cf.contains('test'), true);
      cf.delete('test');
      assert.equal(cf.contains('test'), false);
    });

    it('HyperLogLog: cardinality estimation in 16KB', async () => {
      const { HyperLogLog } = await import('./hyperloglog.js');
      const hll = new HyperLogLog(14);
      for (let i = 0; i < 100000; i++) hll.add(`user-${i}`);
      
      const estimate = hll.estimate();
      const error = Math.abs(estimate - 100000) / 100000;
      assert.ok(error < 0.05, `Error ${(error*100).toFixed(1)}% too high`);
      assert.equal(hll.getStats().bytesUsed, 16384); // Always 16KB
    });

    it('CountMinSketch: frequency estimation (never underestimates)', async () => {
      const { CountMinSketch } = await import('./count-min-sketch.js');
      const cms = new CountMinSketch(2048, 5);
      cms.add('hot-key', 1000);
      cms.add('cold-key', 1);
      
      assert.ok(cms.estimate('hot-key') >= 1000);
      assert.ok(cms.estimate('cold-key') >= 1);
    });
  });

  describe('💾 Storage Structures', () => {
    it('LRUReplacer: O(1) page eviction', async () => {
      const { LRUReplacer } = await import('./lru-replacer.js');
      const r = new LRUReplacer(4);
      r.record(0); r.record(1); r.record(2);
      r.record(0); // Move to MRU
      
      assert.equal(r.evict(), 1); // 1 is LRU
    });

    it('ClockReplacer: PostgreSQL-style usage count sweep', async () => {
      const { ClockReplacer } = await import('./clock-replacer.js');
      const r = new ClockReplacer(100, 5);
      for (let i = 0; i < 100; i++) r.record(i);
      // Hot page
      for (let j = 0; j < 10; j++) r.record(0);
      
      const evicted = r.evict();
      assert.ok(evicted !== 0, 'Hot page should not be evicted first');
    });

    it('BufferPoolManager: page caching with eviction', async () => {
      const { BufferPoolManager, InMemoryDiskManager } = await import('./buffer-pool.js');
      const disk = new InMemoryDiskManager(64);
      const bpm = new BufferPoolManager(3, disk);
      
      const page = bpm.newPage();
      page.data.write('hello');
      bpm.unpinPage(page.pageId, true);
      
      const fetched = bpm.fetchPage(page.pageId);
      assert.equal(fetched.toString('utf8', 0, 5), 'hello');
      bpm.unpinPage(page.pageId, false);
    });

    it('DiskManager: file-backed page I/O', async () => {
      const { DiskManager } = await import('./disk-manager.js');
      const dm = DiskManager.createTemp(128);
      
      const pageId = dm.allocatePage();
      const buf = Buffer.alloc(128);
      buf.write('persistent data');
      dm.writePage(pageId, buf);
      
      const readBack = dm.readPage(pageId);
      assert.equal(readBack.toString('utf8', 0, 15), 'persistent data');
      dm.destroy();
    });
  });

  describe('📊 Performance Summary', () => {
    it('benchmark all structures: 10K operations', async () => {
      const N = 10000;
      const results = [];
      
      // B+Tree
      const { BPlusTree } = await import('./bplus-tree.js');
      const bt = new BPlusTree(64);
      let t = performance.now();
      for (let i = 0; i < N; i++) bt.insert(i, i);
      results.push({ name: 'B+Tree', insertMs: performance.now() - t });
      t = performance.now();
      for (let i = 0; i < N; i++) bt.get(i);
      results[results.length - 1].lookupMs = performance.now() - t;

      // SkipList
      const { SkipList } = await import('./skip-list.js');
      const sl = new SkipList();
      t = performance.now();
      for (let i = 0; i < N; i++) sl.insert(i, i);
      results.push({ name: 'SkipList', insertMs: performance.now() - t });
      t = performance.now();
      for (let i = 0; i < N; i++) sl.get(i);
      results[results.length - 1].lookupMs = performance.now() - t;

      // ExtendibleHash
      const { ExtendibleHashTable } = await import('./extendible-hash.js');
      const ht = new ExtendibleHashTable(16);
      t = performance.now();
      for (let i = 0; i < N; i++) ht.insert(i, i);
      results.push({ name: 'ExtHash', insertMs: performance.now() - t });
      t = performance.now();
      for (let i = 0; i < N; i++) ht.get(i);
      results[results.length - 1].lookupMs = performance.now() - t;

      // RobinHood
      const { RobinHoodHashMap } = await import('./robin-hood-hash.js');
      const rh = new RobinHoodHashMap();
      t = performance.now();
      for (let i = 0; i < N; i++) rh.set(i, i);
      results.push({ name: 'RobinHood', insertMs: performance.now() - t });
      t = performance.now();
      for (let i = 0; i < N; i++) rh.get(i);
      results[results.length - 1].lookupMs = performance.now() - t;

      // LSM-Tree
      const { LSMTree } = await import('./lsm-tree.js');
      const lsm = new LSMTree({ memtableSize: 500 });
      t = performance.now();
      for (let i = 0; i < N; i++) lsm.put(i, i);
      results.push({ name: 'LSM-Tree', insertMs: performance.now() - t });
      t = performance.now();
      for (let i = 0; i < N; i++) lsm.get(i);
      results[results.length - 1].lookupMs = performance.now() - t;

      console.log('\n  ╔════════════════════════════════════════════════╗');
      console.log('  ║  10K Operations Benchmark (all times in ms)    ║');
      console.log('  ╠══════════════╦══════════╦══════════╦═══════════╣');
      console.log('  ║ Structure    ║  Insert  ║  Lookup  ║ Lookup/op ║');
      console.log('  ╠══════════════╬══════════╬══════════╬═══════════╣');
      for (const r of results) {
        console.log(`  ║ ${r.name.padEnd(12)} ║ ${r.insertMs.toFixed(1).padStart(8)} ║ ${r.lookupMs.toFixed(1).padStart(8)} ║ ${(r.lookupMs/N*1000).toFixed(3).padStart(7)}µs ║`);
      }
      console.log('  ╚══════════════╩══════════╩══════════╩═══════════╝');
      
      assert.ok(true);
    });
  });
});
