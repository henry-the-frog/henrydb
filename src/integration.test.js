// integration.test.js — Cross-module integration tests
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

// Import multiple modules to test interactions
import { BPlusTree } from './bplus-tree.js';
import { BufferPoolManager } from './buffer-pool.js';
import { SkipList } from './skip-list.js';
import { LockManager } from './lock-manager.js';
import { DeadlockDetector } from './deadlock-detector.js';
import { WALCompactor } from './wal-compaction.js';
import { CountMinSketch } from './count-min-sketch.js';
import { HyperLogLog } from './hyperloglog.js';
import { TDigest } from './tdigest.js';
import { BitmapIndex } from './bitmap-index.js';
import { RingBuffer } from './ring-buffer.js';
import { CuckooHashTable } from './cuckoo-hash.js';
import { RobinHoodHashTable } from './robin-hood-hash.js';
import { ExpressionCompiler } from './expression-compiler.js';
import { ConstantFolder } from './constant-folding.js';

describe('Integration: B+ Tree + Buffer Pool', () => {
  it('B+ tree indexes buffered pages', () => {
    const bpm = new BufferPoolManager(16, 256);
    const tree = new BPlusTree(16);
    
    // Simulate indexing pages
    for (let i = 0; i < 50; i++) {
      const page = bpm.newPage();
      page.data.write(`row_${i}`, 0);
      bpm.unpinPage(page.pageId);
      tree.insert(i, page.pageId);
    }

    // Lookup via index, then fetch page
    const pageId = tree.get(25);
    const page = bpm.fetchPage(pageId);
    assert.ok(page);
    bpm.unpinPage(page.pageId);
  });

  it('range scan via B+ tree', () => {
    const tree = new BPlusTree(8);
    for (let i = 0; i < 100; i++) tree.insert(i, `value_${i}`);
    const range = tree.range(40, 60);
    assert.equal(range.length, 21);
    assert.equal(range[0].value, 'value_40');
  });
});

describe('Integration: Lock Manager + Deadlock Detector', () => {
  it('detect and resolve deadlock', async () => {
    const lm = new LockManager();
    const dd = new DeadlockDetector();

    dd.registerTxn('T1', { startTime: 100 });
    dd.registerTxn('T2', { startTime: 200 });

    await lm.acquire('T1', 'row1', 'X');
    await lm.acquire('T2', 'row2', 'X');

    // T1 wants row2 (held by T2)
    dd.addWait('T1', 'T2');
    // T2 wants row1 (held by T1)
    dd.addWait('T2', 'T1');

    const victims = dd.resolveDeadlocks();
    assert.equal(victims.length, 1);
    
    // Abort victim
    lm.releaseAll(victims[0]);
  });
});

describe('Integration: WAL + Recovery simulation', () => {
  it('WAL tracks operations and supports replay', () => {
    const wal = new WALCompactor({ autoCheckpoint: false });
    
    wal.append('BEGIN', 'users', null, null, 'txn1');
    wal.append('INSERT', 'users', 1, { name: 'Alice' }, 'txn1');
    wal.append('INSERT', 'users', 2, { name: 'Bob' }, 'txn1');
    wal.append('COMMIT', 'users', null, null, 'txn1');
    
    // Simulate crash recovery — replay from beginning
    const entries = wal.replay(0);
    assert.equal(entries.length, 4);
    
    // Checkpoint and truncate
    wal.checkpoint();
    assert.equal(wal.entryCount, 0);
  });
});

describe('Integration: Sketches for query optimization', () => {
  it('CMS estimates filter selectivity', () => {
    const cms = new CountMinSketch(2048, 5);
    for (let i = 0; i < 10000; i++) {
      cms.add(`dept_${i % 5}`);
    }
    // Each department appears ~2000 times
    assert.ok(cms.estimate('dept_0') >= 1800);
    assert.ok(cms.estimate('dept_0') <= 2500);
  });

  it('HLL estimates join cardinality', () => {
    const hllA = new HyperLogLog(10);
    const hllB = new HyperLogLog(10);
    
    for (let i = 0; i < 5000; i++) hllA.add(i);
    for (let i = 3000; i < 8000; i++) hllB.add(i);
    
    // Union via merge
    hllA.merge(hllB);
    const unionSize = hllA.estimate();
    assert.ok(Math.abs(unionSize - 8000) < 800); // ~10% error
  });

  it('TDigest for query latency monitoring', () => {
    const td = new TDigest(200);
    for (let i = 0; i < 10000; i++) {
      td.add(Math.random() * 100);
    }
    const p99 = td.percentile(99);
    assert.ok(p99 > 90 && p99 < 100);
  });
});

describe('Integration: Bitmap Index + Expression Compiler', () => {
  it('compiled filter on bitmap results', () => {
    const idx = new BitmapIndex();
    const statuses = Array.from({ length: 1000 }, (_, i) => 
      ['active', 'inactive', 'pending'][i % 3]
    );
    idx.build(statuses);
    
    const activeRows = idx.getRows('active');
    assert.ok(activeRows.length > 300);
    
    // Use expression compiler for secondary filter
    const ec = new ExpressionCompiler();
    const { fn } = ec.compile({
      type: 'COMPARE', op: 'GT',
      left: { type: 'column', name: 'id' },
      right: { type: 'literal', value: 500 },
    });
    
    const data = activeRows.map(i => ({ id: i, status: 'active' }));
    const filtered = data.filter(fn);
    assert.ok(filtered.length > 0);
    assert.ok(filtered.every(r => r.id > 500));
  });
});

describe('Integration: Hash Tables comparison', () => {
  it('all hash tables agree on same data', () => {
    const cuckoo = new CuckooHashTable(512);
    const robin = new RobinHoodHashTable(512);
    
    for (let i = 0; i < 200; i++) {
      cuckoo.set(i, i * 7);
      robin.set(i, i * 7);
    }
    
    for (let i = 0; i < 200; i++) {
      assert.equal(cuckoo.get(i), i * 7);
      assert.equal(robin.get(i), i * 7);
    }
  });
});

describe('Integration: Constant Folder + Expression Compiler', () => {
  it('fold then compile', () => {
    const folder = new ConstantFolder();
    const compiler = new ExpressionCompiler();
    
    // Original: (2 + 3) > x
    const expr = {
      type: 'COMPARE', op: 'GT',
      left: { type: 'ARITHMETIC', op: '+', left: { type: 'literal', value: 2 }, right: { type: 'literal', value: 3 } },
      right: { type: 'column', name: 'x' },
    };
    
    // Fold: 5 > x
    const folded = folder.fold(expr);
    assert.equal(folded.left.value, 5);
    
    // Compile folded expression
    const { fn } = compiler.compile(folded);
    assert.ok(fn({ x: 3 })); // 5 > 3 = true
    assert.ok(!fn({ x: 10 })); // 5 > 10 = false
  });
});

describe('Integration: Ring Buffer as query history', () => {
  it('tracks last N queries', () => {
    const history = new RingBuffer(10);
    for (let i = 0; i < 25; i++) history.push({ sql: `SELECT * FROM t${i}`, ms: i * 10 });
    
    assert.equal(history.size, 10);
    assert.equal(history.peek().sql, 'SELECT * FROM t24');
    assert.equal(history.peekOldest().sql, 'SELECT * FROM t15');
  });
});

describe('Integration: Skip List as memtable', () => {
  it('sorted insertion and range scan', () => {
    const memtable = new SkipList();
    for (let i = 0; i < 100; i++) memtable.set(`key_${String(i).padStart(3, '0')}`, `value_${i}`);
    
    // Range scan
    const range = memtable.range('key_020', 'key_030');
    assert.equal(range.length, 11);
    assert.equal(range[0].key, 'key_020');
  });
});
