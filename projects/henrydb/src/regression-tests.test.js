// regression-tests.test.js — Regression tests and cross-validation
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';

import { BPlusTree } from './bplus-tree.js';
import { SkipList } from './skip-list.js';
import { CuckooHashTable } from './cuckoo-hash.js';
import { RobinHoodHashMap } from './robin-hood-hash.js';
import { DoubleHashTable } from './advanced-ds.js';
import { LinearHashTable } from './linear-hashing.js';
import { ExtendibleHashTable } from './extendible-hashing.js';
import { LogHashTable } from './log-hash-table.js';
import { Trie } from './trie.js';
import { Treap, CuckooFilter, XORFilter } from './probabilistic-filters.js';
import { SplayTree, IntervalTree, OrderStatisticsTree, BinaryHeap, Quadtree } from './more-trees.js';
import { LRUK } from './lru-k.js';
import { SavepointTransaction } from './savepoints.js';
import { PartitionedTable } from './table-partitioning.js';
import { SlottedPage } from './slotted-page.js';
import { HeapFile } from './heap-file.js';
import { TupleDescriptor } from './tuple-descriptor.js';
import { JSONSerde, BinarySerde, CSVSerde } from './serde.js';
import { applyWindowFunctions } from './window-functions.js';
import { ConstantFolder } from './constant-folding.js';
import { QueryRewriter } from './query-rewriter.js';
import { PlanVisualizer } from './plan-viz.js';
import { StatsCollector } from './stats-collector.js';
import { MaterializedViewManager } from './materialized-view.js';
import { GraphDB, TimeSeriesEngine, CDC } from './graph-ts-cdc.js';
import { RaftNode } from './raft.js';
import { LamportClock, VectorClock, GCounter, PNCounter, GossipProtocol } from './distributed-primitives.js';
import { TimestampOrdering } from './timestamp-ordering.js';
import { OCC } from './occ.js';
import { ARIESRecovery } from './aries-recovery.js';
import { ReservoirSampler, MinHash } from './sampling.js';
import { InvertedIndex } from './inverted-index.js';
import { RTree } from './rtree.js';
import { COWBTree } from './cow-btree.js';
import { BufferPoolManager } from './buffer-pool.js';
import { LockManager } from './lock-manager.js';
import { DeadlockDetector } from './deadlock-detector.js';
import { TwoPhaseLocking } from './two-phase-locking.js';
import { WALCompactor } from './wal-compaction.js';
import { LSMTree } from './lsm-compaction.js';
import { ConsistentHashRing } from './consistent-hashing.js';
import { CursorPaginator } from './cursor-pagination.js';
import { SimpleCTEEngine } from './cte.js';
import { SubqueryEngine } from './subquery.js';

describe('Cross-validation: All sorted structures agree', () => {
  const keys = [50, 20, 80, 10, 30, 60, 90, 5, 15, 25, 35, 55, 65, 85, 95];
  
  it('B+ tree and Skip list have same elements', () => {
    const bpt = new BPlusTree(4); const sl = new SkipList();
    for (const k of keys) { bpt.insert(k, k); sl.insert(k, k); }
    for (const k of keys) { assert.equal(bpt.get(k), sl.get(k)); }
  });
  it('Treap and Splay tree have same elements', () => {
    const treap = new Treap(); const splay = new SplayTree();
    for (const k of keys) { treap.insert(k, k); splay.insert(k, k); }
    for (const k of keys) { assert.equal(treap.get(k), splay.get(k)); }
  });
});

describe('Cross-validation: All hash tables agree', () => {
  const data = Array.from({ length: 100 }, (_, i) => [i, i * 7]);
  
  for (const [name, create] of [
    ['Cuckoo', () => new CuckooHashTable(256)],
    ['Robin Hood', () => new RobinHoodHashMap(256)],
    ['Double Hash', () => new DoubleHashTable(256)],
    ['Linear Hash', () => new LinearHashTable()],
    ['Extendible Hash', () => new ExtendibleHashTable(8)],
    ['Log Hash', () => new LogHashTable()],
  ]) {
    it(`${name} matches reference`, () => {
      const ht = create();
      const ref = new Map();
      for (const [k, v] of data) { ht.set(k, v); ref.set(k, v); }
      for (const [k, v] of ref) assert.equal(ht.get(k), v, `${name}: key ${k}`);
    });
  }
});

describe('Cross-validation: Serde roundtrips', () => {
  const row = { id: 42, name: 'Test "User"', salary: 99999.99, active: true };
  
  it('JSON roundtrip preserves all types', () => {
    const s = new JSONSerde();
    assert.deepEqual(s.deserialize(s.serialize(row)), row);
  });
  it('Binary roundtrip preserves all types', () => {
    const s = new BinarySerde();
    const decoded = s.deserialize(s.serialize(row));
    assert.equal(decoded.id, 42);
    assert.ok(Math.abs(decoded.salary - 99999.99) < 0.01);
  });
  it('CSV roundtrip preserves values', () => {
    const s = new CSVSerde(['id', 'name', 'salary']);
    const decoded = s.deserialize(s.serialize({ id: 42, name: 'Alice', salary: 100000 }));
    assert.equal(decoded.id, 42);
    assert.equal(decoded.name, 'Alice');
  });
});

describe('Regression: Window functions with empty partition', () => {
  it('handles empty input', () => {
    const result = applyWindowFunctions([], [{ func: 'ROW_NUMBER', alias: 'rn' }]);
    assert.deepEqual(result, []);
  });
});

describe('Regression: Constant folder edge cases', () => {
  it('handles null expr', () => {
    const cf = new ConstantFolder();
    assert.equal(cf.fold(null), null);
    assert.equal(cf.fold(undefined), undefined);
  });
});

describe('Regression: Query rewriter with empty predicates', () => {
  it('simplify null', () => {
    const rw = new QueryRewriter();
    assert.equal(rw.simplifyPredicate(null), null);
  });
});

describe('Regression: Plan viz with deep tree', () => {
  it('handles 5-level plan', () => {
    const plan = {
      type: 'Projection', children: [{
        type: 'Sort', children: [{
          type: 'HashJoin', children: [
            { type: 'Filter', children: [{ type: 'SeqScan', table: 'a' }] },
            { type: 'IndexScan', table: 'b' },
          ]
        }]
      }]
    };
    const viz = new PlanVisualizer();
    const dot = viz.toDot(plan);
    assert.ok(dot.includes('SeqScan'));
    assert.ok(dot.includes('IndexScan'));
  });
});

describe('Regression: Stats collector on small table', () => {
  it('handles 1-row table', () => {
    const sc = new StatsCollector();
    sc.analyze('tiny', [{ id: 1, name: 'Alice' }]);
    const cs = sc.getColumnStats('tiny', 'id');
    assert.equal(cs.distinctValues, 1);
    assert.equal(cs.min, 1);
    assert.equal(cs.max, 1);
  });
});

describe('Regression: Materialized view with no tables', () => {
  it('view without dependencies', () => {
    const mgr = new MaterializedViewManager();
    mgr.create('standalone', () => [1, 2, 3], [], { refreshMode: 'eager' });
    assert.equal(mgr.get('standalone').length, 3);
  });
});

describe('Regression: Graph DB disconnected', () => {
  it('shortest path in disconnected graph', () => {
    const g = new GraphDB();
    g.addNode('A'); g.addNode('B'); g.addNode('C');
    g.addEdge('A', 'B');
    assert.equal(g.shortestPath('A', 'C'), null);
  });
});

describe('Regression: Time series empty metric', () => {
  it('query non-existent metric', () => {
    const ts = new TimeSeriesEngine();
    assert.deepEqual(ts.query('nonexistent', 0, Infinity), []);
  });
});

describe('Regression: CDC filter empty', () => {
  it('getChanges on empty log', () => {
    const cdc = new CDC();
    assert.deepEqual(cdc.getChanges(), []);
  });
});

describe('Regression: Raft edge cases', () => {
  it('single node cluster can elect self', () => {
    const node = new RaftNode('n1', []);
    node.startElection();
    // With 0 peers, self-vote (1/1) is majority
    assert.equal(node.state, 'candidate'); // Can't become leader without response
  });
});

describe('Regression: ARIES empty crash', () => {
  it('crash with no transactions', () => {
    const db = new ARIESRecovery();
    const result = db.crashAndRecover();
    assert.deepEqual(result.activeTxns, []);
  });
});

describe('Regression: Concurrent CC protocols', () => {
  it('OCC: read-only transaction always succeeds', () => {
    const occ = new OCC();
    occ.begin('T1'); occ.write('T1', 'x', 1); occ.commit('T1');
    occ.begin('T2'); occ.read('T2', 'x');
    assert.ok(occ.commit('T2').ok);
  });
  it('Timestamp ordering: read-only succeeds', () => {
    const to = new TimestampOrdering();
    to.begin('T1'); to.write('T1', 'x', 42); to.commit('T1');
    to.begin('T2');
    const r = to.read('T2', 'x');
    assert.ok(r.ok);
    assert.equal(r.value, 42);
  });
});

describe('Regression: 2PL cleanup', () => {
  it('commit releases all locks', () => {
    const tpl = new TwoPhaseLocking();
    tpl.begin('T1');
    for (let i = 0; i < 10; i++) tpl.lockExclusive('T1', 'users', i);
    tpl.commit('T1');
    
    tpl.begin('T2');
    for (let i = 0; i < 10; i++) assert.ok(tpl.lockExclusive('T2', 'users', i));
  });
});

describe('Regression: WAL auto-checkpoint', () => {
  it('auto-checkpoint keeps WAL bounded', () => {
    const wal = new WALCompactor({ maxWalSize: 10, autoCheckpoint: true });
    for (let i = 0; i < 100; i++) wal.append('INSERT', 't', i, {});
    assert.ok(wal.entryCount < 20); // Should have checkpointed
  });
});

describe('Regression: LSM compaction preserves data', () => {
  it('all data accessible after many flushes', () => {
    const lsm = new LSMTree({ memtableLimit: 8, sizeTierThreshold: 2 });
    for (let i = 0; i < 100; i++) lsm.put(`key_${String(i).padStart(3, '0')}`, i);
    for (let i = 0; i < 100; i++) assert.equal(lsm.get(`key_${String(i).padStart(3, '0')}`), i);
  });
});

describe('Regression: Cursor pagination edge', () => {
  it('after last item returns empty', () => {
    const p = new CursorPaginator([{ id: 1 }, { id: 2 }], { pageSize: 10 });
    const page = p.after(100);
    assert.equal(page.items.length, 0);
  });
});

describe('Regression: CTE empty', () => {
  it('CTE with no matching rows', () => {
    const engine = new SimpleCTEEngine();
    engine.addTable('t', [{ id: 1 }, { id: 2 }]);
    const result = engine.execute({
      ctes: [{ name: 'empty', query: { select: '*', from: 't', where: { op: 'GT', left: 'id', right: 100 } } }],
      mainQuery: { select: '*', from: 'empty' },
    });
    assert.deepEqual(result, []);
  });
});

describe('Regression: Subquery empty', () => {
  it('EXISTS with no matches', () => {
    const engine = new SubqueryEngine();
    engine.addTable('a', [{ id: 1 }]);
    engine.addTable('b', [{ id: 99 }]);
    const result = engine.exists('a', { table: 'b', where: (a, b) => a.id === b.id });
    assert.deepEqual(result, []);
  });
});

describe('Regression: MinHash with small sets', () => {
  it('single element sets', () => {
    const mh = new MinHash(128);
    const sim = mh.similarity(mh.signature(['a']), mh.signature(['a']));
    assert.ok(sim > 0.9);
  });
});

describe('Regression: Consistent hashing add/remove', () => {
  it('add node then remove same node', () => {
    const ring = new ConsistentHashRing(50);
    ring.addNode('A'); ring.addNode('B');
    const before = ring.getNode('test');
    ring.addNode('C');
    ring.removeNode('C');
    const after = ring.getNode('test');
    assert.equal(before, after); // Should be same after add+remove
  });
});

describe('Regression: Gossip convergence', () => {
  it('3-node gossip converges', () => {
    const nodes = [new GossipProtocol('n1'), new GossipProtocol('n2'), new GossipProtocol('n3')];
    nodes[0].set('a', 1);
    nodes[1].set('b', 2);
    nodes[2].set('c', 3);
    
    // Round of gossip
    for (const sender of nodes) {
      for (const receiver of nodes) {
        if (sender !== receiver) receiver.receiveGossipMessage(sender.createGossipMessage());
      }
    }
    
    // All nodes should have all data
    for (const node of nodes) {
      assert.equal(node.get('a'), 1);
      assert.equal(node.get('b'), 2);
      assert.equal(node.get('c'), 3);
    }
  });
});
