// cost-optimizer.test.js — Tests for cost-based query optimization
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  CostOptimizer,
  seqScanCost,
  filterScanCost,
  btreeIndexScanCost,
  hashIndexScanCost,
  btreePKLookupCost,
} from './cost-optimizer.js';
import { Database } from './db.js';
import { HeapFile } from './page.js';
import { BPlusTree } from './bplus-tree.js';
import { BTreeTable } from './btree-table.js';

describe('Cost functions', () => {
  it('seq scan cost scales with pages and rows', () => {
    const cost10 = seqScanCost(10, 1);
    const cost1000 = seqScanCost(1000, 10);
    const cost100000 = seqScanCost(100000, 1000);
    
    assert.ok(cost10 < cost1000);
    assert.ok(cost1000 < cost100000);
  });

  it('filter scan costs more than plain seq scan', () => {
    assert.ok(filterScanCost(1000, 10) > seqScanCost(1000, 10));
  });

  it('btree index scan cheaper than seq scan for low selectivity', () => {
    // 100K rows, looking for 1 row (selectivity = 0.00001)
    const seqCost = filterScanCost(100000, 1000);
    const idxCost = btreeIndexScanCost(100000, 1000, 0.00001);
    
    assert.ok(idxCost < seqCost, `Index ${idxCost} should be < seq ${seqCost}`);
  });

  it('btree index scan MORE expensive for high selectivity', () => {
    // 100K rows, matching 80% (selectivity = 0.8)
    const seqCost = filterScanCost(100000, 1000);
    const idxCost = btreeIndexScanCost(100000, 1000, 0.8);
    
    // Index with random I/O on 80% of table should be more expensive
    assert.ok(idxCost > seqCost, `Index ${idxCost} should be > seq ${seqCost} at 80% selectivity`);
  });

  it('hash index scan cheaper than btree for equality', () => {
    // Single row match
    const hashCost = hashIndexScanCost(1);
    const btreeCost = btreeIndexScanCost(100000, 1000, 0.00001);
    
    assert.ok(hashCost < btreeCost, `Hash ${hashCost} should be < BTree ${btreeCost}`);
  });

  it('PK lookup is cheapest', () => {
    const pkCost = btreePKLookupCost(3);
    const hashCost = hashIndexScanCost(1);
    const seqCost = filterScanCost(100000, 1000);
    
    assert.ok(pkCost < hashCost, `PK ${pkCost} should be < Hash ${hashCost}`);
    assert.ok(pkCost < seqCost, `PK ${pkCost} should be < Seq ${seqCost}`);
  });
});

describe('CostOptimizer.choosePath', () => {
  it('chooses seq scan for no-WHERE query', () => {
    const heap = new HeapFile('test');
    for (let i = 0; i < 100; i++) heap.insert([i, `val-${i}`]);
    
    const schema = [{ name: 'id', type: 'INTEGER' }, { name: 'val', type: 'TEXT' }];
    const optimizer = new CostOptimizer();
    const path = optimizer.choosePath({ schema, heap, indexes: new Map() }, null, null);
    
    assert.equal(path.type, 'seq_scan');
  });

  it('chooses BTreeTable PK lookup for WHERE pk = value', () => {
    const heap = new BTreeTable('test', { pkIndices: [0] });
    for (let i = 0; i < 1000; i++) heap.insert([i, `val-${i}`]);
    
    const schema = [{ name: 'id', type: 'INTEGER' }, { name: 'val', type: 'TEXT' }];
    const optimizer = new CostOptimizer();
    const where = {
      type: 'COMPARE',
      op: 'EQ',
      left: { type: 'column_ref', name: 'id' },
      right: { type: 'literal', value: 500 },
    };
    
    const path = optimizer.choosePath({ schema, heap, indexes: new Map() }, where, null);
    assert.equal(path.type, 'btree_pk_lookup');
  });

  it('chooses hash index for equality on indexed column', () => {
    const heap = new HeapFile('test');
    for (let i = 0; i < 10000; i++) heap.insert([i, `code-${i}`]);
    
    const schema = [{ name: 'id', type: 'INTEGER' }, { name: 'code', type: 'TEXT' }];
    const index = { _isHash: true }; // Mock hash index
    const indexes = new Map([['code', index]]);
    
    const optimizer = new CostOptimizer();
    const where = {
      type: 'COMPARE',
      op: 'EQ',
      left: { type: 'column_ref', name: 'code' },
      right: { type: 'literal', value: 'code-500' },
    };
    
    const path = optimizer.choosePath({ schema, heap, indexes }, where, null);
    assert.equal(path.type, 'hash_scan');
  });

  it('chooses btree index for equality when no hash', () => {
    const heap = new HeapFile('test');
    for (let i = 0; i < 10000; i++) heap.insert([i, `code-${i}`]);
    
    const schema = [{ name: 'id', type: 'INTEGER' }, { name: 'code', type: 'TEXT' }];
    const index = new BPlusTree(32); // B+tree index
    const indexes = new Map([['code', index]]);
    
    const optimizer = new CostOptimizer();
    const where = {
      type: 'COMPARE',
      op: 'EQ',
      left: { type: 'column_ref', name: 'code' },
      right: { type: 'literal', value: 'code-500' },
    };
    
    const path = optimizer.choosePath({ schema, heap, indexes }, where, null);
    assert.equal(path.type, 'index_scan');
  });

  it('cost ordering: PK lookup < hash < btree index < seq scan', () => {
    // For a point query on 100K rows, costs should be in this order
    const pkCost = btreePKLookupCost(3);
    const hashCost = hashIndexScanCost(1);
    const btreeCost = btreeIndexScanCost(100000, 1000, 1/100000);
    const seqCost = filterScanCost(100000, 1000);
    
    console.log(`  Cost ordering: PK=${pkCost.toFixed(1)}, Hash=${hashCost.toFixed(1)}, BTree=${btreeCost.toFixed(1)}, Seq=${seqCost.toFixed(1)}`);
    assert.ok(pkCost < hashCost);
    assert.ok(hashCost < btreeCost);
    assert.ok(btreeCost < seqCost);
  });

  it('crossover point: where index becomes worse than seq scan', () => {
    const rowCount = 10000;
    const pageCount = 100;
    
    // Find the selectivity where index cost > seq cost
    let crossover = 0;
    for (let s = 0.01; s <= 1.0; s += 0.01) {
      const idxCost = btreeIndexScanCost(rowCount, pageCount, s);
      const seqCost = filterScanCost(rowCount, pageCount);
      if (idxCost > seqCost) {
        crossover = s;
        break;
      }
    }
    
    console.log(`  Index→SeqScan crossover at ${(crossover * 100).toFixed(0)}% selectivity (10K rows)`);
    assert.ok(crossover > 0, 'Should find a crossover point');
    assert.ok(crossover < 0.5, 'Crossover should be before 50%');
  });
});
