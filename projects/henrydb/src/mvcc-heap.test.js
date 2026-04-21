// mvcc-heap.test.js — Unit tests for TransactionalMVCCHeap wrapper class
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TransactionalMVCCHeap, createTransactionalMVCCHeap } from './mvcc-heap.js';

// --- Mock helpers ---

function mockHeap(rows = []) {
  let nextSlot = rows.length;
  const data = new Map();
  for (let i = 0; i < rows.length; i++) {
    data.set(`0:${i}`, rows[i]);
  }

  return {
    _syntheticPageSize: 1000,
    _pkToRid: new Map(),
    *scan() {
      for (const [key, values] of data) {
        const [pageId, slotIdx] = key.split(':').map(Number);
        yield { pageId, slotIdx, values };
      }
    },
    insert(values) {
      const slotIdx = nextSlot++;
      data.set(`0:${slotIdx}`, values);
      return { pageId: 0, slotIdx };
    },
    delete(pageId, slotIdx) {
      data.delete(`${pageId}:${slotIdx}`);
    },
    get(pageId, slotIdx) {
      return data.get(`${pageId}:${slotIdx}`) || null;
    },
    findByPK(pkValue) {
      for (const [, values] of data) {
        if (values[0] === pkValue) return values;
      }
      return null;
    }
  };
}

function mockTdb(tables, versionMaps = new Map(), activeTx = null) {
  return {
    _db: { tables },
    _versionMaps: versionMaps,
    _activeTx: activeTx,
    _mvcc: {
      committedTxns: new Set(),
      isVisible(xid, tx) { return xid <= tx.txId; },
      recordRead: null,
      recordWrite: null
    },
    _visibilityMap: {
      isAllVisible() { return false; },
      onPageModified() {}
    }
  };
}

describe('TransactionalMVCCHeap', () => {
  it('should create wrapper with correct class identity', () => {
    const heap = mockHeap([[1, 'Alice']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const tdb = mockTdb(tables);
    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);

    assert.equal(wrapper._mvccWrapped, true);
    assert.equal(wrapper._tableName, 'users');
    assert.deepEqual(wrapper._pkIndices, [0]);
  });

  it('should provide physical access methods', () => {
    const heap = mockHeap([[1, 'Alice'], [2, 'Bob']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const tdb = mockTdb(tables);
    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);

    // physicalScan returns all rows regardless of MVCC
    const rows = [...wrapper.physicalScan()];
    assert.equal(rows.length, 2);

    // physicalGet
    const val = wrapper.physicalGet(0, 0);
    assert.deepEqual(val, [1, 'Alice']);

    // physicalFindByPK
    const found = wrapper.physicalFindByPK(2);
    assert.deepEqual(found, [2, 'Bob']);
  });

  it('should scan all rows when no version maps exist', () => {
    const heap = mockHeap([[1, 'Alice'], [2, 'Bob']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const tdb = mockTdb(tables);
    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);

    const rows = [...wrapper.scan()];
    assert.equal(rows.length, 2);
  });

  it('should filter deleted rows outside transaction', () => {
    const heap = mockHeap([[1, 'Alice'], [2, 'Bob']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([
      ['0:0', { xmin: 1, xmax: 0 }],
      ['0:1', { xmin: 1, xmax: 2 }]  // deleted by committed tx 2
    ]);
    const tdb = mockTdb(tables, new Map([['users', vm]]));
    tdb._mvcc.committedTxns.add(2);

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    const rows = [...wrapper.scan()];
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0].values, [1, 'Alice']);
  });

  it('should handle MVCC visibility within transaction', () => {
    const heap = mockHeap([[1, 'Alice'], [2, 'Bob'], [3, 'Charlie']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([
      ['0:0', { xmin: 1, xmax: 0 }],    // created by tx1, visible
      ['0:1', { xmin: 5, xmax: 0 }],    // created by tx5, NOT visible to tx3
      ['0:2', { xmin: 2, xmax: 3 }]     // created by tx2, deleted by tx3
    ]);
    const tdb = mockTdb(tables, new Map([['users', vm]]));
    tdb._activeTx = { txId: 3, suppressReadTracking: true, writeSet: new Set(), undoLog: [] };

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    const rows = [...wrapper.scan()];
    // Row 0 (xmin=1 ≤ 3): visible, not deleted → included
    // Row 1 (xmin=5 > 3): not yet created → excluded
    // Row 2 (xmin=2 ≤ 3, xmax=3 ≤ 3): created AND deleted → excluded
    assert.equal(rows.length, 1);
    assert.deepEqual(rows[0].values, [1, 'Alice']);
  });

  it('should deduplicate by PK keeping newest visible version', () => {
    // Simulate two physical versions of same PK (after UPDATE)
    const heap = mockHeap([[1, 'Alice-old'], [2, 'Bob'], [1, 'Alice-new']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([
      ['0:0', { xmin: 1, xmax: 0 }],
      ['0:1', { xmin: 1, xmax: 0 }],
      ['0:2', { xmin: 1, xmax: 0 }]
    ]);
    const tdb = mockTdb(tables, new Map([['users', vm]]));
    tdb._activeTx = { txId: 5, suppressReadTracking: true, writeSet: new Set(), undoLog: [] };

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    const rows = [...wrapper.scan()];
    // PK 1 has two versions — newest (Alice-new) wins
    assert.equal(rows.length, 2);
    const names = rows.map(r => r.values[1]);
    assert.ok(names.includes('Alice-new'));
    assert.ok(names.includes('Bob'));
    assert.ok(!names.includes('Alice-old'));
  });

  it('should mark xmax on MVCC delete (not physical delete)', () => {
    const heap = mockHeap([[1, 'Alice']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([['0:0', { xmin: 1, xmax: 0 }]]);
    const tx = {
      txId: 5,
      writeSet: new Set(),
      undoLog: [],
      manager: { activeTxns: new Map() }
    };
    const tdb = mockTdb(tables, new Map([['users', vm]]), tx);

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    wrapper.delete(0, 0);

    // Version map should be marked, but physical row still exists
    assert.equal(vm.get('0:0').xmax, 5);
    const physicalRows = [...wrapper.physicalScan()];
    assert.equal(physicalRows.length, 1); // Still there physically
  });

  it('should detect write-write conflicts on delete', () => {
    const heap = mockHeap([[1, 'Alice']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([['0:0', { xmin: 1, xmax: 3 }]]); // already marked by tx3
    const otherTx = { txId: 3, committed: false, aborted: false };
    const tx = {
      txId: 5,
      writeSet: new Set(),
      undoLog: [],
      manager: { activeTxns: new Map([[3, otherTx]]) }
    };
    const tdb = mockTdb(tables, new Map([['users', vm]]), tx);

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    assert.throws(() => wrapper.delete(0, 0), /Write-write conflict/);
  });

  it('should forward unknown properties via Proxy', () => {
    const heap = mockHeap([[1, 'Alice']]);
    heap.customProp = 'hello';
    heap.customMethod = () => 42;
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const tdb = mockTdb(tables);

    const wrapper = createTransactionalMVCCHeap(heap, 'users', tdb);
    assert.equal(wrapper.customProp, 'hello');
    assert.equal(wrapper.customMethod(), 42);
    assert.equal(wrapper._mvccWrapped, true);
  });

  it('should provide _origScan/_origDelete for backward compat', () => {
    const heap = mockHeap([[1, 'Alice']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const tdb = mockTdb(tables);

    const wrapper = createTransactionalMVCCHeap(heap, 'users', tdb);
    const origRows = [...wrapper._origScan()];
    assert.equal(origRows.length, 1);
    assert.ok(typeof wrapper._origDelete === 'function');
  });

  it('should handle get() with MVCC visibility', () => {
    const heap = mockHeap([[1, 'Alice']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([['0:0', { xmin: 5, xmax: 0 }]]); // created by future tx
    const tx = {
      txId: 3,
      suppressReadTracking: true,
      writeSet: new Set(),
      undoLog: []
    };
    const tdb = mockTdb(tables, new Map([['users', vm]]), tx);

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    const result = wrapper.get(0, 0);
    assert.equal(result, null); // Not visible to tx3
  });

  it('should handle findByPK with MVCC fallback to scan', () => {
    const heap = mockHeap([[1, 'Alice'], [2, 'Bob']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const tdb = mockTdb(tables);

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    const result = wrapper.findByPK(2);
    assert.deepEqual(result, [2, 'Bob']);
  });

  it('should support delete undo via undoLog', () => {
    const heap = mockHeap([[1, 'Alice']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([['0:0', { xmin: 1, xmax: 0 }]]);
    const tx = {
      txId: 5,
      writeSet: new Set(),
      undoLog: [],
      manager: { activeTxns: new Map() }
    };
    const tdb = mockTdb(tables, new Map([['users', vm]]), tx);

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    wrapper.delete(0, 0);
    assert.equal(vm.get('0:0').xmax, 5);

    // Undo the delete
    tx.undoLog[0]();
    assert.equal(vm.get('0:0').xmax, 0); // Restored
  });

  it('should handle tables without PK (no dedup)', () => {
    const heap = mockHeap([[1, 'Alice'], [1, 'Alice-dup']]);
    const tables = new Map([['logs', { heap, schema: [{}, {}] }]]); // No primaryKey
    const tdb = mockTdb(tables);

    const wrapper = new TransactionalMVCCHeap(heap, 'logs', tdb);
    const rows = [...wrapper.scan()];
    assert.equal(rows.length, 2); // No dedup without PK
  });

  it('should handle non-transaction delete (physical)', () => {
    const heap = mockHeap([[1, 'Alice'], [2, 'Bob']]);
    const tables = new Map([['users', { heap, schema: [{ primaryKey: true }, {}] }]]);
    const vm = new Map([
      ['0:0', { xmin: 1, xmax: 0 }],
      ['0:1', { xmin: 1, xmax: 0 }]
    ]);
    const tdb = mockTdb(tables, new Map([['users', vm]]));
    // No active transaction
    tdb._activeTx = null;

    const wrapper = new TransactionalMVCCHeap(heap, 'users', tdb);
    wrapper.delete(0, 0);

    // Version map should be marked -1 (permanently deleted)
    assert.equal(vm.get('0:0').xmax, -1);
    // Physical row should be removed
    assert.equal(wrapper.physicalGet(0, 0), null);
  });
});
