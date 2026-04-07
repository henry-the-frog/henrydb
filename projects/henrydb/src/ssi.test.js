// ssi.test.js — Serializable Snapshot Isolation tests
// Proves that SSI detects and prevents serialization anomalies
// that standard Snapshot Isolation allows (like write skew)

import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { SSIManager } from './ssi.js';

describe('SSI: rw-dependency tracking', () => {
  it('records read operations', () => {
    const ssi = new SSIManager();
    const tx = ssi.begin();
    
    ssi.recordRead(tx.txId, 'doctors:1', 1);
    ssi.recordRead(tx.txId, 'doctors:2', 1);
    
    const readSet = ssi.readSets.get(tx.txId);
    assert.equal(readSet.size, 2);
    assert.ok(readSet.has('doctors:1'));
    
    ssi.commit(tx.txId);
  });

  it('detects rw-antidependency when writer follows reader', () => {
    const ssi = new SSIManager();
    
    const t1 = ssi.begin();
    const t2 = ssi.begin();
    
    // T1 reads key X
    ssi.recordRead(t1.txId, 'X', 1);
    
    // T2 writes key X — creates rw-antidependency: T1 →rw→ T2
    ssi.recordWrite(t2.txId, 'X');
    
    const t1Out = ssi.outConflicts.get(t1.txId);
    assert.ok(t1Out.has(t2.txId), 'T1 should have outConflict to T2');
    
    const t2In = ssi.inConflicts.get(t2.txId);
    assert.ok(t2In.has(t1.txId), 'T2 should have inConflict from T1');
    
    ssi.commit(t1.txId);
    ssi.commit(t2.txId);
  });

  it('no false rw-dependency for non-conflicting transactions', () => {
    const ssi = new SSIManager();
    
    const t1 = ssi.begin();
    const t2 = ssi.begin();
    
    // T1 reads X, T2 writes Y — no conflict
    ssi.recordRead(t1.txId, 'X', 1);
    ssi.recordWrite(t2.txId, 'Y');
    
    const t1Out = ssi.outConflicts.get(t1.txId);
    assert.equal(t1Out.size, 0, 'No rw-dependency for disjoint keys');
    
    ssi.commit(t1.txId);
    ssi.commit(t2.txId);
  });
});

describe('SSI: dangerous structure detection', () => {
  it('detects dangerous structure (pivot with in and out conflicts)', () => {
    const ssi = new SSIManager();
    
    const t1 = ssi.begin();
    const t2 = ssi.begin();
    const t3 = ssi.begin();
    
    // Create: T1 →rw→ T2 →rw→ T3
    // T1 reads X, T2 writes X (T1→T2)
    ssi.recordRead(t1.txId, 'X', 1);
    ssi.recordWrite(t2.txId, 'X');
    
    // T2 reads Y, T3 writes Y (T2→T3)
    ssi.recordRead(t2.txId, 'Y', 1);
    ssi.recordWrite(t3.txId, 'Y');
    
    // T2 is the pivot: has inConflict from T1, outConflict to T3
    const t2In = ssi.inConflicts.get(t2.txId);
    const t2Out = ssi.outConflicts.get(t2.txId);
    assert.ok(t2In.has(t1.txId));
    assert.ok(t2Out.has(t3.txId));
    
    ssi.commit(t1.txId);
    ssi.commit(t3.txId);
    
    // T2 should be aborted because it's the pivot of a dangerous structure
    // with both T1 (in) and T3 (out) committed
    assert.throws(
      () => ssi.commit(t2.txId),
      /serialization/i,
      'Should detect dangerous structure and abort T2'
    );
  });
});

describe('SSI: write skew prevention', () => {
  it('prevents the classic doctor on-call write skew', () => {
    const ssi = new SSIManager();
    
    // Setup: Two doctors on call
    // Doctor 1: {name: 'Alice', oncall: 1}
    // Doctor 2: {name: 'Bob', oncall: 1}
    
    const t1 = ssi.begin();
    const t2 = ssi.begin();
    
    // Both read the count of on-call doctors
    ssi.recordRead(t1.txId, 'doctors:1:oncall', 1);
    ssi.recordRead(t1.txId, 'doctors:2:oncall', 1);
    ssi.recordRead(t2.txId, 'doctors:1:oncall', 1);
    ssi.recordRead(t2.txId, 'doctors:2:oncall', 1);
    
    // T1 decides to take Alice off-call (writes to Alice's record)
    t1.writeSet.add('doctors:1:oncall');
    ssi.recordWrite(t1.txId, 'doctors:1:oncall');
    
    // T2 decides to take Bob off-call (writes to Bob's record)
    t2.writeSet.add('doctors:2:oncall');
    ssi.recordWrite(t2.txId, 'doctors:2:oncall');
    
    // T1 commits first — should succeed
    ssi.commit(t1.txId);
    
    // T2 tries to commit — should be aborted!
    // Because:
    // - T2 read doctors:1:oncall (which T1 wrote) → T2 →rw→ T1
    // - T1 read doctors:2:oncall (which T2 wrote) → T1 →rw→ T2
    // This is a cycle: T1 →rw→ T2 →rw→ T1
    // T2 is the pivot with T1 as both in-conflict and out-conflict
    assert.throws(
      () => ssi.commit(t2.txId),
      /serialization/i,
      'SSI should prevent write skew (both doctors going off-call)'
    );
  });

  it('allows write skew when transactions are disjoint', () => {
    const ssi = new SSIManager();
    
    const t1 = ssi.begin();
    const t2 = ssi.begin();
    
    // T1 reads and writes X (different from T2's data)
    ssi.recordRead(t1.txId, 'X', 1);
    t1.writeSet.add('X');
    ssi.recordWrite(t1.txId, 'X');
    
    // T2 reads and writes Y (different from T1's data)
    ssi.recordRead(t2.txId, 'Y', 1);
    t2.writeSet.add('Y');
    ssi.recordWrite(t2.txId, 'Y');
    
    // Both should commit — no conflict
    ssi.commit(t1.txId);
    ssi.commit(t2.txId);
  });

  it('allows sequential transactions that would be serializable', () => {
    const ssi = new SSIManager();
    
    // T1 runs and commits before T2 starts
    const t1 = ssi.begin();
    ssi.recordRead(t1.txId, 'X', 1);
    t1.writeSet.add('Y');
    ssi.recordWrite(t1.txId, 'Y');
    ssi.commit(t1.txId);
    
    // T2 starts after T1 commits — no conflict possible
    const t2 = ssi.begin();
    ssi.recordRead(t2.txId, 'Y', 1);
    t2.writeSet.add('X');
    ssi.recordWrite(t2.txId, 'X');
    ssi.commit(t2.txId); // Should succeed
  });
});

describe('SSI: complex scenarios', () => {
  it('3-transaction cycle with write skew', () => {
    const ssi = new SSIManager();
    
    const t1 = ssi.begin();
    const t2 = ssi.begin();
    const t3 = ssi.begin();
    
    // T1 reads X, writes Y
    ssi.recordRead(t1.txId, 'X', 1);
    t1.writeSet.add('Y');
    ssi.recordWrite(t1.txId, 'Y');
    
    // T2 reads Y, writes Z
    ssi.recordRead(t2.txId, 'Y', 1);
    t2.writeSet.add('Z');
    ssi.recordWrite(t2.txId, 'Z');
    
    // T3 reads Z, writes X
    ssi.recordRead(t3.txId, 'Z', 1);
    t3.writeSet.add('X');
    ssi.recordWrite(t3.txId, 'X');
    
    // T1 and T2 commit
    ssi.commit(t1.txId);
    ssi.commit(t2.txId);
    
    // T3 should be aborted — completes the cycle
    assert.throws(
      () => ssi.commit(t3.txId),
      /serialization/i,
      'Should detect 3-way cycle'
    );
  });

  it('read-only transaction is never aborted', () => {
    const ssi = new SSIManager();
    
    const t1 = ssi.begin();
    const t2 = ssi.begin();
    
    // T1 only reads
    ssi.recordRead(t1.txId, 'X', 1);
    ssi.recordRead(t1.txId, 'Y', 1);
    
    // T2 writes X
    t2.writeSet.add('X');
    ssi.recordWrite(t2.txId, 'X');
    
    // T2 commits
    ssi.commit(t2.txId);
    
    // T1 should commit fine — read-only transactions can't cause write skew
    ssi.commit(t1.txId);
  });

  it('many concurrent transactions with no conflicts all commit', () => {
    const ssi = new SSIManager();
    const txns = [];
    
    for (let i = 0; i < 20; i++) {
      const tx = ssi.begin();
      ssi.recordRead(tx.txId, `key_${i}`, 1);
      tx.writeSet.add(`key_${i}`);
      ssi.recordWrite(tx.txId, `key_${i}`);
      txns.push(tx);
    }
    
    // All should commit — each transaction only touches its own key
    for (const tx of txns) {
      ssi.commit(tx.txId);
    }
  });
});
