// wal-replay.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WAL } from './wal-replay.js';

describe('WAL Replay', () => {
  it('basic redo of committed transaction', () => {
    const wal = new WAL();
    wal.append(1, 'INSERT', 'users', 1, null, { name: 'Alice' });
    wal.append(1, 'INSERT', 'users', 2, null, { name: 'Bob' });
    wal.append(1, 'COMMIT');
    
    const state = wal.replay();
    assert.equal(state.get('users').get(1).name, 'Alice');
    assert.equal(state.get('users').get(2).name, 'Bob');
  });

  it('skip aborted transactions', () => {
    const wal = new WAL();
    wal.append(1, 'INSERT', 'users', 1, null, { name: 'Alice' });
    wal.append(1, 'ABORT');
    
    const state = wal.replay();
    assert.equal(state.has('users'), false);
  });

  it('skip uncommitted transactions', () => {
    const wal = new WAL();
    wal.append(1, 'INSERT', 'users', 1, null, { name: 'Alice' });
    // No COMMIT — crash scenario
    
    const state = wal.replay();
    assert.equal(state.has('users'), false);
  });

  it('undo uncommitted', () => {
    const wal = new WAL();
    wal.append(1, 'INSERT', 'users', 1, null, { name: 'Alice' });
    wal.append(1, 'COMMIT');
    wal.append(2, 'UPDATE', 'users', 1, { name: 'Alice' }, { name: 'Alicia' });
    // Txn 2 not committed
    
    const undone = wal.undo();
    assert.equal(undone.length, 1);
    assert.equal(undone[0].txnId, 2);
  });

  it('multiple tables', () => {
    const wal = new WAL();
    wal.append(1, 'INSERT', 'users', 1, null, { name: 'Alice' });
    wal.append(1, 'INSERT', 'orders', 100, null, { amount: 50 });
    wal.append(1, 'COMMIT');
    
    const state = wal.replay();
    assert.equal(state.get('users').size, 1);
    assert.equal(state.get('orders').size, 1);
  });

  it('delete during replay', () => {
    const wal = new WAL();
    wal.append(1, 'INSERT', 'users', 1, null, { name: 'Alice' });
    wal.append(1, 'COMMIT');
    wal.append(2, 'DELETE', 'users', 1, { name: 'Alice' }, null);
    wal.append(2, 'COMMIT');
    
    const state = wal.replay();
    assert.equal(state.get('users').has(1), false);
  });

  it('checkpoint and truncate', () => {
    const wal = new WAL();
    for (let i = 0; i < 10; i++) {
      wal.append(i, 'INSERT', 't', i, null, { val: i });
      wal.append(i, 'COMMIT');
    }
    const cp = wal.checkpoint();
    assert.equal(wal.length, 21); // 10 inserts + 10 commits + 1 checkpoint
    
    wal.truncate(cp.lsn - 5);
    assert.ok(wal.length < 21);
  });

  it('LSN monotonically increasing', () => {
    const wal = new WAL();
    const e1 = wal.append(1, 'INSERT', 't', 1, null, {});
    const e2 = wal.append(1, 'INSERT', 't', 2, null, {});
    assert.ok(e2.lsn > e1.lsn);
  });

  it('committed count', () => {
    const wal = new WAL();
    wal.append(1, 'INSERT', 't', 1, null, {}); wal.append(1, 'COMMIT');
    wal.append(2, 'INSERT', 't', 2, null, {}); wal.append(2, 'COMMIT');
    wal.append(3, 'INSERT', 't', 3, null, {}); wal.append(3, 'ABORT');
    assert.equal(wal.committedCount, 2);
  });
});
