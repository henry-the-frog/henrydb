// wal-compaction.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { WALCompactor } from './wal-compaction.js';

describe('WAL Compaction', () => {
  it('append entries', () => {
    const wal = new WALCompactor({ autoCheckpoint: false });
    const lsn1 = wal.append('INSERT', 'users', 1, { name: 'Alice' });
    const lsn2 = wal.append('UPDATE', 'users', 1, { name: 'Bob' });
    assert.equal(lsn1, 1);
    assert.equal(lsn2, 2);
    assert.equal(wal.entryCount, 2);
  });

  it('checkpoint and truncate', () => {
    const wal = new WALCompactor({ autoCheckpoint: false });
    for (let i = 0; i < 10; i++) wal.append('INSERT', 'users', i, { i });
    
    assert.equal(wal.entryCount, 10);
    const cpLSN = wal.checkpoint();
    assert.equal(cpLSN, 10);
    assert.equal(wal.entryCount, 0); // Truncated after checkpoint
    assert.equal(wal.stats.checkpoints, 1);
    assert.equal(wal.stats.truncations, 1);
  });

  it('auto-checkpoint at threshold', () => {
    const wal = new WALCompactor({ maxWalSize: 5, autoCheckpoint: true });
    for (let i = 0; i < 10; i++) wal.append('INSERT', 't', i, {});
    
    assert.ok(wal.stats.checkpoints >= 1);
    assert.ok(wal.entryCount < 10);
  });

  it('replay from LSN', () => {
    const wal = new WALCompactor({ autoCheckpoint: false });
    for (let i = 0; i < 5; i++) wal.append('INSERT', 'users', i, { v: i });
    
    const replay = wal.replay(3); // From LSN 3
    assert.equal(replay.length, 2); // LSN 4 and 5
    assert.equal(replay[0].lsn, 4);
  });

  it('cannot truncate beyond checkpoint', () => {
    const wal = new WALCompactor({ autoCheckpoint: false });
    for (let i = 0; i < 10; i++) wal.append('INSERT', 't', i, {});
    
    // No checkpoint yet — truncate should do nothing
    const removed = wal.truncate(10);
    assert.equal(removed, 0);
    assert.equal(wal.entryCount, 10);
    
    // After checkpoint, can truncate
    wal.checkpoint();
    assert.equal(wal.entryCount, 0);
  });

  it('transaction entries filter', () => {
    const wal = new WALCompactor({ autoCheckpoint: false });
    wal.append('INSERT', 't', 1, {}, 'txn1');
    wal.append('INSERT', 't', 2, {}, 'txn2');
    wal.append('UPDATE', 't', 1, {}, 'txn1');
    
    const txn1 = wal.getTransactionEntries('txn1');
    assert.equal(txn1.length, 2);
  });

  it('table entries filter', () => {
    const wal = new WALCompactor({ autoCheckpoint: false });
    wal.append('INSERT', 'users', 1, {});
    wal.append('INSERT', 'orders', 1, {});
    wal.append('INSERT', 'users', 2, {});
    
    assert.equal(wal.getTableEntries('users').length, 2);
    assert.equal(wal.getTableEntries('orders').length, 1);
  });

  it('stats tracking', () => {
    const wal = new WALCompactor({ maxWalSize: 50, autoCheckpoint: true });
    for (let i = 0; i < 200; i++) wal.append('INSERT', 't', i, {});
    
    const stats = wal.getStats();
    assert.equal(stats.entriesWritten, 200);
    assert.ok(stats.checkpointCount >= 3);
    assert.ok(stats.bytesReclaimed > 0);
  });
});
