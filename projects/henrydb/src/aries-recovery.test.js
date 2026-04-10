// aries-recovery.test.js — Tests for ARIES recovery protocol
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  LOG_TYPE, LogRecord, WriteAheadLog, ARIESRecovery, InMemoryPageStore,
} from './aries-recovery.js';

// Helper: simulate a normal database operation sequence
function runNormal(wal, pages) {
  // Transaction 1: begin, update page 1, update page 2, commit
  wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
  wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: 'A', after: 'B' });
  pages.applyRedo('P1', 'B', wal.nextLsn - 1);
  wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P2', before: 'X', after: 'Y' });
  pages.applyRedo('P2', 'Y', wal.nextLsn - 1);
  wal.append({ type: LOG_TYPE.COMMIT, txId: 1 });
  wal.append({ type: LOG_TYPE.END, txId: 1 });
}

describe('WriteAheadLog', () => {
  it('assigns sequential LSNs', () => {
    const wal = new WriteAheadLog();
    const lsn1 = wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    const lsn2 = wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1' });
    assert.equal(lsn1, 1);
    assert.equal(lsn2, 2);
    assert.equal(wal.records[1].prevLsn, 1); // prevLsn chain
  });

  it('tracks prevLsn chain per transaction', () => {
    const wal = new WriteAheadLog();
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.BEGIN, txId: 2 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1' });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 2, pageId: 'P2' });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P3' });
    
    // tx1 chain: BEGIN(1) → UPDATE(3) → UPDATE(5)
    assert.equal(wal.records[2].prevLsn, 1); // UPDATE P1 prev = BEGIN
    assert.equal(wal.records[4].prevLsn, 3); // UPDATE P3 prev = UPDATE P1
    
    // tx2 chain: BEGIN(2) → UPDATE(4)
    assert.equal(wal.records[3].prevLsn, 2); // UPDATE P2 prev = BEGIN
  });

  it('finds last checkpoint', () => {
    const wal = new WriteAheadLog();
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.CHECKPOINT, activeTxns: new Map([[1, { status: 'active', lastLsn: 1 }]]), dirtyPages: new Map() });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1' });
    
    const cp = wal.lastCheckpoint();
    assert.equal(cp.lsn, 2);
    assert.ok(cp.activeTxns.has(1));
  });
});

describe('ARIES Recovery — Phase 1: Analysis', () => {
  it('reconstructs ATT and DPT from log', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    // tx1: committed
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: 'A', after: 'B' });
    wal.append({ type: LOG_TYPE.COMMIT, txId: 1 });
    
    // tx2: still active (crash before commit)
    wal.append({ type: LOG_TYPE.BEGIN, txId: 2 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 2, pageId: 'P2', before: 'X', after: 'Y' });
    // NO COMMIT — simulates crash
    
    const recovery = new ARIESRecovery(wal, pages);
    recovery._analysis();
    
    // tx1 should be committed
    assert.equal(recovery.activeTxnTable.get(1)?.status, 'committed');
    // tx2 should be active
    assert.equal(recovery.activeTxnTable.get(2)?.status, 'active');
    // DPT should have P1 and P2
    assert.ok(recovery.dirtyPageTable.has('P1'));
    assert.ok(recovery.dirtyPageTable.has('P2'));
  });
});

describe('ARIES Recovery — Phase 2: Redo', () => {
  it('redo replays changes to pages', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: null, after: 'data1' });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P2', before: null, after: 'data2' });
    wal.append({ type: LOG_TYPE.COMMIT, txId: 1 });
    // Simulate crash: pages never got the data
    
    const recovery = new ARIESRecovery(wal, pages);
    recovery._analysis();
    recovery._redo();
    
    assert.equal(pages.getData('P1'), 'data1');
    assert.equal(pages.getData('P2'), 'data2');
    assert.ok(recovery.stats.redoRecords >= 2);
  });

  it('redo skips already-applied changes', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: null, after: 'data' });
    
    // Simulate: page already has this change (pageLSN = 2)
    pages.applyRedo('P1', 'data', 2);
    
    wal.append({ type: LOG_TYPE.COMMIT, txId: 1 });
    
    const recovery = new ARIESRecovery(wal, pages);
    recovery._analysis();
    recovery._redo();
    
    // Should not redo (pageLSN >= record LSN)
    assert.equal(recovery.stats.redoRecords, 0);
  });
});

describe('ARIES Recovery — Phase 3: Undo', () => {
  it('undo rolls back uncommitted transactions', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    // tx1: committed
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: 'old1', after: 'new1' });
    wal.append({ type: LOG_TYPE.COMMIT, txId: 1 });
    
    // tx2: uncommitted (crash)
    wal.append({ type: LOG_TYPE.BEGIN, txId: 2 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 2, pageId: 'P2', before: 'old2', after: 'new2' });
    // NO COMMIT
    
    const recovery = new ARIESRecovery(wal, pages);
    recovery._analysis();
    recovery._redo();
    
    // After redo: P1=new1, P2=new2 (redo replays everything)
    assert.equal(pages.getData('P1'), 'new1');
    assert.equal(pages.getData('P2'), 'new2');
    
    recovery._undo();
    
    // After undo: P2 should be rolled back to old2
    assert.equal(pages.getData('P1'), 'new1'); // tx1 committed — kept
    assert.equal(pages.getData('P2'), 'old2'); // tx2 uncommitted — undone
    assert.ok(recovery.stats.undoRecords >= 1);
    assert.ok(recovery.stats.clrsWritten >= 1);
  });

  it('undo writes CLR records', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: 'old', after: 'new' });
    // Crash without commit
    
    const recovery = new ARIESRecovery(wal, pages);
    recovery.recover();
    
    // Check that CLR was written to the log
    const clrs = wal.records.filter(r => r.type === LOG_TYPE.CLR);
    assert.ok(clrs.length >= 1, 'CLR should be written during undo');
    assert.equal(clrs[0].txId, 1);
    assert.equal(clrs[0].pageId, 'P1');
  });

  it('undo handles multiple updates in one transaction', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: 'A', after: 'B' });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P2', before: 'C', after: 'D' });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: 'B', after: 'E' });
    // Crash
    
    const recovery = new ARIESRecovery(wal, pages);
    recovery.recover();
    
    // Undo should walk backward: E→B, D→C, B→A
    assert.equal(pages.getData('P1'), 'A');
    assert.equal(pages.getData('P2'), 'C');
  });
});

describe('ARIES Recovery — Full Recovery', () => {
  it('full recovery scenario with multiple transactions', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    // tx1: committed
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'accounts', before: 1000, after: 800 });
    wal.append({ type: LOG_TYPE.COMMIT, txId: 1 });
    wal.append({ type: LOG_TYPE.END, txId: 1 });
    
    // tx2: committed
    wal.append({ type: LOG_TYPE.BEGIN, txId: 2 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 2, pageId: 'accounts', before: 800, after: 600 });
    wal.append({ type: LOG_TYPE.COMMIT, txId: 2 });
    wal.append({ type: LOG_TYPE.END, txId: 2 });
    
    // tx3: still running (crash)
    wal.append({ type: LOG_TYPE.BEGIN, txId: 3 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 3, pageId: 'accounts', before: 600, after: 100 });
    // CRASH — no commit
    
    const recovery = new ARIESRecovery(wal, pages);
    const stats = recovery.recover();
    
    // After recovery: account should be 600 (tx3 undone)
    assert.equal(pages.getData('accounts'), 600);
    
    console.log(`  Recovery stats: analysis=${stats.analysisRecords}, redo=${stats.redoRecords}, undo=${stats.undoRecords}, CLRs=${stats.clrsWritten}`);
  });

  it('recovery with checkpoint', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    // Early work
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: null, after: 'v1' });
    pages.applyRedo('P1', 'v1', 2);
    wal.append({ type: LOG_TYPE.COMMIT, txId: 1 });
    wal.append({ type: LOG_TYPE.END, txId: 1 });
    
    // Checkpoint
    wal.append({ type: LOG_TYPE.CHECKPOINT, 
      activeTxns: new Map(),
      dirtyPages: new Map([['P1', 2]]),
    });
    
    // More work after checkpoint
    wal.append({ type: LOG_TYPE.BEGIN, txId: 2 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 2, pageId: 'P1', before: 'v1', after: 'v2' });
    wal.append({ type: LOG_TYPE.COMMIT, txId: 2 });
    wal.append({ type: LOG_TYPE.END, txId: 2 });
    
    // tx3 uncommitted
    wal.append({ type: LOG_TYPE.BEGIN, txId: 3 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 3, pageId: 'P1', before: 'v2', after: 'v3' });
    // Crash
    
    const recovery = new ARIESRecovery(wal, new InMemoryPageStore());
    const stats = recovery.recover();
    
    assert.equal(recovery.pageStore.getData('P1'), 'v2'); // tx3 undone, tx2 kept
  });

  it('recovery is idempotent (can run twice)', () => {
    const wal = new WriteAheadLog();
    const pages = new InMemoryPageStore();
    
    wal.append({ type: LOG_TYPE.BEGIN, txId: 1 });
    wal.append({ type: LOG_TYPE.UPDATE, txId: 1, pageId: 'P1', before: 'old', after: 'new' });
    // Crash
    
    // First recovery
    const r1 = new ARIESRecovery(wal, pages);
    r1.recover();
    assert.equal(pages.getData('P1'), 'old');
    
    // Second recovery (simulate crash during recovery)
    const r2 = new ARIESRecovery(wal, pages);
    r2.recover();
    assert.equal(pages.getData('P1'), 'old'); // Same result
  });
});
