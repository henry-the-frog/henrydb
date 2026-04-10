// group-commit-wal.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GroupCommitWAL } from './group-commit-wal.js';

describe('GroupCommitWAL — Basic', () => {
  it('append assigns sequential LSNs', () => {
    const wal = new GroupCommitWAL();
    const l1 = wal.append({ type: 'UPDATE', key: 'a' });
    const l2 = wal.append({ type: 'UPDATE', key: 'b' });
    assert.equal(l1, 1);
    assert.equal(l2, 2);
  });

  it('flush makes entries durable', () => {
    const wal = new GroupCommitWAL();
    wal.append({ type: 'UPDATE', key: 'x', value: 1 });
    wal.append({ type: 'UPDATE', key: 'y', value: 2 });
    
    const count = wal.flush();
    assert.equal(count, 2);
    
    const entries = wal.readFrom(1);
    assert.equal(entries.length, 2);
    assert.equal(entries[0].key, 'x');
  });

  it('commit batches entries with COMMIT record', () => {
    const wal = new GroupCommitWAL({ batchSize: 100 }); // Large batch = no auto-flush
    const result = wal.commit(1, [
      { type: 'UPDATE', key: 'a', value: 1 },
      { type: 'UPDATE', key: 'b', value: 2 },
    ]);
    
    assert.equal(result.lsns.length, 3); // 2 updates + 1 commit
    wal.flush();
    
    const entries = wal.readFrom(1);
    const commit = entries.find(e => e.type === 'COMMIT');
    assert.ok(commit);
    assert.equal(commit.txId, 1);
  });
});

describe('GroupCommitWAL — Batching', () => {
  it('auto-flushes when batch is full', () => {
    const wal = new GroupCommitWAL({ batchSize: 4 });
    
    for (let i = 0; i < 8; i++) {
      wal.append({ type: 'UPDATE', key: `k${i}` });
    }
    
    assert.equal(wal.stats.flushCount, 2); // Should have flushed twice
    assert.equal(wal.stats.totalEntries, 8);
  });

  it('group commit reduces fsyncs', () => {
    const wal = new GroupCommitWAL({ batchSize: 32 });
    
    // 100 transactions, each with 3 ops + commit = 4 entries
    for (let i = 0; i < 100; i++) {
      wal.commit(i, [
        { type: 'UPDATE', key: 'a' },
        { type: 'UPDATE', key: 'b' },
        { type: 'UPDATE', key: 'c' },
      ]);
    }
    wal.flush();
    
    const stats = wal.stats;
    console.log(`    100 txns: ${stats.flushCount} fsyncs (vs 100 without group commit)`);
    console.log(`    Avg batch: ${stats.avgBatchSize.toFixed(1)} entries`);
    assert.ok(stats.flushCount < 100, 'Should batch fsyncs');
    assert.ok(stats.flushCount < 20, 'Should batch effectively');
  });
});

describe('GroupCommitWAL — Benchmark', () => {
  it('10K transactions with group commit', () => {
    const result = GroupCommitWAL.benchmark(10000, 3, 32);
    
    console.log(`    10K txns: ${result.elapsed.toFixed(1)}ms`);
    console.log(`    TPS: ${result.tps.toFixed(0)}`);
    console.log(`    Fsyncs: ${result.flushes} (batch avg: ${result.avgBatchSize.toFixed(1)})`);
    console.log(`    Fsync time: ${result.totalFsyncMs.toFixed(1)}ms (vs ${result.withoutGroupCommit.toFixed(1)}ms without batching)`);
    console.log(`    ${result.savings}`);
    
    assert.ok(result.flushes < result.txns, 'Should batch');
    assert.ok(result.totalFsyncMs < result.withoutGroupCommit, 'Should save fsync time');
  });

  it('different batch sizes affect throughput', () => {
    const sizes = [1, 8, 32, 128, 512];
    console.log('    Batch size comparison:');
    
    for (const batchSize of sizes) {
      const result = GroupCommitWAL.benchmark(10000, 3, batchSize);
      console.log(`      batch=${String(batchSize).padStart(3)}: ${result.flushes} fsyncs, ${result.tps.toFixed(0)} TPS, ${result.savings}`);
    }
  });
});
