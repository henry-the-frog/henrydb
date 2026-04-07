// two-phase-commit.test.js — 2PC protocol tests

import { describe, it } from 'node:test';
import { strict as assert } from 'node:assert';
import { TwoPhaseCoordinator, TwoPhaseParticipant, InMemoryLog, TxState } from './two-phase-commit.js';

describe('2PC: Normal Operation', () => {
  it('all participants vote YES → commit', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2');
    const p3 = new TwoPhaseParticipant('node-3');
    
    const coordinator = new TwoPhaseCoordinator('tx-1', [p1, p2, p3]);
    const result = await coordinator.execute();
    
    assert.equal(result.decision, 'commit');
    assert.equal(coordinator.state, TxState.COMMITTED);
    assert.equal(p1.state, TxState.COMMITTED);
    assert.equal(p2.state, TxState.COMMITTED);
    assert.equal(p3.state, TxState.COMMITTED);
  });

  it('single participant → commit', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const coordinator = new TwoPhaseCoordinator('tx-1', [p1]);
    const result = await coordinator.execute();
    
    assert.equal(result.decision, 'commit');
  });

  it('coordinator logs contain full protocol trace', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2');
    const log = new InMemoryLog();
    
    const coordinator = new TwoPhaseCoordinator('tx-1', [p1, p2], { log });
    await coordinator.execute();
    
    const entries = log.entriesFor('tx-1');
    assert.ok(entries.some(e => e.type === 'prepare-start'));
    assert.ok(entries.some(e => e.type === 'decision' && e.decision === 'commit'));
    assert.ok(entries.some(e => e.type === 'committed'));
  });
});

describe('2PC: Participant Abort', () => {
  it('one participant votes NO → abort', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2', { canPrepare: false });
    const p3 = new TwoPhaseParticipant('node-3');
    
    const coordinator = new TwoPhaseCoordinator('tx-1', [p1, p2, p3]);
    const result = await coordinator.execute();
    
    assert.equal(result.decision, 'abort');
    assert.equal(coordinator.state, TxState.ABORTED);
  });

  it('all participants vote NO → abort', async () => {
    const p1 = new TwoPhaseParticipant('node-1', { canPrepare: false });
    const p2 = new TwoPhaseParticipant('node-2', { canPrepare: false });
    
    const coordinator = new TwoPhaseCoordinator('tx-1', [p1, p2]);
    const result = await coordinator.execute();
    
    assert.equal(result.decision, 'abort');
  });

  it('participant crashes during prepare → abort', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2', { failOnPrepare: true });
    
    const coordinator = new TwoPhaseCoordinator('tx-1', [p1, p2]);
    const result = await coordinator.execute();
    
    assert.equal(result.decision, 'abort');
  });
});

describe('2PC: Timeout Handling', () => {
  it('slow participant causes timeout → abort', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2', { prepareDelay: 10000 }); // Very slow
    
    const coordinator = new TwoPhaseCoordinator('tx-1', [p1, p2]);
    coordinator.timeoutMs = 100; // Short timeout
    
    const result = await coordinator.execute();
    assert.equal(result.decision, 'abort', 'Timeout should cause abort');
  });
});

describe('2PC: Coordinator Recovery', () => {
  it('recovery after commit decision → re-commit', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2');
    const log = new InMemoryLog();
    
    // Simulate: coordinator decided commit but crashed before completing phase 2
    log.append({ txId: 'tx-1', type: 'prepare-start', participants: ['node-1', 'node-2'] });
    log.append({ txId: 'tx-1', type: 'decision', decision: 'commit', votes: { 'node-1': 'yes', 'node-2': 'yes' } });
    
    // Recovery
    const result = await TwoPhaseCoordinator.recover('tx-1', log, [p1, p2]);
    
    assert.equal(result.decision, 'commit');
    assert.ok(result.recovered);
    assert.equal(p1.state, TxState.COMMITTED);
    assert.equal(p2.state, TxState.COMMITTED);
  });

  it('recovery with no decision → abort', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2');
    const log = new InMemoryLog();
    
    // Simulate: coordinator crashed before making a decision
    log.append({ txId: 'tx-1', type: 'prepare-start', participants: ['node-1', 'node-2'] });
    
    // Recovery — no decision found, safe to abort
    const result = await TwoPhaseCoordinator.recover('tx-1', log, [p1, p2]);
    
    assert.equal(result.decision, 'abort');
    assert.ok(result.recovered);
  });

  it('recovery after abort decision → re-abort', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    const p2 = new TwoPhaseParticipant('node-2');
    const log = new InMemoryLog();
    
    log.append({ txId: 'tx-1', type: 'prepare-start', participants: ['node-1', 'node-2'] });
    log.append({ txId: 'tx-1', type: 'decision', decision: 'abort', votes: { 'node-1': 'yes', 'node-2': 'no' } });
    
    const result = await TwoPhaseCoordinator.recover('tx-1', log, [p1, p2]);
    
    assert.equal(result.decision, 'abort');
    assert.ok(result.recovered);
  });
});

describe('2PC: Participant Log', () => {
  it('participant logs prepare and commit', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    
    await p1.prepare('tx-1');
    assert.equal(p1.state, TxState.PREPARED);
    
    await p1.commit('tx-1');
    assert.equal(p1.state, TxState.COMMITTED);
    
    const entries = p1.log.entriesFor('tx-1');
    assert.ok(entries.some(e => e.type === 'vote-yes'));
    assert.ok(entries.some(e => e.type === 'committed'));
  });

  it('participant tracks prepared transactions', async () => {
    const p1 = new TwoPhaseParticipant('node-1');
    
    await p1.prepare('tx-1');
    const txData = p1.transactions.get('tx-1');
    assert.ok(txData);
    assert.equal(txData.state, 'prepared');
    
    await p1.commit('tx-1');
    assert.equal(txData.state, 'committed');
  });
});

describe('2PC: Multiple Concurrent Transactions', () => {
  it('5 independent 2PC transactions all commit', async () => {
    const results = await Promise.all(
      Array.from({ length: 5 }, (_, i) => {
        const participants = [
          new TwoPhaseParticipant(`node-${i}-1`),
          new TwoPhaseParticipant(`node-${i}-2`)
        ];
        const coordinator = new TwoPhaseCoordinator(`tx-${i}`, participants);
        return coordinator.execute();
      })
    );
    
    assert.equal(results.filter(r => r.decision === 'commit').length, 5);
  });

  it('mixed: some commit, some abort', async () => {
    const results = await Promise.all(
      Array.from({ length: 4 }, (_, i) => {
        const canPrepare = i < 2; // First 2 can commit, last 2 have a failing node
        const participants = [
          new TwoPhaseParticipant(`node-${i}-1`),
          new TwoPhaseParticipant(`node-${i}-2`, { canPrepare })
        ];
        const coordinator = new TwoPhaseCoordinator(`tx-${i}`, participants);
        return coordinator.execute();
      })
    );
    
    const commits = results.filter(r => r.decision === 'commit').length;
    const aborts = results.filter(r => r.decision === 'abort').length;
    assert.equal(commits, 2);
    assert.equal(aborts, 2);
  });
});
