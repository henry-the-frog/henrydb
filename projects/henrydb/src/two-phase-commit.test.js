// two-phase-commit.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TwoPhaseCommitCoordinator, Participant, TXN_STATE } from './two-phase-commit.js';

function setup(n = 3) {
  const coord = new TwoPhaseCommitCoordinator();
  for (let i = 0; i < n; i++) {
    coord.addParticipant(new Participant(`node-${i}`));
  }
  return coord;
}

describe('2PC — Happy Path', () => {
  it('commits when all participants vote YES', () => {
    const coord = setup(3);
    const result = coord.execute((participants) => {
      participants.get('node-0').write('x', 1);
      participants.get('node-1').write('y', 2);
      participants.get('node-2').write('z', 3);
    });
    
    assert.ok(result.committed);
    assert.equal(coord.getParticipant('node-0').data.x, 1);
    assert.equal(coord.getParticipant('node-1').data.y, 2);
    assert.equal(coord.getParticipant('node-2').data.z, 3);
  });

  it('multiple transactions in sequence', () => {
    const coord = setup(2);
    
    coord.execute((p) => {
      p.get('node-0').write('counter', 1);
      p.get('node-1').write('counter', 1);
    });
    
    coord.execute((p) => {
      p.get('node-0').write('counter', 2);
      p.get('node-1').write('counter', 2);
    });
    
    assert.equal(coord.getParticipant('node-0').data.counter, 2);
    assert.equal(coord.stats.commits, 2);
  });
});

describe('2PC — Abort Scenarios', () => {
  it('aborts when one participant fails prepare', () => {
    const coord = setup(3);
    coord.getParticipant('node-1').setFailOnPrepare(true);
    
    const result = coord.execute((p) => {
      p.get('node-0').write('x', 1);
      p.get('node-1').write('y', 2);
      p.get('node-2').write('z', 3);
    });
    
    assert.ok(!result.committed);
    assert.ok(result.reason.includes('node-1'));
    
    // NO participant should have committed
    assert.equal(coord.getParticipant('node-0').data.x, undefined);
    assert.equal(coord.getParticipant('node-2').data.z, undefined);
  });

  it('aborts when participant crashes before prepare', () => {
    const coord = setup(3);
    coord.getParticipant('node-2').crash();
    
    const result = coord.execute((p) => {
      p.get('node-0').write('x', 1);
    });
    
    assert.ok(!result.committed);
    assert.ok(result.reason.includes('crashed'));
  });

  it('aborts when transaction body throws', () => {
    const coord = setup(2);
    
    const result = coord.execute(() => {
      throw new Error('application error');
    });
    
    assert.ok(!result.committed);
    assert.ok(result.reason.includes('application error'));
  });

  it('no partial commits: all-or-nothing guarantee', () => {
    const coord = setup(5);
    coord.getParticipant('node-3').setFailOnPrepare(true);
    
    coord.execute((p) => {
      for (let i = 0; i < 5; i++) {
        p.get(`node-${i}`).write('shared_key', 'value');
      }
    });
    
    // ALL participants should have no data
    for (let i = 0; i < 5; i++) {
      const data = coord.getParticipant(`node-${i}`).data;
      assert.equal(data.shared_key, undefined, `node-${i} should have no data`);
    }
  });
});

describe('2PC — Recovery', () => {
  it('participant can recover and see final state', () => {
    const coord = setup(3);
    
    const result = coord.execute((p) => {
      p.get('node-0').write('x', 42);
      p.get('node-1').write('y', 43);
      p.get('node-2').write('z', 44);
    });
    
    assert.ok(result.committed);
    
    // All participants have commit log entries
    for (let i = 0; i < 3; i++) {
      const p = coord.getParticipant(`node-${i}`);
      const hasCommit = p.log.some(e => e.type === 'COMMIT');
      assert.ok(hasCommit, `node-${i} should have COMMIT log entry`);
    }
  });

  it('coordinator logs decisions for recovery', () => {
    const coord = setup(2);
    
    coord.execute((p) => p.get('node-0').write('x', 1));
    
    assert.ok(coord.log.some(e => e.type === 'PREPARE_START'));
    assert.ok(coord.log.some(e => e.type === 'COMMIT_DECISION'));
  });
});

describe('2PC — Stats', () => {
  it('tracks transaction statistics', () => {
    const coord = setup(3);
    
    // 2 successful
    coord.execute((p) => p.get('node-0').write('a', 1));
    coord.execute((p) => p.get('node-0').write('b', 2));
    
    // 1 failed
    coord.getParticipant('node-1').setFailOnPrepare(true);
    coord.execute((p) => p.get('node-0').write('c', 3));
    
    assert.equal(coord.stats.txns, 3);
    assert.equal(coord.stats.commits, 2);
    assert.equal(coord.stats.aborts, 1);
    assert.ok(coord.stats.participantFailures >= 1);
  });
});
