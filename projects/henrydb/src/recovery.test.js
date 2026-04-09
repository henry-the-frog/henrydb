// recovery.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RecoveryManager } from './recovery.js';
import { LogManager, LogTypes } from './log-record.js';

describe('RecoveryManager', () => {
  it('redo committed transactions', () => {
    const lm = new LogManager();
    lm.write(LogTypes.BEGIN, 1);
    lm.write(LogTypes.UPDATE, 1, 'key1', null, 'value1');
    lm.write(LogTypes.COMMIT, 1);
    
    const store = new Map();
    const rm = new RecoveryManager(lm);
    const redone = rm.redo(store);
    assert.equal(redone, 1);
    assert.equal(store.get('key1'), 'value1');
  });

  it('undo uncommitted transactions', () => {
    const lm = new LogManager();
    lm.write(LogTypes.BEGIN, 1);
    lm.write(LogTypes.UPDATE, 1, 'key1', 'old', 'new');
    // No commit!
    
    const store = new Map([['key1', 'new']]);
    const rm = new RecoveryManager(lm);
    const undone = rm.undo(store);
    assert.equal(undone, 1);
    assert.equal(store.get('key1'), 'old');
  });
});
