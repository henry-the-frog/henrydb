// log-record.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LogManager, LogTypes } from './log-record.js';

describe('LogManager', () => {
  it('write and retrieve', () => {
    const lm = new LogManager();
    const lsn1 = lm.write(LogTypes.BEGIN, 1);
    const lsn2 = lm.write(LogTypes.UPDATE, 1, 5, 'old', 'new');
    const lsn3 = lm.write(LogTypes.COMMIT, 1);
    
    assert.equal(lm.size, 3);
    assert.equal(lm.get(lsn2).before, 'old');
    assert.equal(lm.get(lsn2).after, 'new');
  });

  it('getByTx', () => {
    const lm = new LogManager();
    lm.write(LogTypes.BEGIN, 1);
    lm.write(LogTypes.BEGIN, 2);
    lm.write(LogTypes.UPDATE, 1, 5, null, 'x');
    
    assert.equal(lm.getByTx(1).length, 2);
    assert.equal(lm.getByTx(2).length, 1);
  });
});
