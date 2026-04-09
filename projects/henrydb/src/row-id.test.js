// row-id.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { RowId } from './row-id.js';

describe('RowId', () => {
  it('create and compare', () => {
    const a = new RowId(5, 3);
    const b = new RowId(5, 3);
    assert.equal(a.equals(b), true);
    assert.equal(a.toString(), '5:3');
  });

  it('parse from string', () => {
    const rid = RowId.parse('10:7');
    assert.equal(rid.pageId, 10);
    assert.equal(rid.slotId, 7);
  });
});
