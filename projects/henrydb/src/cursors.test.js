// cursors.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { CursorManager } from './cursors.js';

let cm;
const sampleRows = Array.from({ length: 20 }, (_, i) => ({ id: i + 1, name: `Row ${i + 1}` }));

describe('CursorManager', () => {
  beforeEach(() => {
    cm = new CursorManager();
  });

  test('DECLARE creates a cursor', () => {
    const info = cm.declare('my_cursor', sampleRows);
    assert.equal(info.name, 'my_cursor');
    assert.equal(info.isOpen, true);
    assert.equal(info.totalRows, 20);
  });

  test('FETCH NEXT returns rows sequentially', () => {
    cm.declare('c', sampleRows);
    const r1 = cm.fetch('c', 'NEXT');
    assert.equal(r1.rows[0].id, 1);
    const r2 = cm.fetch('c', 'NEXT');
    assert.equal(r2.rows[0].id, 2);
  });

  test('FETCH FORWARD n returns multiple rows', () => {
    cm.declare('c', sampleRows);
    const r = cm.fetch('c', 'FORWARD', 5);
    assert.equal(r.count, 5);
    assert.equal(r.rows[0].id, 1);
    assert.equal(r.rows[4].id, 5);
  });

  test('FETCH ALL returns remaining rows', () => {
    cm.declare('c', sampleRows);
    cm.fetch('c', 'FORWARD', 3); // Skip first 3
    const r = cm.fetch('c', 'ALL');
    assert.equal(r.count, 17);
    assert.equal(r.rows[0].id, 4);
  });

  test('FETCH past end returns empty', () => {
    cm.declare('c', [{ id: 1 }]);
    cm.fetch('c', 'NEXT');
    const r = cm.fetch('c', 'NEXT');
    assert.equal(r.count, 0);
  });

  test('SCROLL cursor supports BACKWARD', () => {
    cm.declare('c', sampleRows, { scroll: true });
    cm.fetch('c', 'FORWARD', 5);
    const r = cm.fetch('c', 'BACKWARD', 2);
    assert.equal(r.count, 2);
    assert.equal(r.rows[0].id, 5); // Position moves back from 5 to 4, returns row at index 4
    assert.equal(r.rows[1].id, 4); // Then back to 3, returns row at index 3
  });

  test('SCROLL cursor supports FIRST', () => {
    cm.declare('c', sampleRows, { scroll: true });
    cm.fetch('c', 'FORWARD', 10);
    const r = cm.fetch('c', 'FIRST');
    assert.equal(r.rows[0].id, 1);
  });

  test('SCROLL cursor supports LAST', () => {
    cm.declare('c', sampleRows, { scroll: true });
    const r = cm.fetch('c', 'LAST');
    assert.equal(r.rows[0].id, 20);
  });

  test('SCROLL cursor supports ABSOLUTE', () => {
    cm.declare('c', sampleRows, { scroll: true });
    const r = cm.fetch('c', 'ABSOLUTE', 10);
    assert.equal(r.rows[0].id, 10);
  });

  test('ABSOLUTE negative indexes from end', () => {
    cm.declare('c', sampleRows, { scroll: true });
    const r = cm.fetch('c', 'ABSOLUTE', -1);
    assert.equal(r.rows[0].id, 20);
  });

  test('RELATIVE moves from current position', () => {
    cm.declare('c', sampleRows, { scroll: true });
    cm.fetch('c', 'FORWARD', 5); // Position at 5
    const r = cm.fetch('c', 'RELATIVE', 3);
    assert.equal(r.rows[0].id, 9); // 5 + 3 + 1 = 9
  });

  test('non-scroll cursor rejects BACKWARD', () => {
    cm.declare('c', sampleRows);
    assert.throws(() => cm.fetch('c', 'BACKWARD'), /not scrollable/);
  });

  test('CLOSE cursor', () => {
    cm.declare('c', sampleRows);
    cm.close('c');
    assert.ok(!cm.has('c'));
  });

  test('CLOSE non-existent throws', () => {
    assert.throws(() => cm.close('nonexistent'), /does not exist/);
  });

  test('FETCH from closed cursor throws', () => {
    cm.declare('c', sampleRows);
    cm.close('c');
    assert.throws(() => cm.fetch('c'), /does not exist/);
  });

  test('closeAll closes everything', () => {
    cm.declare('c1', sampleRows);
    cm.declare('c2', sampleRows);
    const count = cm.closeAll();
    assert.equal(count, 2);
    assert.equal(cm.listCursors().length, 0);
  });

  test('closeNonHoldable keeps WITH HOLD cursors', () => {
    cm.declare('normal', sampleRows);
    cm.declare('holdable', sampleRows, { hold: true });
    const closed = cm.closeNonHoldable();
    assert.equal(closed, 1);
    assert.ok(!cm.has('normal'));
    assert.ok(cm.has('holdable'));
  });

  test('MOVE advances without returning rows', () => {
    cm.declare('c', sampleRows);
    cm.move('c', 'FORWARD', 5);
    const r = cm.fetch('c', 'NEXT');
    assert.equal(r.rows[0].id, 6);
  });

  test('getInfo returns cursor state', () => {
    cm.declare('c', sampleRows, { scroll: true });
    cm.fetch('c', 'FORWARD', 3);
    const info = cm.getInfo('c');
    assert.equal(info.position, 3);
    assert.equal(info.isScrollable, true);
    assert.equal(info.fetchCount, 1);
  });

  test('listCursors returns all active', () => {
    cm.declare('c1', sampleRows);
    cm.declare('c2', sampleRows);
    const list = cm.listCursors();
    assert.equal(list.length, 2);
  });

  test('case-insensitive cursor names', () => {
    cm.declare('MyCursor', sampleRows);
    const r = cm.fetch('mycursor', 'NEXT');
    assert.equal(r.rows[0].id, 1);
  });
});
