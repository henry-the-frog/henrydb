// slotted-page.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SlottedPage } from './slotted-page.js';

describe('SlottedPage', () => {
  it('insert and read', () => {
    const page = new SlottedPage(0, 1024);
    const slot = page.insert({ name: 'Alice', age: 30 });
    assert.ok(slot >= 0);
    const record = page.read(slot);
    assert.equal(record.name, 'Alice');
    assert.equal(record.age, 30);
  });

  it('multiple inserts', () => {
    const page = new SlottedPage(0, 1024);
    const s1 = page.insert({ id: 1 });
    const s2 = page.insert({ id: 2 });
    const s3 = page.insert({ id: 3 });
    assert.equal(page.read(s1).id, 1);
    assert.equal(page.read(s2).id, 2);
    assert.equal(page.read(s3).id, 3);
    assert.equal(page.liveRecords, 3);
  });

  it('delete marks as deleted', () => {
    const page = new SlottedPage(0, 1024);
    const s = page.insert({ data: 'hello' });
    assert.ok(page.delete(s));
    assert.equal(page.read(s), null);
    assert.equal(page.liveRecords, 0);
  });

  it('update in-place when fits', () => {
    const page = new SlottedPage(0, 1024);
    const s = page.insert({ name: 'Alice' });
    const s2 = page.update(s, { name: 'Bob' });
    assert.equal(s2, s); // Same slot
    assert.equal(page.read(s).name, 'Bob');
  });

  it('update with larger record', () => {
    const page = new SlottedPage(0, 1024);
    const s = page.insert({ x: 1 });
    const s2 = page.update(s, { x: 1, y: 2, z: 3, description: 'a much longer record' });
    assert.ok(s2 >= 0);
    assert.equal(page.read(s2).description, 'a much longer record');
  });

  it('page fills up', () => {
    const page = new SlottedPage(0, 256);
    let count = 0;
    while (true) {
      const s = page.insert({ id: count, data: 'padding' });
      if (s < 0) break;
      count++;
    }
    assert.ok(count > 0);
    assert.ok(count < 20); // Small page, shouldn't fit too many
  });

  it('iteration', () => {
    const page = new SlottedPage(0, 1024);
    page.insert({ id: 1 });
    page.insert({ id: 2 });
    const s3 = page.insert({ id: 3 });
    page.delete(s3);
    
    const records = [...page];
    assert.equal(records.length, 2);
    assert.equal(records[0].record.id, 1);
  });

  it('compaction reclaims space', () => {
    const page = new SlottedPage(0, 512);
    // Fill page
    const slots = [];
    for (let i = 0; i < 10; i++) {
      const s = page.insert({ id: i, data: 'x'.repeat(20) });
      if (s >= 0) slots.push(s);
    }
    // Delete half
    for (let i = 0; i < slots.length; i += 2) page.delete(slots[i]);
    
    const freeBefore = page.freeSpace;
    // Insert should trigger compaction and succeed
    const s = page.insert({ id: 99, data: 'new record after compaction' });
    assert.ok(s >= 0);
  });

  it('free space and fill factor', () => {
    const page = new SlottedPage(0, 1024);
    assert.ok(page.freeSpace > 0);
    page.insert({ big: 'x'.repeat(200) });
    assert.ok(parseFloat(page.fillFactor) > 0);
  });
});
