// logical-replication.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { LogicalReplicationManager } from './logical-replication.js';

let lrm;

describe('LogicalReplicationManager', () => {
  beforeEach(() => {
    lrm = new LogicalReplicationManager();
  });

  describe('Publications', () => {
    test('create publication for specific tables', () => {
      const pub = lrm.createPublication('my_pub', { tables: ['users', 'orders'] });
      assert.equal(pub.name, 'my_pub');
      assert.deepEqual(pub.tables, ['users', 'orders']);
    });

    test('create publication for all tables', () => {
      const pub = lrm.createPublication('all_pub', { allTables: true });
      assert.equal(pub.allTables, true);
    });

    test('publication with specific operations', () => {
      const pub = lrm.createPublication('inserts_only', {
        tables: ['logs'],
        operations: ['INSERT'],
      });
      assert.deepEqual(pub.operations, ['INSERT']);
    });

    test('drop publication', () => {
      lrm.createPublication('temp');
      lrm.dropPublication('temp');
      assert.throws(() => lrm.dropPublication('temp'));
    });

    test('alter publication adds/removes tables', () => {
      lrm.createPublication('mutable', { tables: ['users'] });
      lrm.alterPublication('mutable', { addTables: ['orders'] });
      lrm.alterPublication('mutable', { removeTables: ['users'] });
      // Verify by using it
    });
  });

  describe('Subscriptions', () => {
    test('create subscription', () => {
      lrm.createPublication('pub1', { tables: ['users'] });
      const sub = lrm.createSubscription('sub1', 'pub1');
      assert.equal(sub.name, 'sub1');
      assert.equal(sub.publication, 'pub1');
    });

    test('enable/disable subscription', () => {
      lrm.createPublication('pub1', { tables: ['users'] });
      lrm.createSubscription('sub1', 'pub1');
      lrm.disableSubscription('sub1');
      
      // Should not receive changes while disabled
      lrm.captureChange('users', 'INSERT', { id: 1 });
      assert.equal(lrm.poll('sub1').length, 0);
      
      lrm.enableSubscription('sub1');
      lrm.captureChange('users', 'INSERT', { id: 2 });
      assert.equal(lrm.poll('sub1').length, 1);
    });
  });

  describe('Change Data Capture', () => {
    test('captures INSERT events', () => {
      lrm.createPublication('pub', { tables: ['users'] });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('users', 'INSERT', { id: 1, name: 'Alice' });
      
      const events = lrm.poll('sub');
      assert.equal(events.length, 1);
      assert.equal(events[0].operation, 'INSERT');
      assert.equal(events[0].table, 'users');
      assert.deepEqual(events[0].newRow, { id: 1, name: 'Alice' });
    });

    test('captures UPDATE with old and new rows', () => {
      lrm.createPublication('pub', { tables: ['users'] });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('users', 'UPDATE',
        { id: 1, name: 'Alice Updated' },
        { id: 1, name: 'Alice' }
      );

      const events = lrm.poll('sub');
      assert.equal(events[0].operation, 'UPDATE');
      assert.equal(events[0].newRow.name, 'Alice Updated');
      assert.equal(events[0].oldRow.name, 'Alice');
    });

    test('captures DELETE with old row', () => {
      lrm.createPublication('pub', { tables: ['users'] });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('users', 'DELETE', null, { id: 1, name: 'Alice' });

      const events = lrm.poll('sub');
      assert.equal(events[0].operation, 'DELETE');
      assert.equal(events[0].oldRow.name, 'Alice');
      assert.equal(events[0].newRow, null);
    });

    test('only publishes matching tables', () => {
      lrm.createPublication('pub', { tables: ['users'] });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('users', 'INSERT', { id: 1 });
      lrm.captureChange('orders', 'INSERT', { id: 1 }); // Different table
      
      const events = lrm.poll('sub');
      assert.equal(events.length, 1);
      assert.equal(events[0].table, 'users');
    });

    test('only publishes matching operations', () => {
      lrm.createPublication('pub', { tables: ['users'], operations: ['INSERT'] });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('users', 'INSERT', { id: 1 });
      lrm.captureChange('users', 'DELETE', null, { id: 1 });
      
      const events = lrm.poll('sub');
      assert.equal(events.length, 1);
      assert.equal(events[0].operation, 'INSERT');
    });

    test('multiple subscribers receive same events', () => {
      lrm.createPublication('pub', { allTables: true });
      lrm.createSubscription('sub1', 'pub');
      lrm.createSubscription('sub2', 'pub');

      lrm.captureChange('users', 'INSERT', { id: 1 });

      assert.equal(lrm.poll('sub1').length, 1);
      assert.equal(lrm.poll('sub2').length, 1);
    });

    test('LSN increases monotonically', () => {
      lrm.createPublication('pub', { allTables: true });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('t', 'INSERT', { id: 1 });
      lrm.captureChange('t', 'INSERT', { id: 2 });
      lrm.captureChange('t', 'INSERT', { id: 3 });

      const events = lrm.poll('sub');
      assert.ok(events[0].lsn < events[1].lsn);
      assert.ok(events[1].lsn < events[2].lsn);
    });
  });

  describe('Replication Slots', () => {
    test('confirm advances slot position', () => {
      lrm.createPublication('pub', { allTables: true });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('t', 'INSERT', { id: 1 });
      lrm.captureChange('t', 'INSERT', { id: 2 });

      const events = lrm.poll('sub');
      lrm.confirm('sub', events[1].lsn);

      const status = lrm.getSlotStatus('sub');
      assert.equal(status.confirmedLSN, events[1].lsn);
      assert.equal(status.lag, 0);
    });

    test('slot tracks lag', () => {
      lrm.createPublication('pub', { allTables: true });
      lrm.createSubscription('sub', 'pub');

      lrm.captureChange('t', 'INSERT', { id: 1 });
      lrm.captureChange('t', 'INSERT', { id: 2 });

      const status = lrm.getSlotStatus('sub');
      assert.equal(status.lag, 2); // 2 events not confirmed
    });
  });

  describe('Change Log', () => {
    test('getChangeLog returns all events', () => {
      lrm.captureChange('users', 'INSERT', { id: 1 });
      lrm.captureChange('orders', 'INSERT', { id: 1 });
      
      assert.equal(lrm.getChangeLog().length, 2);
    });

    test('filter by table', () => {
      lrm.captureChange('users', 'INSERT', { id: 1 });
      lrm.captureChange('orders', 'INSERT', { id: 1 });
      
      assert.equal(lrm.getChangeLog({ table: 'users' }).length, 1);
    });

    test('filter since LSN', () => {
      lrm.captureChange('t', 'INSERT', { id: 1 });
      lrm.captureChange('t', 'INSERT', { id: 2 });
      lrm.captureChange('t', 'INSERT', { id: 3 });
      
      assert.equal(lrm.getChangeLog({ since: 1 }).length, 2);
    });

    test('filter by operation', () => {
      lrm.captureChange('t', 'INSERT', { id: 1 });
      lrm.captureChange('t', 'UPDATE', { id: 1 }, { id: 1 });
      lrm.captureChange('t', 'DELETE', null, { id: 1 });
      
      assert.equal(lrm.getChangeLog({ operation: 'INSERT' }).length, 1);
    });
  });

  describe('Callbacks', () => {
    test('subscription callback fires on change', () => {
      lrm.createPublication('pub', { allTables: true });
      
      const received = [];
      lrm.createSubscription('sub', 'pub', {
        callback: (event) => received.push(event.toJSON()),
      });

      lrm.captureChange('users', 'INSERT', { id: 1 });
      lrm.captureChange('users', 'INSERT', { id: 2 });

      assert.equal(received.length, 2);
    });
  });

  test('stats tracking', () => {
    lrm.createPublication('pub', { allTables: true });
    lrm.createSubscription('sub', 'pub');

    lrm.captureChange('t', 'INSERT', { id: 1 });
    lrm.captureChange('t', 'INSERT', { id: 2 });

    const stats = lrm.getStats();
    assert.equal(stats.totalChanges, 2);
    assert.equal(stats.totalDeliveries, 2);
    assert.equal(stats.publications, 1);
    assert.equal(stats.subscriptions, 1);
  });
});
