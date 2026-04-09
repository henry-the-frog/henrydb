// logical-replication.js — Logical replication and Change Data Capture (CDC)
// Publisher/subscriber model with WAL-based event streaming.

import { EventEmitter } from 'node:events';

/**
 * ChangeEvent — represents a single data change.
 */
class ChangeEvent {
  constructor(options) {
    this.lsn = options.lsn; // Log Sequence Number
    this.timestamp = options.timestamp || Date.now();
    this.table = options.table;
    this.operation = options.operation; // INSERT, UPDATE, DELETE
    this.newRow = options.newRow || null;
    this.oldRow = options.oldRow || null;
    this.transactionId = options.transactionId || null;
  }

  toJSON() {
    return {
      lsn: this.lsn,
      timestamp: this.timestamp,
      table: this.table,
      operation: this.operation,
      newRow: this.newRow,
      oldRow: this.oldRow,
      transactionId: this.transactionId,
    };
  }
}

/**
 * Publication — defines which tables and operations are published.
 */
class Publication {
  constructor(name, options = {}) {
    this.name = name;
    this.tables = new Set(options.tables || []);
    this.allTables = options.allTables || false;
    this.operations = new Set(options.operations || ['INSERT', 'UPDATE', 'DELETE']);
    this.createdAt = Date.now();
  }

  includes(table, operation) {
    if (!this.allTables && !this.tables.has(table.toLowerCase())) return false;
    return this.operations.has(operation.toUpperCase());
  }
}

/**
 * ReplicationSlot — tracks subscriber position in the change stream.
 */
class ReplicationSlot {
  constructor(name) {
    this.name = name;
    this.confirmedLSN = 0; // Last confirmed position
    this.sentLSN = 0;      // Last sent position
    this.active = false;
    this.createdAt = Date.now();
    this.statistics = {
      totalSent: 0,
      totalConfirmed: 0,
      bytesTransferred: 0,
    };
  }
}

/**
 * Subscription — a subscriber consuming from a publication.
 */
class Subscription {
  constructor(name, publication, options = {}) {
    this.name = name;
    this.publication = publication;
    this.slot = new ReplicationSlot(`${name}_slot`);
    this.enabled = options.enabled !== false;
    this.callback = options.callback || null;
    this.buffer = []; // Buffered events not yet delivered
    this.createdAt = Date.now();
  }
}

/**
 * LogicalReplicationManager — manages publications, subscriptions, and CDC.
 */
export class LogicalReplicationManager extends EventEmitter {
  constructor() {
    super();
    this._publications = new Map(); // name → Publication
    this._subscriptions = new Map(); // name → Subscription
    this._changeLog = []; // All change events (the WAL)
    this._nextLSN = 1;
    this._nextTxId = 1;
    this._stats = {
      totalChanges: 0,
      totalDeliveries: 0,
    };
  }

  // --- Publication Management ---

  createPublication(name, options = {}) {
    if (this._publications.has(name)) {
      throw new Error(`Publication '${name}' already exists`);
    }
    const pub = new Publication(name, options);
    this._publications.set(name, pub);
    return { name, tables: [...pub.tables], allTables: pub.allTables, operations: [...pub.operations] };
  }

  dropPublication(name, ifExists = false) {
    if (!this._publications.has(name)) {
      if (ifExists) return false;
      throw new Error(`Publication '${name}' does not exist`);
    }
    this._publications.delete(name);
    return true;
  }

  alterPublication(name, changes) {
    const pub = this._publications.get(name);
    if (!pub) throw new Error(`Publication '${name}' does not exist`);

    if (changes.addTables) {
      for (const t of changes.addTables) pub.tables.add(t.toLowerCase());
    }
    if (changes.removeTables) {
      for (const t of changes.removeTables) pub.tables.delete(t.toLowerCase());
    }
    if (changes.operations) {
      pub.operations.clear();
      for (const op of changes.operations) pub.operations.add(op);
    }
  }

  // --- Subscription Management ---

  createSubscription(name, publicationName, options = {}) {
    if (this._subscriptions.has(name)) {
      throw new Error(`Subscription '${name}' already exists`);
    }
    const pub = this._publications.get(publicationName);
    if (!pub) throw new Error(`Publication '${publicationName}' does not exist`);

    const sub = new Subscription(name, pub, options);
    this._subscriptions.set(name, sub);
    return { name, publication: publicationName, enabled: sub.enabled };
  }

  dropSubscription(name, ifExists = false) {
    if (!this._subscriptions.has(name)) {
      if (ifExists) return false;
      throw new Error(`Subscription '${name}' does not exist`);
    }
    this._subscriptions.delete(name);
    return true;
  }

  enableSubscription(name) {
    const sub = this._subscriptions.get(name);
    if (!sub) throw new Error(`Subscription '${name}' does not exist`);
    sub.enabled = true;
  }

  disableSubscription(name) {
    const sub = this._subscriptions.get(name);
    if (!sub) throw new Error(`Subscription '${name}' does not exist`);
    sub.enabled = false;
  }

  // --- Change Capture ---

  /**
   * Record a change event (called by the database engine on DML).
   */
  captureChange(table, operation, newRow = null, oldRow = null, txId = null) {
    const event = new ChangeEvent({
      lsn: this._nextLSN++,
      table: table.toLowerCase(),
      operation: operation.toUpperCase(),
      newRow,
      oldRow,
      transactionId: txId || this._nextTxId++,
    });

    this._changeLog.push(event);
    this._stats.totalChanges++;

    // Deliver to matching subscriptions
    for (const sub of this._subscriptions.values()) {
      if (!sub.enabled) continue;
      if (!sub.publication.includes(table, operation)) continue;

      sub.buffer.push(event);
      sub.slot.sentLSN = event.lsn;
      sub.slot.statistics.totalSent++;
      this._stats.totalDeliveries++;

      // Call subscriber callback if registered
      if (sub.callback) {
        try {
          sub.callback(event);
        } catch {}
      }
    }

    this.emit('change', event);
    return event;
  }

  // --- Consuming Changes ---

  /**
   * Poll for new changes on a subscription.
   */
  poll(subscriptionName, maxEvents = 100) {
    const sub = this._subscriptions.get(subscriptionName);
    if (!sub) throw new Error(`Subscription '${subscriptionName}' does not exist`);

    const events = sub.buffer.splice(0, maxEvents);
    return events.map(e => e.toJSON());
  }

  /**
   * Confirm receipt of changes up to a given LSN.
   */
  confirm(subscriptionName, lsn) {
    const sub = this._subscriptions.get(subscriptionName);
    if (!sub) throw new Error(`Subscription '${subscriptionName}' does not exist`);

    sub.slot.confirmedLSN = lsn;
    sub.slot.statistics.totalConfirmed++;
  }

  /**
   * Get the full change log (for debugging/auditing).
   */
  getChangeLog(options = {}) {
    let log = this._changeLog;
    if (options.table) {
      log = log.filter(e => e.table === options.table.toLowerCase());
    }
    if (options.since) {
      log = log.filter(e => e.lsn > options.since);
    }
    if (options.operation) {
      log = log.filter(e => e.operation === options.operation.toUpperCase());
    }
    return log.map(e => e.toJSON());
  }

  // --- Stats ---

  getStats() {
    return {
      ...this._stats,
      publications: this._publications.size,
      subscriptions: this._subscriptions.size,
      changeLogSize: this._changeLog.length,
      currentLSN: this._nextLSN - 1,
    };
  }

  getSlotStatus(subscriptionName) {
    const sub = this._subscriptions.get(subscriptionName);
    if (!sub) throw new Error(`Subscription '${subscriptionName}' does not exist`);
    return {
      name: sub.slot.name,
      confirmedLSN: sub.slot.confirmedLSN,
      sentLSN: sub.slot.sentLSN,
      lag: sub.slot.sentLSN - sub.slot.confirmedLSN,
      bufferSize: sub.buffer.length,
      statistics: { ...sub.slot.statistics },
    };
  }
}
