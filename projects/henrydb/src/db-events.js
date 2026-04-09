// db-events.js — Event system for HenryDB
// Subscribe to table changes (INSERT, UPDATE, DELETE) with pattern matching.

/**
 * DatabaseEventEmitter — pub/sub for database changes.
 * 
 * Usage:
 *   const events = new DatabaseEventEmitter();
 *   events.on('users', 'INSERT', (data) => console.log('New user:', data));
 *   events.on('*', '*', (data) => console.log('Any change:', data));
 *   events.emit('users', 'INSERT', { id: 1, name: 'Alice' });
 */
export class DatabaseEventEmitter {
  constructor() {
    this._listeners = new Map();
    this._stats = { emitted: 0, delivered: 0 };
  }

  /**
   * Subscribe to table events.
   * @param {string} table - Table name or '*' for all
   * @param {string} event - 'INSERT', 'UPDATE', 'DELETE', or '*' for all
   * @param {Function} callback - (data) => void
   * @returns {Function} Unsubscribe function
   */
  on(table, event, callback) {
    const key = `${table}:${event}`;
    if (!this._listeners.has(key)) this._listeners.set(key, new Set());
    this._listeners.get(key).add(callback);
    
    return () => {
      const set = this._listeners.get(key);
      if (set) {
        set.delete(callback);
        if (set.size === 0) this._listeners.delete(key);
      }
    };
  }

  /**
   * Subscribe to a single event (auto-unsubscribes after first fire).
   */
  once(table, event, callback) {
    const unsub = this.on(table, event, (data) => {
      unsub();
      callback(data);
    });
    return unsub;
  }

  /**
   * Emit an event.
   * @param {string} table - Table name
   * @param {string} event - Event type (INSERT, UPDATE, DELETE)
   * @param {Object} data - Event data
   */
  emit(table, event, data) {
    this._stats.emitted++;
    const eventData = {
      table,
      event,
      data,
      timestamp: Date.now(),
    };

    // Exact match
    this._deliver(`${table}:${event}`, eventData);
    // Table wildcard
    this._deliver(`${table}:*`, eventData);
    // Event wildcard
    this._deliver(`*:${event}`, eventData);
    // Full wildcard
    this._deliver('*:*', eventData);
  }

  _deliver(key, data) {
    const listeners = this._listeners.get(key);
    if (!listeners) return;
    for (const cb of listeners) {
      try { cb(data); this._stats.delivered++; } catch(e) {}
    }
  }

  /**
   * Remove all listeners for a table (or all listeners).
   */
  off(table) {
    if (!table) {
      this._listeners.clear();
      return;
    }
    for (const key of [...this._listeners.keys()]) {
      if (key.startsWith(`${table}:`)) this._listeners.delete(key);
    }
  }

  /**
   * Get listener count.
   */
  get listenerCount() {
    let count = 0;
    for (const set of this._listeners.values()) count += set.size;
    return count;
  }

  stats() {
    return { ...this._stats, listeners: this.listenerCount };
  }
}
