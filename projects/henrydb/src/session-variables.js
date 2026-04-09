// session-variables.js — PostgreSQL-compatible session GUC parameters for HenryDB
// SET/SHOW/RESET, search_path, client_encoding, timezone, etc.

/**
 * GUCDefaults — built-in Grand Unified Configuration defaults.
 */
const GUC_DEFAULTS = {
  search_path: '"$user", public',
  client_encoding: 'UTF8',
  timezone: 'UTC',
  datestyle: 'ISO, MDY',
  statement_timeout: '0',
  lock_timeout: '0',
  idle_in_transaction_session_timeout: '0',
  work_mem: '4MB',
  maintenance_work_mem: '64MB',
  temp_buffers: '8MB',
  effective_cache_size: '4GB',
  random_page_cost: '4.0',
  seq_page_cost: '1.0',
  enable_seqscan: 'on',
  enable_indexscan: 'on',
  enable_hashjoin: 'on',
  enable_mergejoin: 'on',
  enable_nestloop: 'on',
  default_transaction_isolation: 'read committed',
  default_transaction_read_only: 'off',
  log_statement: 'none',
  application_name: '',
  extra_float_digits: '1',
  client_min_messages: 'notice',
  row_security: 'on',
};

/**
 * SessionVariables — per-session GUC parameter management.
 */
export class SessionVariables {
  constructor(serverDefaults = {}) {
    this._defaults = { ...GUC_DEFAULTS, ...serverDefaults };
    this._session = new Map();      // Session-level settings (SET)
    this._local = new Map();        // Transaction-local settings (SET LOCAL)
    this._savepoints = [];          // Stack for savepoint state
  }

  /**
   * SET variable = value (session-scope).
   */
  set(name, value) {
    const lowerName = name.toLowerCase();
    this._session.set(lowerName, String(value));
  }

  /**
   * SET LOCAL variable = value (transaction-scope).
   */
  setLocal(name, value) {
    const lowerName = name.toLowerCase();
    this._local.set(lowerName, String(value));
  }

  /**
   * SHOW variable — returns current effective value.
   */
  show(name) {
    const lowerName = name.toLowerCase();
    // Local overrides session overrides default
    if (this._local.has(lowerName)) return this._local.get(lowerName);
    if (this._session.has(lowerName)) return this._session.get(lowerName);
    if (lowerName in this._defaults) return this._defaults[lowerName];
    throw new Error(`Unrecognized configuration parameter "${name}"`);
  }

  /**
   * SHOW ALL — returns all parameters with current values.
   */
  showAll() {
    const all = {};
    for (const [k, v] of Object.entries(this._defaults)) {
      all[k] = this._session.get(k) ?? v;
    }
    for (const [k, v] of this._session) {
      all[k] = v;
    }
    for (const [k, v] of this._local) {
      all[k] = v;
    }
    return all;
  }

  /**
   * RESET variable — restore to default.
   */
  reset(name) {
    if (name.toLowerCase() === 'all') {
      this._session.clear();
      this._local.clear();
      return;
    }
    const lowerName = name.toLowerCase();
    this._session.delete(lowerName);
    this._local.delete(lowerName);
  }

  /**
   * Called when transaction commits — local settings are discarded.
   */
  commitTransaction() {
    this._local.clear();
  }

  /**
   * Called when transaction rolls back — local settings are discarded.
   */
  rollbackTransaction() {
    this._local.clear();
  }

  /**
   * Save state for SAVEPOINT.
   */
  savepoint() {
    this._savepoints.push(new Map(this._local));
  }

  /**
   * Restore state for ROLLBACK TO SAVEPOINT.
   */
  rollbackToSavepoint() {
    if (this._savepoints.length > 0) {
      this._local = this._savepoints.pop();
    }
  }

  /**
   * Release savepoint (keep current state).
   */
  releaseSavepoint() {
    this._savepoints.pop();
  }

  /**
   * Get a typed boolean value.
   */
  getBoolean(name) {
    const val = this.show(name).toLowerCase();
    return val === 'on' || val === 'true' || val === '1' || val === 'yes';
  }

  /**
   * Get a typed integer value.
   */
  getInteger(name) {
    return parseInt(this.show(name), 10);
  }

  /**
   * Get a typed float value.
   */
  getFloat(name) {
    return parseFloat(this.show(name));
  }

  /**
   * Parse memory size strings like '4MB', '1GB'.
   */
  getMemoryBytes(name) {
    const val = this.show(name);
    const match = val.match(/^(\d+)\s*(KB|MB|GB|TB)?$/i);
    if (!match) return parseInt(val, 10);
    const num = parseInt(match[1], 10);
    const unit = (match[2] || '').toUpperCase();
    const multipliers = { '': 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3, TB: 1024 ** 4 };
    return num * (multipliers[unit] || 1);
  }
}
