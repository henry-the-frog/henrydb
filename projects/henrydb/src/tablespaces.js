// tablespaces.js — PostgreSQL-compatible tablespace management for HenryDB
// CREATE TABLESPACE, SET TABLESPACE, storage location management.

/**
 * Tablespace — a named storage location.
 */
class Tablespace {
  constructor(name, location, options = {}) {
    this.name = name;
    this.location = location;
    this.owner = options.owner || 'postgres';
    this.options = options.options || {};
    this.objects = new Set(); // tables, indexes assigned here
    this.sizeBytes = 0;
    this.createdAt = Date.now();
  }

  addObject(objectName) {
    this.objects.add(objectName.toLowerCase());
  }

  removeObject(objectName) {
    this.objects.delete(objectName.toLowerCase());
  }

  hasObject(objectName) {
    return this.objects.has(objectName.toLowerCase());
  }
}

/**
 * TablespaceManager — manages tablespace definitions.
 */
export class TablespaceManager {
  constructor() {
    this._tablespaces = new Map();
    this._objectMap = new Map(); // object → tablespace name

    // Built-in tablespaces
    this._createBuiltin('pg_default', '/data/base');
    this._createBuiltin('pg_global', '/data/global');
  }

  _createBuiltin(name, location) {
    const ts = new Tablespace(name, location, { owner: 'postgres' });
    this._tablespaces.set(name, ts);
  }

  /**
   * CREATE TABLESPACE name LOCATION 'path' [OWNER user].
   */
  create(name, location, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._tablespaces.has(lowerName)) {
      throw new Error(`Tablespace '${name}' already exists`);
    }
    if (!location) {
      throw new Error('Tablespace location is required');
    }

    const ts = new Tablespace(lowerName, location, options);
    this._tablespaces.set(lowerName, ts);
    return { name: lowerName, location, owner: ts.owner };
  }

  /**
   * DROP TABLESPACE name.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (lowerName === 'pg_default' || lowerName === 'pg_global') {
      throw new Error(`Cannot drop built-in tablespace '${name}'`);
    }
    if (!this._tablespaces.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Tablespace '${name}' does not exist`);
    }
    const ts = this._tablespaces.get(lowerName);
    if (ts.objects.size > 0) {
      throw new Error(`Tablespace '${name}' is not empty`);
    }
    this._tablespaces.delete(lowerName);
    return true;
  }

  /**
   * ALTER TABLESPACE — change owner or options.
   */
  alter(name, options) {
    const ts = this._tablespaces.get(name.toLowerCase());
    if (!ts) throw new Error(`Tablespace '${name}' does not exist`);

    if (options.owner) ts.owner = options.owner;
    if (options.options) Object.assign(ts.options, options.options);
    return { name: ts.name, owner: ts.owner };
  }

  /**
   * Move an object (table/index) to a tablespace.
   */
  setTablespace(objectName, tablespaceName) {
    const lowerObj = objectName.toLowerCase();
    const lowerTs = tablespaceName.toLowerCase();

    if (!this._tablespaces.has(lowerTs)) {
      throw new Error(`Tablespace '${tablespaceName}' does not exist`);
    }

    // Remove from current tablespace
    const currentTs = this._objectMap.get(lowerObj);
    if (currentTs) {
      this._tablespaces.get(currentTs)?.removeObject(lowerObj);
    }

    // Add to new tablespace
    this._tablespaces.get(lowerTs).addObject(lowerObj);
    this._objectMap.set(lowerObj, lowerTs);
  }

  /**
   * Get tablespace for an object.
   */
  getTablespace(objectName) {
    return this._objectMap.get(objectName.toLowerCase()) || 'pg_default';
  }

  /**
   * Get tablespace info.
   */
  getInfo(name) {
    const ts = this._tablespaces.get(name.toLowerCase());
    if (!ts) throw new Error(`Tablespace '${name}' does not exist`);
    return {
      name: ts.name,
      location: ts.location,
      owner: ts.owner,
      objectCount: ts.objects.size,
      sizeBytes: ts.sizeBytes,
    };
  }

  /**
   * Update size tracking for a tablespace.
   */
  updateSize(name, sizeBytes) {
    const ts = this._tablespaces.get(name.toLowerCase());
    if (ts) ts.sizeBytes = sizeBytes;
  }

  has(name) { return this._tablespaces.has(name.toLowerCase()); }

  list() {
    return [...this._tablespaces.values()].map(ts => ({
      name: ts.name,
      location: ts.location,
      owner: ts.owner,
      objectCount: ts.objects.size,
    }));
  }
}
