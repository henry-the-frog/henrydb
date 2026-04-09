// schema-management.js — PostgreSQL-compatible schema management for HenryDB
// CREATE/DROP/ALTER SCHEMA, search_path resolution, qualified names.

/**
 * Schema — a namespace for database objects.
 */
class Schema {
  constructor(name, options = {}) {
    this.name = name;
    this.owner = options.owner || 'postgres';
    this.objects = new Map(); // objectName → {type, name}
    this.createdAt = Date.now();
  }

  addObject(name, type) {
    this.objects.set(name.toLowerCase(), { name: name.toLowerCase(), type });
  }

  removeObject(name) {
    return this.objects.delete(name.toLowerCase());
  }

  hasObject(name) {
    return this.objects.has(name.toLowerCase());
  }

  getObject(name) {
    return this.objects.get(name.toLowerCase()) || null;
  }

  listObjects(type = null) {
    const objs = [...this.objects.values()];
    return type ? objs.filter(o => o.type === type) : objs;
  }
}

/**
 * SchemaManager — manages database schemas and name resolution.
 */
export class SchemaManager {
  constructor() {
    this._schemas = new Map();
    this._searchPath = ['public'];
    this._currentUser = 'postgres';

    // Built-in schemas
    this._createBuiltin('public');
    this._createBuiltin('pg_catalog');
    this._createBuiltin('information_schema');
    this._createBuiltin('pg_temp');
  }

  _createBuiltin(name) {
    this._schemas.set(name, new Schema(name, { owner: 'postgres' }));
  }

  /**
   * CREATE SCHEMA name [AUTHORIZATION owner].
   */
  create(name, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._schemas.has(lowerName)) {
      if (options.ifNotExists) return this.getInfo(lowerName);
      throw new Error(`Schema '${name}' already exists`);
    }
    const schema = new Schema(lowerName, options);
    this._schemas.set(lowerName, schema);
    return { name: lowerName, owner: schema.owner };
  }

  /**
   * DROP SCHEMA name [CASCADE|RESTRICT].
   */
  drop(name, options = {}) {
    const lowerName = name.toLowerCase();
    if (['public', 'pg_catalog', 'information_schema'].includes(lowerName)) {
      throw new Error(`Cannot drop built-in schema '${name}'`);
    }
    if (!this._schemas.has(lowerName)) {
      if (options.ifExists) return false;
      throw new Error(`Schema '${name}' does not exist`);
    }

    const schema = this._schemas.get(lowerName);
    if (schema.objects.size > 0 && !options.cascade) {
      throw new Error(`Cannot drop schema '${name}': contains objects (use CASCADE)`);
    }

    this._schemas.delete(lowerName);
    return true;
  }

  /**
   * ALTER SCHEMA name RENAME TO newName | OWNER TO owner.
   */
  alter(name, options) {
    const schema = this._schemas.get(name.toLowerCase());
    if (!schema) throw new Error(`Schema '${name}' does not exist`);

    if (options.renameTo) {
      const newName = options.renameTo.toLowerCase();
      if (this._schemas.has(newName)) throw new Error(`Schema '${options.renameTo}' already exists`);
      this._schemas.delete(schema.name);
      schema.name = newName;
      this._schemas.set(newName, schema);
    }
    if (options.owner) {
      schema.owner = options.owner;
    }

    return { name: schema.name, owner: schema.owner };
  }

  /**
   * SET search_path TO schema1, schema2, ...
   */
  setSearchPath(schemas) {
    this._searchPath = schemas.map(s =>
      s === '"$user"' ? this._currentUser : s.toLowerCase()
    );
  }

  /**
   * SHOW search_path.
   */
  getSearchPath() {
    return [...this._searchPath];
  }

  /**
   * Resolve an unqualified name using search_path.
   * Returns {schema, name} or null.
   */
  resolve(objectName, objectType = null) {
    // If qualified (schema.name), resolve directly
    if (objectName.includes('.')) {
      const [schemaName, name] = objectName.split('.');
      const schema = this._schemas.get(schemaName.toLowerCase());
      if (!schema) return null;
      const obj = schema.getObject(name);
      if (obj && (!objectType || obj.type === objectType)) {
        return { schema: schemaName.toLowerCase(), name: name.toLowerCase() };
      }
      return null;
    }

    // Search through search_path
    // pg_catalog is always searched first implicitly
    const searchOrder = ['pg_catalog', ...this._searchPath];
    for (const schemaName of searchOrder) {
      const schema = this._schemas.get(schemaName);
      if (!schema) continue;
      const obj = schema.getObject(objectName);
      if (obj && (!objectType || obj.type === objectType)) {
        return { schema: schemaName, name: objectName.toLowerCase() };
      }
    }

    return null;
  }

  /**
   * Get the default creation schema (first writable schema in search_path).
   */
  getDefaultSchema() {
    for (const name of this._searchPath) {
      if (this._schemas.has(name)) return name;
    }
    return 'public';
  }

  /**
   * Register an object in a schema.
   */
  registerObject(schemaName, objectName, objectType) {
    const schema = this._schemas.get(schemaName.toLowerCase());
    if (!schema) throw new Error(`Schema '${schemaName}' does not exist`);
    schema.addObject(objectName, objectType);
  }

  /**
   * Remove an object from a schema.
   */
  unregisterObject(schemaName, objectName) {
    const schema = this._schemas.get(schemaName.toLowerCase());
    if (schema) schema.removeObject(objectName);
  }

  /**
   * Set the current user (for $user in search_path).
   */
  setCurrentUser(user) {
    this._currentUser = user.toLowerCase();
  }

  getInfo(name) {
    const schema = this._schemas.get(name.toLowerCase());
    if (!schema) throw new Error(`Schema '${name}' does not exist`);
    return {
      name: schema.name,
      owner: schema.owner,
      objectCount: schema.objects.size,
    };
  }

  has(name) { return this._schemas.has(name.toLowerCase()); }

  list() {
    return [...this._schemas.values()].map(s => ({
      name: s.name,
      owner: s.owner,
      objectCount: s.objects.size,
    }));
  }
}
