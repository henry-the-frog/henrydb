// foreign-data-wrapper.js — PostgreSQL-compatible FDW for HenryDB
// CREATE SERVER, CREATE FOREIGN TABLE, user mappings, predicate pushdown.

/**
 * ForeignServer — a remote data source.
 */
class ForeignServer {
  constructor(name, fdwName, options = {}) {
    this.name = name;
    this.fdwName = fdwName;
    this.host = options.host || 'localhost';
    this.port = options.port || 5432;
    this.dbname = options.dbname || '';
    this.options = options;
    this.createdAt = Date.now();
  }
}

/**
 * UserMapping — credentials for a foreign server.
 */
class UserMapping {
  constructor(user, serverName, options = {}) {
    this.user = user;
    this.serverName = serverName;
    this.username = options.username || user;
    this.password = options.password || '';
  }
}

/**
 * ForeignTable — a table mapped to a remote source.
 */
class ForeignTable {
  constructor(name, serverName, columns, options = {}) {
    this.name = name;
    this.serverName = serverName;
    this.columns = columns; // [{name, type}]
    this.remoteTable = options.remoteTable || name;
    this.remoteSchema = options.remoteSchema || 'public';
    this.options = options;
    this.createdAt = Date.now();
  }
}

/**
 * FDWManager — manages foreign data wrappers, servers, and tables.
 */
export class FDWManager {
  constructor() {
    this._wrappers = new Map(); // fdwName → {name, handler, validator}
    this._servers = new Map();
    this._tables = new Map();
    this._userMappings = new Map(); // "user@server" → UserMapping
    this._dataProviders = new Map(); // serverName → function that returns data

    // Register built-in wrappers
    this.createWrapper('file_fdw', {
      handler: 'file_fdw_handler',
      validator: 'file_fdw_validator',
    });
    this.createWrapper('postgres_fdw', {
      handler: 'postgres_fdw_handler',
      validator: 'postgres_fdw_validator',
    });
  }

  /**
   * CREATE FOREIGN DATA WRAPPER name.
   */
  createWrapper(name, options = {}) {
    const lowerName = name.toLowerCase();
    this._wrappers.set(lowerName, {
      name: lowerName,
      handler: options.handler || null,
      validator: options.validator || null,
    });
    return { name: lowerName };
  }

  /**
   * CREATE SERVER name FOREIGN DATA WRAPPER fdwName OPTIONS (...).
   */
  createServer(name, fdwName, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._servers.has(lowerName)) {
      throw new Error(`Server '${name}' already exists`);
    }
    if (!this._wrappers.has(fdwName.toLowerCase())) {
      throw new Error(`Foreign data wrapper '${fdwName}' does not exist`);
    }
    const server = new ForeignServer(lowerName, fdwName.toLowerCase(), options);
    this._servers.set(lowerName, server);
    return { name: lowerName, fdw: fdwName };
  }

  /**
   * CREATE USER MAPPING FOR user SERVER server OPTIONS (...).
   */
  createUserMapping(user, serverName, options = {}) {
    const key = `${user.toLowerCase()}@${serverName.toLowerCase()}`;
    const mapping = new UserMapping(user, serverName.toLowerCase(), options);
    this._userMappings.set(key, mapping);
    return { user, server: serverName };
  }

  /**
   * CREATE FOREIGN TABLE name (columns...) SERVER server OPTIONS (...).
   */
  createForeignTable(name, serverName, columns, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._tables.has(lowerName)) {
      throw new Error(`Table '${name}' already exists`);
    }
    if (!this._servers.has(serverName.toLowerCase())) {
      throw new Error(`Server '${serverName}' does not exist`);
    }
    const table = new ForeignTable(lowerName, serverName.toLowerCase(), columns, options);
    this._tables.set(lowerName, table);
    return { name: lowerName, server: serverName, columns: columns.map(c => c.name) };
  }

  /**
   * Register a data provider for testing (simulates remote data).
   */
  registerDataProvider(serverName, provider) {
    this._dataProviders.set(serverName.toLowerCase(), provider);
  }

  /**
   * Query a foreign table with optional predicate pushdown.
   */
  query(tableName, options = {}) {
    const table = this._tables.get(tableName.toLowerCase());
    if (!table) throw new Error(`Foreign table '${tableName}' does not exist`);

    const provider = this._dataProviders.get(table.serverName);
    if (!provider) {
      throw new Error(`No data provider for server '${table.serverName}'`);
    }

    // Build remote query context
    const ctx = {
      table: table.remoteTable,
      schema: table.remoteSchema,
      columns: options.columns || table.columns.map(c => c.name),
      where: options.where || null,
      limit: options.limit || null,
      offset: options.offset || null,
      orderBy: options.orderBy || null,
    };

    return provider(ctx);
  }

  /**
   * IMPORT FOREIGN SCHEMA — discover remote tables.
   */
  importSchema(serverName, remoteSchema = 'public') {
    const provider = this._dataProviders.get(serverName.toLowerCase());
    if (!provider) throw new Error(`No data provider for server '${serverName}'`);

    // Provider should handle schema import
    return provider({
      type: 'import_schema',
      schema: remoteSchema,
    });
  }

  /**
   * DROP FOREIGN TABLE.
   */
  dropForeignTable(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._tables.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Foreign table '${name}' does not exist`);
    }
    this._tables.delete(lowerName);
    return true;
  }

  /**
   * DROP SERVER.
   */
  dropServer(name, ifExists = false, cascade = false) {
    const lowerName = name.toLowerCase();
    if (!this._servers.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Server '${name}' does not exist`);
    }
    if (cascade) {
      // Drop all foreign tables using this server
      for (const [tName, table] of this._tables) {
        if (table.serverName === lowerName) this._tables.delete(tName);
      }
    }
    this._servers.delete(lowerName);
    return true;
  }

  hasServer(name) { return this._servers.has(name.toLowerCase()); }
  hasForeignTable(name) { return this._tables.has(name.toLowerCase()); }

  listServers() {
    return [...this._servers.values()].map(s => ({
      name: s.name, fdw: s.fdwName, host: s.host, port: s.port,
    }));
  }

  listForeignTables() {
    return [...this._tables.values()].map(t => ({
      name: t.name, server: t.serverName, remoteTable: t.remoteTable,
      columns: t.columns.map(c => c.name),
    }));
  }
}
