// extensions.js — PostgreSQL-compatible extension system for HenryDB
// CREATE/DROP EXTENSION, version tracking, dependencies.

/**
 * Extension — a packaged set of database objects.
 */
class Extension {
  constructor(name, options = {}) {
    this.name = name;
    this.version = options.version || '1.0';
    this.schema = options.schema || 'public';
    this.description = options.description || '';
    this.dependencies = options.dependencies || [];
    this.objects = []; // [{type, name}]
    this.install = options.install || null; // Install function
    this.uninstall = options.uninstall || null;
    this.createdAt = Date.now();
    this.installed = false;
  }
}

/**
 * ExtensionManager — manages database extensions.
 */
export class ExtensionManager {
  constructor() {
    this._available = new Map(); // All known extensions
    this._installed = new Map(); // Currently installed extensions

    // Register some well-known extensions
    this.register('uuid-ossp', {
      version: '1.1',
      description: 'Generate universally unique identifiers (UUIDs)',
      install: () => [
        { type: 'function', name: 'uuid_generate_v4' },
        { type: 'function', name: 'uuid_generate_v1' },
      ],
    });
    this.register('pgcrypto', {
      version: '1.3',
      description: 'Cryptographic functions',
      install: () => [
        { type: 'function', name: 'gen_random_uuid' },
        { type: 'function', name: 'crypt' },
        { type: 'function', name: 'gen_salt' },
        { type: 'function', name: 'digest' },
      ],
    });
    this.register('pg_trgm', {
      version: '1.6',
      description: 'Text similarity using trigram matching',
      install: () => [
        { type: 'function', name: 'similarity' },
        { type: 'function', name: 'show_trgm' },
        { type: 'operator', name: '%' },
      ],
    });
    this.register('hstore', {
      version: '1.8',
      description: 'Key-value pair data type',
      install: () => [{ type: 'type', name: 'hstore' }],
    });
    this.register('postgis', {
      version: '3.4.0',
      description: 'PostGIS geometry and geography spatial types',
      dependencies: [],
      install: () => [
        { type: 'type', name: 'geometry' },
        { type: 'type', name: 'geography' },
        { type: 'function', name: 'ST_Distance' },
        { type: 'function', name: 'ST_Contains' },
      ],
    });
  }

  /**
   * Register an extension as available.
   */
  register(name, options = {}) {
    this._available.set(name.toLowerCase(), new Extension(name.toLowerCase(), options));
  }

  /**
   * CREATE EXTENSION name [VERSION 'ver'] [SCHEMA schema].
   */
  create(name, options = {}) {
    const lowerName = name.toLowerCase();

    if (this._installed.has(lowerName)) {
      if (options.ifNotExists) return this._getInstalled(lowerName);
      throw new Error(`Extension '${name}' already exists`);
    }

    const ext = this._available.get(lowerName);
    if (!ext) {
      throw new Error(`Extension '${name}' is not available`);
    }

    // Check dependencies
    for (const dep of ext.dependencies) {
      if (!this._installed.has(dep.toLowerCase())) {
        throw new Error(`Required extension '${dep}' is not installed`);
      }
    }

    // Install
    const installed = new Extension(lowerName, {
      version: options.version || ext.version,
      schema: options.schema || ext.schema,
      description: ext.description,
      dependencies: ext.dependencies,
    });

    if (ext.install) {
      installed.objects = ext.install();
    }
    installed.installed = true;

    this._installed.set(lowerName, installed);
    return this._getInstalled(lowerName);
  }

  /**
   * DROP EXTENSION name [CASCADE].
   */
  drop(name, options = {}) {
    const lowerName = name.toLowerCase();
    if (!this._installed.has(lowerName)) {
      if (options.ifExists) return false;
      throw new Error(`Extension '${name}' is not installed`);
    }

    // Check for dependents
    if (!options.cascade) {
      for (const [depName, ext] of this._installed) {
        if (ext.dependencies.includes(lowerName)) {
          throw new Error(`Cannot drop '${name}': extension '${depName}' depends on it (use CASCADE)`);
        }
      }
    } else {
      // CASCADE: also drop dependent extensions
      for (const [depName, ext] of this._installed) {
        if (ext.dependencies.includes(lowerName)) {
          this._installed.delete(depName);
        }
      }
    }

    this._installed.delete(lowerName);
    return true;
  }

  /**
   * ALTER EXTENSION name UPDATE [TO 'version'].
   */
  update(name, toVersion) {
    const ext = this._installed.get(name.toLowerCase());
    if (!ext) throw new Error(`Extension '${name}' is not installed`);

    const oldVersion = ext.version;
    ext.version = toVersion;
    return { name: ext.name, oldVersion, newVersion: toVersion };
  }

  /**
   * Check if an extension is installed.
   */
  isInstalled(name) {
    return this._installed.has(name.toLowerCase());
  }

  /**
   * Check if an extension is available.
   */
  isAvailable(name) {
    return this._available.has(name.toLowerCase());
  }

  /**
   * List installed extensions.
   */
  listInstalled() {
    return [...this._installed.values()].map(e => this._extInfo(e));
  }

  /**
   * List available extensions.
   */
  listAvailable() {
    return [...this._available.values()].map(e => ({
      name: e.name,
      version: e.version,
      description: e.description,
      installed: this._installed.has(e.name),
    }));
  }

  _getInstalled(name) {
    return this._extInfo(this._installed.get(name));
  }

  _extInfo(ext) {
    return {
      name: ext.name,
      version: ext.version,
      schema: ext.schema,
      description: ext.description,
      objects: ext.objects,
      dependencies: ext.dependencies,
    };
  }
}
