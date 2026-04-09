// domains.js — PostgreSQL-compatible domain types for HenryDB
// CREATE DOMAIN, ALTER DOMAIN, DROP DOMAIN, CHECK constraints.

/**
 * Domain — a named type with constraints.
 */
class Domain {
  constructor(name, baseType, options = {}) {
    this.name = name;
    this.baseType = baseType;
    this.notNull = options.notNull || false;
    this.defaultValue = options.default ?? null;
    this.constraints = []; // [{name, check: (value) => boolean}]
    this.createdAt = Date.now();
  }

  /**
   * Validate a value against this domain's constraints.
   */
  validate(value) {
    if (value === null || value === undefined) {
      if (this.notNull) {
        return { valid: false, error: `Domain '${this.name}' does not allow NULL values` };
      }
      return { valid: true };
    }

    for (const constraint of this.constraints) {
      if (!constraint.check(value)) {
        return {
          valid: false,
          error: `Value violates check constraint '${constraint.name}' on domain '${this.name}'`,
        };
      }
    }

    return { valid: true };
  }

  /**
   * Apply default value if input is null/undefined.
   */
  applyDefault(value) {
    if ((value === null || value === undefined) && this.defaultValue !== null) {
      return typeof this.defaultValue === 'function' ? this.defaultValue() : this.defaultValue;
    }
    return value;
  }

  addConstraint(name, check) {
    this.constraints.push({ name, check });
  }

  removeConstraint(name) {
    const idx = this.constraints.findIndex(c => c.name === name);
    if (idx >= 0) this.constraints.splice(idx, 1);
    return idx >= 0;
  }
}

/**
 * DomainManager — manages domain type definitions.
 */
export class DomainManager {
  constructor() {
    this._domains = new Map();
  }

  /**
   * CREATE DOMAIN name AS baseType [DEFAULT val] [NOT NULL] [CHECK (expr)].
   */
  create(name, baseType, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._domains.has(lowerName)) {
      throw new Error(`Domain '${name}' already exists`);
    }

    const domain = new Domain(lowerName, baseType, options);

    if (options.check) {
      domain.addConstraint(
        options.constraintName || `${lowerName}_check`,
        options.check
      );
    }

    this._domains.set(lowerName, domain);
    return this._info(domain);
  }

  /**
   * ALTER DOMAIN: add/drop constraint, set/drop NOT NULL, set DEFAULT.
   */
  alter(name, action) {
    const domain = this._domains.get(name.toLowerCase());
    if (!domain) throw new Error(`Domain '${name}' does not exist`);

    switch (action.type) {
      case 'ADD_CONSTRAINT':
        domain.addConstraint(action.name, action.check);
        break;
      case 'DROP_CONSTRAINT':
        domain.removeConstraint(action.name);
        break;
      case 'SET_NOT_NULL':
        domain.notNull = true;
        break;
      case 'DROP_NOT_NULL':
        domain.notNull = false;
        break;
      case 'SET_DEFAULT':
        domain.defaultValue = action.value;
        break;
      case 'DROP_DEFAULT':
        domain.defaultValue = null;
        break;
    }

    return this._info(domain);
  }

  /**
   * DROP DOMAIN.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._domains.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Domain '${name}' does not exist`);
    }
    this._domains.delete(lowerName);
    return true;
  }

  /**
   * Validate a value against a domain.
   */
  validate(name, value) {
    const domain = this._domains.get(name.toLowerCase());
    if (!domain) throw new Error(`Domain '${name}' does not exist`);
    return domain.validate(value);
  }

  /**
   * Get the default value for a domain.
   */
  getDefault(name, value) {
    const domain = this._domains.get(name.toLowerCase());
    if (!domain) return value;
    return domain.applyDefault(value);
  }

  has(name) {
    return this._domains.has(name.toLowerCase());
  }

  list() {
    return [...this._domains.values()].map(d => this._info(d));
  }

  _info(domain) {
    return {
      name: domain.name,
      baseType: domain.baseType,
      notNull: domain.notNull,
      default: domain.defaultValue,
      constraints: domain.constraints.map(c => c.name),
    };
  }
}
