// enum-types.js — PostgreSQL-compatible enum types for HenryDB
// CREATE TYPE name AS ENUM ('val1', 'val2', ...)
// ALTER TYPE name ADD VALUE 'new_val'

/**
 * EnumType — a user-defined enumeration type.
 */
class EnumType {
  constructor(name, values) {
    this.name = name;
    this.values = [...values]; // Ordered list
    this._valueSet = new Set(values);
    this._ordering = new Map(); // value → ordinal
    this._rebuildOrdering();
    this.createdAt = Date.now();
  }

  /**
   * Check if a value is valid for this enum.
   */
  isValid(value) {
    return this._valueSet.has(value);
  }

  /**
   * Compare two enum values. Returns -1, 0, or 1.
   */
  compare(a, b) {
    const ordA = this._ordering.get(a);
    const ordB = this._ordering.get(b);
    if (ordA === undefined) throw new Error(`'${a}' is not a valid value for enum '${this.name}'`);
    if (ordB === undefined) throw new Error(`'${b}' is not a valid value for enum '${this.name}'`);
    return ordA < ordB ? -1 : ordA > ordB ? 1 : 0;
  }

  /**
   * Add a new value to the enum.
   */
  addValue(value, options = {}) {
    if (this._valueSet.has(value)) {
      if (options.ifNotExists) return;
      throw new Error(`Enum value '${value}' already exists in type '${this.name}'`);
    }

    if (options.before) {
      const idx = this.values.indexOf(options.before);
      if (idx < 0) throw new Error(`Enum value '${options.before}' does not exist`);
      this.values.splice(idx, 0, value);
    } else if (options.after) {
      const idx = this.values.indexOf(options.after);
      if (idx < 0) throw new Error(`Enum value '${options.after}' does not exist`);
      this.values.splice(idx + 1, 0, value);
    } else {
      this.values.push(value);
    }

    this._valueSet.add(value);
    this._rebuildOrdering();
  }

  /**
   * Rename a value.
   */
  renameValue(oldValue, newValue) {
    const idx = this.values.indexOf(oldValue);
    if (idx < 0) throw new Error(`Enum value '${oldValue}' does not exist`);
    if (this._valueSet.has(newValue)) throw new Error(`Enum value '${newValue}' already exists`);

    this.values[idx] = newValue;
    this._valueSet.delete(oldValue);
    this._valueSet.add(newValue);
    this._rebuildOrdering();
  }

  _rebuildOrdering() {
    this._ordering.clear();
    for (let i = 0; i < this.values.length; i++) {
      this._ordering.set(this.values[i], i);
    }
  }
}

/**
 * EnumManager — manages enum type definitions.
 */
export class EnumManager {
  constructor() {
    this._enums = new Map();
  }

  /**
   * CREATE TYPE name AS ENUM ('val1', 'val2', ...).
   */
  create(name, values) {
    const lowerName = name.toLowerCase();
    if (this._enums.has(lowerName)) {
      throw new Error(`Type '${name}' already exists`);
    }
    const enumType = new EnumType(lowerName, values);
    this._enums.set(lowerName, enumType);
    return { name: lowerName, values: [...values] };
  }

  /**
   * ALTER TYPE name ADD VALUE 'val' [BEFORE|AFTER 'existing'].
   */
  addValue(name, value, options = {}) {
    const enumType = this._enums.get(name.toLowerCase());
    if (!enumType) throw new Error(`Type '${name}' does not exist`);
    enumType.addValue(value, options);
  }

  /**
   * ALTER TYPE name RENAME VALUE 'old' TO 'new'.
   */
  renameValue(name, oldValue, newValue) {
    const enumType = this._enums.get(name.toLowerCase());
    if (!enumType) throw new Error(`Type '${name}' does not exist`);
    enumType.renameValue(oldValue, newValue);
  }

  /**
   * DROP TYPE name.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._enums.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Type '${name}' does not exist`);
    }
    this._enums.delete(lowerName);
    return true;
  }

  /**
   * Validate a value for an enum type.
   */
  validate(name, value) {
    const enumType = this._enums.get(name.toLowerCase());
    if (!enumType) throw new Error(`Type '${name}' does not exist`);
    return enumType.isValid(value);
  }

  /**
   * Compare two values of an enum type.
   */
  compare(name, a, b) {
    const enumType = this._enums.get(name.toLowerCase());
    if (!enumType) throw new Error(`Type '${name}' does not exist`);
    return enumType.compare(a, b);
  }

  /**
   * Get all values for an enum type.
   */
  getValues(name) {
    const enumType = this._enums.get(name.toLowerCase());
    if (!enumType) throw new Error(`Type '${name}' does not exist`);
    return [...enumType.values];
  }

  has(name) {
    return this._enums.has(name.toLowerCase());
  }

  list() {
    return [...this._enums.values()].map(e => ({
      name: e.name,
      values: [...e.values],
    }));
  }
}
