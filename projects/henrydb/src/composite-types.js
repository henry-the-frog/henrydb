// composite-types.js — PostgreSQL-compatible composite/record types for HenryDB
// CREATE TYPE name AS (field1 type1, field2 type2, ...)
// Row constructors, field access, type coercion.

/**
 * CompositeType — a structured type with named fields.
 */
class CompositeType {
  constructor(name, fields) {
    this.name = name;
    this.fields = fields; // [{name, type, position}]
    this._fieldMap = new Map();
    for (let i = 0; i < fields.length; i++) {
      fields[i].position = i;
      this._fieldMap.set(fields[i].name.toLowerCase(), fields[i]);
    }
    this.createdAt = Date.now();
  }

  /**
   * Create a value of this type.
   */
  createValue(values) {
    if (Array.isArray(values)) {
      const obj = {};
      for (let i = 0; i < this.fields.length; i++) {
        obj[this.fields[i].name] = i < values.length ? values[i] : null;
      }
      return obj;
    }
    // Object — validate field names
    const obj = {};
    for (const field of this.fields) {
      obj[field.name] = values[field.name] ?? null;
    }
    return obj;
  }

  /**
   * Get a field's value from a composite value.
   */
  getField(value, fieldName) {
    const field = this._fieldMap.get(fieldName.toLowerCase());
    if (!field) throw new Error(`Field '${fieldName}' does not exist in type '${this.name}'`);
    return value[field.name];
  }

  /**
   * Set a field's value.
   */
  setField(value, fieldName, newValue) {
    const field = this._fieldMap.get(fieldName.toLowerCase());
    if (!field) throw new Error(`Field '${fieldName}' does not exist in type '${this.name}'`);
    value[field.name] = newValue;
    return value;
  }

  /**
   * Validate that a value matches this type's structure.
   */
  validate(value) {
    if (typeof value !== 'object' || value === null) {
      return { valid: false, error: 'Value must be an object' };
    }
    for (const field of this.fields) {
      if (!(field.name in value)) {
        return { valid: false, error: `Missing field '${field.name}'` };
      }
    }
    return { valid: true };
  }

  /**
   * Convert to string representation: ROW(val1, val2, ...).
   */
  toString(value) {
    const vals = this.fields.map(f => {
      const v = value[f.name];
      if (v === null) return '';
      if (typeof v === 'string') return `"${v}"`;
      return String(v);
    });
    return `(${vals.join(',')})`;
  }

  /**
   * Compare two composite values field by field.
   */
  compare(a, b) {
    for (const field of this.fields) {
      const va = a[field.name];
      const vb = b[field.name];
      if (va === null && vb === null) continue;
      if (va === null) return -1;
      if (vb === null) return 1;
      if (va < vb) return -1;
      if (va > vb) return 1;
    }
    return 0;
  }

  getInfo() {
    return {
      name: this.name,
      fields: this.fields.map(f => ({ name: f.name, type: f.type })),
    };
  }
}

/**
 * CompositeTypeManager — manages composite type definitions.
 */
export class CompositeTypeManager {
  constructor() {
    this._types = new Map();
  }

  /**
   * CREATE TYPE name AS (field1 type1, field2 type2, ...).
   */
  create(name, fields) {
    const lowerName = name.toLowerCase();
    if (this._types.has(lowerName)) {
      throw new Error(`Type '${name}' already exists`);
    }
    const type = new CompositeType(lowerName, fields);
    this._types.set(lowerName, type);
    return type.getInfo();
  }

  /**
   * ALTER TYPE: add or drop attributes.
   */
  alter(name, action) {
    const type = this._types.get(name.toLowerCase());
    if (!type) throw new Error(`Type '${name}' does not exist`);

    if (action.type === 'ADD_ATTRIBUTE') {
      if (type._fieldMap.has(action.name.toLowerCase())) {
        throw new Error(`Attribute '${action.name}' already exists`);
      }
      const field = { name: action.name, type: action.dataType, position: type.fields.length };
      type.fields.push(field);
      type._fieldMap.set(action.name.toLowerCase(), field);
    } else if (action.type === 'DROP_ATTRIBUTE') {
      const idx = type.fields.findIndex(f => f.name.toLowerCase() === action.name.toLowerCase());
      if (idx < 0) throw new Error(`Attribute '${action.name}' does not exist`);
      type.fields.splice(idx, 1);
      type._fieldMap.delete(action.name.toLowerCase());
    }

    return type.getInfo();
  }

  /**
   * DROP TYPE.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._types.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Type '${name}' does not exist`);
    }
    this._types.delete(lowerName);
    return true;
  }

  /**
   * Create a value of a composite type.
   */
  createValue(typeName, values) {
    const type = this._types.get(typeName.toLowerCase());
    if (!type) throw new Error(`Type '${typeName}' does not exist`);
    return type.createValue(values);
  }

  /**
   * Access a field of a composite value.
   */
  getField(typeName, value, fieldName) {
    const type = this._types.get(typeName.toLowerCase());
    if (!type) throw new Error(`Type '${typeName}' does not exist`);
    return type.getField(value, fieldName);
  }

  /**
   * Compare two composite values.
   */
  compare(typeName, a, b) {
    const type = this._types.get(typeName.toLowerCase());
    if (!type) throw new Error(`Type '${typeName}' does not exist`);
    return type.compare(a, b);
  }

  /**
   * Convert a composite value to ROW() string.
   */
  toString(typeName, value) {
    const type = this._types.get(typeName.toLowerCase());
    if (!type) throw new Error(`Type '${typeName}' does not exist`);
    return type.toString(value);
  }

  has(name) { return this._types.has(name.toLowerCase()); }
  list() { return [...this._types.values()].map(t => t.getInfo()); }
}
