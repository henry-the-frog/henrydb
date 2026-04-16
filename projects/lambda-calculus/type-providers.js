/**
 * Type Providers: Generate types from external data
 * 
 * TypeScript-style: given a JSON schema, derive the type.
 * Given a SQL table, derive the row type.
 * Given an API response, infer the type.
 */

class TStr { constructor() { this.tag = 'TStr'; } toString() { return 'string'; } }
class TNum { constructor() { this.tag = 'TNum'; } toString() { return 'number'; } }
class TBool { constructor() { this.tag = 'TBool'; } toString() { return 'boolean'; } }
class TNull { constructor() { this.tag = 'TNull'; } toString() { return 'null'; } }
class TArr { constructor(elem) { this.tag = 'TArr'; this.elem = elem; } toString() { return `${this.elem}[]`; } }
class TObj { constructor(fields) { this.tag = 'TObj'; this.fields = fields; } toString() { return `{${Object.entries(this.fields).map(([k,v]) => `${k}: ${v}`).join(', ')}}`; } }
class TUnion { constructor(types) { this.tag = 'TUnion'; this.types = types; } toString() { return this.types.join(' | '); } }
class TAny { constructor() { this.tag = 'TAny'; } toString() { return 'any'; } }

// Infer type from a JSON value
function inferType(value) {
  if (value === null) return new TNull();
  if (typeof value === 'string') return new TStr();
  if (typeof value === 'number') return new TNum();
  if (typeof value === 'boolean') return new TBool();
  if (Array.isArray(value)) {
    if (value.length === 0) return new TArr(new TAny());
    const elemTypes = value.map(inferType);
    const unified = unifyTypes(elemTypes);
    return new TArr(unified);
  }
  if (typeof value === 'object') {
    const fields = {};
    for (const [k, v] of Object.entries(value)) fields[k] = inferType(v);
    return new TObj(fields);
  }
  return new TAny();
}

function unifyTypes(types) {
  const unique = new Map();
  for (const t of types) unique.set(t.toString(), t);
  const values = [...unique.values()];
  if (values.length === 1) return values[0];
  return new TUnion(values);
}

// Generate type from JSON Schema
function fromJsonSchema(schema) {
  switch (schema.type) {
    case 'string': return new TStr();
    case 'number': case 'integer': return new TNum();
    case 'boolean': return new TBool();
    case 'null': return new TNull();
    case 'array': return new TArr(schema.items ? fromJsonSchema(schema.items) : new TAny());
    case 'object': {
      const fields = {};
      for (const [k, v] of Object.entries(schema.properties || {})) fields[k] = fromJsonSchema(v);
      return new TObj(fields);
    }
    default: return new TAny();
  }
}

// Generate TypeScript-like declaration
function toDeclaration(name, type) {
  if (type.tag === 'TObj') {
    const fields = Object.entries(type.fields).map(([k, v]) => `  ${k}: ${v};`).join('\n');
    return `interface ${name} {\n${fields}\n}`;
  }
  return `type ${name} = ${type};`;
}

export { TStr, TNum, TBool, TNull, TArr, TObj, TUnion, TAny, inferType, fromJsonSchema, toDeclaration };
