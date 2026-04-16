/**
 * Structural Typing: Types compatible if structure matches
 * Go/TypeScript-style: no explicit "implements" needed
 */

class StructType {
  constructor(fields) { this.fields = new Map(Object.entries(fields)); }
  toString() { return `{${[...this.fields].map(([k,v]) => `${k}:${v}`).join(', ')}}`; }
}

function structSubtype(sub, sup) {
  for (const [name, type] of sup.fields) {
    if (!sub.fields.has(name)) return false;
    if (sub.fields.get(name) !== type) return false;
  }
  return true;
}

function structEquiv(a, b) { return structSubtype(a, b) && structSubtype(b, a); }

function commonFields(a, b) {
  const result = {};
  for (const [name, type] of a.fields) {
    if (b.fields.has(name) && b.fields.get(name) === type) result[name] = type;
  }
  return new StructType(result);
}

function mergeTypes(a, b) {
  const result = {};
  for (const [k, v] of a.fields) result[k] = v;
  for (const [k, v] of b.fields) {
    if (result[k] && result[k] !== v) throw new Error(`Conflict: ${k}`);
    result[k] = v;
  }
  return new StructType(result);
}

function matchesInterface(value, iface) {
  for (const [name, type] of iface.fields) {
    if (!(name in value)) return false;
    if (typeof value[name] !== type) return false;
  }
  return true;
}

export { StructType, structSubtype, structEquiv, commonFields, mergeTypes, matchesInterface };
