/**
 * Row Polymorphism
 * 
 * Extensible records with polymorphic row types.
 * Enables structural typing similar to TypeScript's interfaces.
 * 
 * Key concepts:
 * - Row: ordered set of (label: type) pairs + optional row variable
 * - Record type: {label₁: T₁, label₂: T₂, ... | ρ}
 * - Row variable (ρ): makes the record extensible
 * - Field access: r.label extracts the field type
 * - Record extension: {label: T | r} adds a field
 * - Record restriction: r \ label removes a field
 * 
 * Based on:
 * - Remy's row polymorphism (1989)
 * - Wand's row types (1989)
 * - Leijen's extensible records (2005)
 */

// ============================================================
// Row Types
// ============================================================

class RowEmpty {
  constructor() { this.tag = 'RowEmpty'; }
  toString() { return '∅'; }
}

class RowExtend {
  constructor(label, type, rest) {
    this.tag = 'RowExtend';
    this.label = label;
    this.type = type;
    this.rest = rest; // Another row (RowEmpty, RowExtend, or RowVar)
  }
  toString() {
    const rest = this.rest.tag === 'RowEmpty' ? '' : ` | ${this.rest}`;
    return `${this.label}: ${this.type}${rest}`;
  }
}

class RowVar {
  constructor(name) { this.tag = 'RowVar'; this.name = name; }
  toString() { return this.name; }
}

class RecordType {
  constructor(row) { this.tag = 'RecordType'; this.row = row; }
  toString() { return `{${this.row}}`; }
}

class VariantType {
  constructor(row) { this.tag = 'VariantType'; this.row = row; }
  toString() { return `⟨${this.row}⟩`; }
}

// ============================================================
// Record Values
// ============================================================

class Record {
  constructor(fields) { this.tag = 'Record'; this.fields = fields; } // Map<label, value>
  toString() {
    const fs = [...this.fields].map(([k, v]) => `${k}: ${v}`).join(', ');
    return `{${fs}}`;
  }
  get(label) { return this.fields.get(label); }
  has(label) { return this.fields.has(label); }
  extend(label, value) {
    const newFields = new Map(this.fields);
    newFields.set(label, value);
    return new Record(newFields);
  }
  restrict(label) {
    const newFields = new Map(this.fields);
    newFields.delete(label);
    return new Record(newFields);
  }
}

// ============================================================
// Row Operations
// ============================================================

function rowFields(row) {
  const fields = [];
  let current = row;
  while (current.tag === 'RowExtend') {
    fields.push({ label: current.label, type: current.type });
    current = current.rest;
  }
  return { fields, tail: current };
}

function rowHas(row, label) {
  const { fields } = rowFields(row);
  return fields.some(f => f.label === label);
}

function rowGet(row, label) {
  const { fields } = rowFields(row);
  const field = fields.find(f => f.label === label);
  return field ? field.type : null;
}

function rowWithout(row, label) {
  if (row.tag === 'RowEmpty') return row;
  if (row.tag === 'RowVar') return row;
  if (row.tag === 'RowExtend') {
    if (row.label === label) return row.rest;
    return new RowExtend(row.label, row.type, rowWithout(row.rest, label));
  }
  return row;
}

function makeRow(fields, tail = new RowEmpty()) {
  let row = tail;
  for (let i = fields.length - 1; i >= 0; i--) {
    row = new RowExtend(fields[i].label, fields[i].type, row);
  }
  return row;
}

// ============================================================
// Row Unification
// ============================================================

class RowSubst {
  constructor() { this.map = new Map(); }
  
  apply(row) {
    if (row.tag === 'RowVar') {
      const t = this.map.get(row.name);
      if (t) return this.apply(t);
      return row;
    }
    if (row.tag === 'RowExtend') {
      return new RowExtend(row.label, row.type, this.apply(row.rest));
    }
    return row;
  }
  
  set(name, row) { this.map.set(name, row); }
}

function unifyRows(r1, r2, subst = new RowSubst()) {
  r1 = subst.apply(r1);
  r2 = subst.apply(r2);
  
  // Both empty: success
  if (r1.tag === 'RowEmpty' && r2.tag === 'RowEmpty') return subst;
  
  // One is a variable: bind it
  if (r1.tag === 'RowVar') {
    subst.set(r1.name, r2);
    return subst;
  }
  if (r2.tag === 'RowVar') {
    subst.set(r2.name, r1);
    return subst;
  }
  
  // Both extend: match first label
  if (r1.tag === 'RowExtend' && r2.tag === 'RowExtend') {
    // Find r1.label in r2
    if (r2.label === r1.label) {
      // Same label: unify types and continue with rest
      return unifyRows(r1.rest, r2.rest, subst);
    }
    // Different labels: rewrite r2 to expose r1.label
    const r2WithoutLabel = rowWithout(r2, r1.label);
    if (rowHas(r2, r1.label)) {
      return unifyRows(r1.rest, new RowExtend(r2.label, r2.type, r2WithoutLabel), subst);
    }
    // Label not in r2: fail unless r2 has a tail variable
    const { tail } = rowFields(r2);
    if (tail.tag === 'RowVar') {
      const fresh = new RowVar(`ρ${Date.now()}`);
      subst.set(tail.name, new RowExtend(r1.label, r1.type, fresh));
      return unifyRows(r1.rest, subst.apply(r2.rest), subst);
    }
    throw new Error(`Row mismatch: ${r1.label} not in ${r2}`);
  }
  
  throw new Error(`Cannot unify rows: ${r1} and ${r2}`);
}

// ============================================================
// Row Subtyping (width + depth)
// ============================================================

function rowSubtype(r1, r2) {
  // r1 <: r2 if r1 has all fields of r2 (width subtyping)
  const { fields: f2, tail: tail2 } = rowFields(r2);
  
  for (const { label, type: t2 } of f2) {
    const t1 = rowGet(r1, label);
    if (!t1) return { isSubtype: false, reason: `missing field: ${label}` };
    // Depth subtyping would check t1 <: t2 here
  }
  
  // If r2 has a row variable, r1 can have extra fields
  if (tail2.tag === 'RowVar') return { isSubtype: true, reason: 'open row' };
  
  // If r2 is closed, r1 must not have extra fields
  const { fields: f1 } = rowFields(r1);
  if (f1.length > f2.length) {
    return { isSubtype: false, reason: 'extra fields in closed record' };
  }
  
  return { isSubtype: true, reason: 'all fields match' };
}

// ============================================================
// Type Checking for Records
// ============================================================

function checkRecord(record, recordType) {
  if (recordType.tag !== 'RecordType') return false;
  const { fields } = rowFields(recordType.row);
  
  for (const { label } of fields) {
    if (!record.has(label)) return false;
  }
  return true;
}

// ============================================================
// Exports
// ============================================================

export {
  RowEmpty, RowExtend, RowVar, RecordType, VariantType, Record,
  rowFields, rowHas, rowGet, rowWithout, makeRow,
  RowSubst, unifyRows, rowSubtype, checkRecord
};
