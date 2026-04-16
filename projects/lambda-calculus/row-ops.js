/**
 * Row Elimination: Remove fields from row types
 * Complement to row-poly.js (which adds fields)
 */

class TRow { constructor(fields, rest = null) { this.fields = new Map(Object.entries(fields)); this.rest = rest; } }

function rowExtend(row, field, type) {
  const newFields = new Map(row.fields);
  newFields.set(field, type);
  return new TRow(Object.fromEntries(newFields), row.rest);
}

function rowRemove(row, field) {
  if (!row.fields.has(field)) throw new Error(`Field ${field} not in row`);
  const newFields = new Map(row.fields);
  newFields.delete(field);
  return new TRow(Object.fromEntries(newFields), row.rest);
}

function rowSelect(row, fields) {
  const result = {};
  for (const f of fields) {
    if (!row.fields.has(f)) throw new Error(`Missing: ${f}`);
    result[f] = row.fields.get(f);
  }
  return new TRow(result, null);
}

function rowRename(row, from, to) {
  if (!row.fields.has(from)) throw new Error(`Missing: ${from}`);
  const type = row.fields.get(from);
  return rowExtend(rowRemove(row, from), to, type);
}

function rowMerge(r1, r2) {
  const result = new Map(r1.fields);
  for (const [k, v] of r2.fields) {
    if (result.has(k)) throw new Error(`Duplicate: ${k}`);
    result.set(k, v);
  }
  return new TRow(Object.fromEntries(result));
}

function rowDiff(r1, r2) {
  const result = {};
  for (const [k, v] of r1.fields) if (!r2.fields.has(k)) result[k] = v;
  return new TRow(result);
}

function rowIntersect(r1, r2) {
  const result = {};
  for (const [k, v] of r1.fields) if (r2.fields.has(k)) result[k] = v;
  return new TRow(result);
}

export { TRow, rowExtend, rowRemove, rowSelect, rowRename, rowMerge, rowDiff, rowIntersect };
