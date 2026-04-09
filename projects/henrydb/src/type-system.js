// type-system.js — Database type system with coercion
export const Types = {
  INT: 'int',
  FLOAT: 'float',
  VARCHAR: 'varchar',
  BOOL: 'bool',
  DATE: 'date',
  NULL: 'null',
};

export function inferType(value) {
  if (value === null || value === undefined) return Types.NULL;
  if (typeof value === 'number') return Number.isInteger(value) ? Types.INT : Types.FLOAT;
  if (typeof value === 'boolean') return Types.BOOL;
  if (value instanceof Date) return Types.DATE;
  return Types.VARCHAR;
}

export function coerce(value, targetType) {
  if (value === null) return null;
  switch (targetType) {
    case Types.INT: return parseInt(value, 10);
    case Types.FLOAT: return parseFloat(value);
    case Types.VARCHAR: return String(value);
    case Types.BOOL: return Boolean(value);
    case Types.DATE: return new Date(value);
    default: return value;
  }
}

export function isCompatible(type1, type2) {
  if (type1 === type2) return true;
  if (type1 === Types.NULL || type2 === Types.NULL) return true;
  if ((type1 === Types.INT && type2 === Types.FLOAT) || (type1 === Types.FLOAT && type2 === Types.INT)) return true;
  return false;
}
