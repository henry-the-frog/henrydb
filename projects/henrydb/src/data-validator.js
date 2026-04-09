// data-validator.js — Data validation utilities for HenryDB
// Validate data quality: type checks, uniqueness, null constraints, range checks.

/**
 * Validate data in a table against rules.
 * @param {Database} db
 * @param {string} table
 * @param {Object} rules - { column: [rule, ...] }
 * @returns {Object} { valid: boolean, errors: [...] }
 */
export function validateTable(db, table, rules) {
  const errors = [];
  const rows = db.execute(`SELECT * FROM ${table}`).rows;
  
  for (const [column, columnRules] of Object.entries(rules)) {
    for (const rule of columnRules) {
      const ruleErrors = applyRule(rows, column, rule, table);
      errors.push(...ruleErrors);
    }
  }
  
  return {
    valid: errors.length === 0,
    errors,
    rowCount: rows.length,
    rulesChecked: Object.values(rules).reduce((s, r) => s + r.length, 0),
  };
}

function applyRule(rows, column, rule, table) {
  const errors = [];
  
  switch (rule.type) {
    case 'notNull':
      rows.forEach((row, i) => {
        if (row[column] === null || row[column] === undefined) {
          errors.push({ row: i, column, rule: 'notNull', message: `${column} is NULL at row ${i}` });
        }
      });
      break;
      
    case 'unique': {
      const seen = new Map();
      rows.forEach((row, i) => {
        const val = row[column];
        if (seen.has(val)) {
          errors.push({ row: i, column, rule: 'unique', 
            message: `Duplicate ${column}=${val} at rows ${seen.get(val)} and ${i}` });
        } else {
          seen.set(val, i);
        }
      });
      break;
    }
    
    case 'type': {
      const expected = rule.expected;
      rows.forEach((row, i) => {
        const val = row[column];
        if (val !== null && typeof val !== expected) {
          errors.push({ row: i, column, rule: 'type',
            message: `${column} expected ${expected}, got ${typeof val} at row ${i}` });
        }
      });
      break;
    }
    
    case 'range': {
      const { min, max } = rule;
      rows.forEach((row, i) => {
        const val = row[column];
        if (val !== null) {
          if (min !== undefined && val < min) {
            errors.push({ row: i, column, rule: 'range',
              message: `${column}=${val} below minimum ${min} at row ${i}` });
          }
          if (max !== undefined && val > max) {
            errors.push({ row: i, column, rule: 'range',
              message: `${column}=${val} above maximum ${max} at row ${i}` });
          }
        }
      });
      break;
    }
    
    case 'pattern': {
      const regex = new RegExp(rule.pattern);
      rows.forEach((row, i) => {
        const val = row[column];
        if (val !== null && !regex.test(String(val))) {
          errors.push({ row: i, column, rule: 'pattern',
            message: `${column}='${val}' doesn't match pattern ${rule.pattern} at row ${i}` });
        }
      });
      break;
    }
    
    case 'enum': {
      const allowed = new Set(rule.values);
      rows.forEach((row, i) => {
        const val = row[column];
        if (val !== null && !allowed.has(val)) {
          errors.push({ row: i, column, rule: 'enum',
            message: `${column}='${val}' not in allowed values at row ${i}` });
        }
      });
      break;
    }
    
    case 'custom': {
      rows.forEach((row, i) => {
        if (!rule.fn(row[column], row)) {
          errors.push({ row: i, column, rule: 'custom',
            message: rule.message || `Custom validation failed for ${column} at row ${i}` });
        }
      });
      break;
    }
  }
  
  return errors;
}

// Convenience rule builders
export const rules = {
  notNull: () => ({ type: 'notNull' }),
  unique: () => ({ type: 'unique' }),
  type: (expected) => ({ type: 'type', expected }),
  range: (min, max) => ({ type: 'range', min, max }),
  pattern: (pattern) => ({ type: 'pattern', pattern }),
  enum: (...values) => ({ type: 'enum', values }),
  custom: (fn, message) => ({ type: 'custom', fn, message }),
};
