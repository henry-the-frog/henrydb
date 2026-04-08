// subquery.js — Correlated subqueries: EXISTS, IN, scalar subquery
// Supports semi-join (EXISTS), anti-join (NOT EXISTS), 
// IN with subselect, and scalar subqueries in SELECT.

/**
 * SubqueryEngine — execute queries with subquery support.
 */
export class SubqueryEngine {
  constructor() {
    this._tables = new Map();
  }

  addTable(name, rows) {
    this._tables.set(name, rows);
  }

  /**
   * EXISTS subquery: return outer rows where subquery returns ≥1 row.
   * 
   * @param {string} outerTable
   * @param {Object} subquery - { table, where: (outerRow, innerRow) => boolean }
   */
  exists(outerTable, subquery) {
    const outer = this._getTable(outerTable);
    const inner = this._getTable(subquery.table);

    return outer.filter(outerRow => {
      return inner.some(innerRow => subquery.where(outerRow, innerRow));
    });
  }

  /**
   * NOT EXISTS: return outer rows where subquery returns 0 rows.
   */
  notExists(outerTable, subquery) {
    const outer = this._getTable(outerTable);
    const inner = this._getTable(subquery.table);

    return outer.filter(outerRow => {
      return !inner.some(innerRow => subquery.where(outerRow, innerRow));
    });
  }

  /**
   * IN with subselect: WHERE col IN (SELECT col FROM ...)
   * 
   * @param {string} outerTable
   * @param {string} outerCol - Column to check
   * @param {Object} subquery - { table, select: column, where?: (row) => boolean }
   */
  inSubquery(outerTable, outerCol, subquery) {
    const outer = this._getTable(outerTable);
    let inner = this._getTable(subquery.table);
    
    if (subquery.where) inner = inner.filter(subquery.where);
    const inSet = new Set(inner.map(r => r[subquery.select]));

    return outer.filter(row => inSet.has(row[outerCol]));
  }

  /**
   * NOT IN with subselect.
   */
  notInSubquery(outerTable, outerCol, subquery) {
    const outer = this._getTable(outerTable);
    let inner = this._getTable(subquery.table);
    
    if (subquery.where) inner = inner.filter(subquery.where);
    const inSet = new Set(inner.map(r => r[subquery.select]));

    return outer.filter(row => !inSet.has(row[outerCol]));
  }

  /**
   * Scalar subquery in SELECT: add a computed column from a correlated subquery.
   * 
   * @param {string} outerTable
   * @param {string} alias - Output column name
   * @param {Object} subquery - { table, select: column, agg: 'COUNT'|'SUM'|'MAX', where: (outerRow, innerRow) => boolean }
   */
  scalarSubquery(outerTable, alias, subquery) {
    const outer = this._getTable(outerTable);
    const inner = this._getTable(subquery.table);

    return outer.map(outerRow => {
      const matching = inner.filter(innerRow => subquery.where(outerRow, innerRow));
      let value;

      switch (subquery.agg) {
        case 'COUNT': value = matching.length; break;
        case 'SUM': value = matching.reduce((s, r) => s + (r[subquery.select] || 0), 0); break;
        case 'MAX': value = matching.length > 0 ? Math.max(...matching.map(r => r[subquery.select])) : null; break;
        case 'MIN': value = matching.length > 0 ? Math.min(...matching.map(r => r[subquery.select])) : null; break;
        case 'AVG': value = matching.length > 0 ? matching.reduce((s, r) => s + r[subquery.select], 0) / matching.length : null; break;
        default: value = matching.length > 0 ? matching[0][subquery.select] : null;
      }

      return { ...outerRow, [alias]: value };
    });
  }

  /**
   * Lateral join: for each outer row, run a subquery and join results.
   */
  lateral(outerTable, subquery) {
    const outer = this._getTable(outerTable);
    const inner = this._getTable(subquery.table);
    const results = [];

    for (const outerRow of outer) {
      const matching = inner.filter(innerRow => subquery.where(outerRow, innerRow));
      
      if (subquery.limit) matching.splice(subquery.limit);
      if (subquery.orderBy) {
        matching.sort((a, b) => {
          const va = a[subquery.orderBy.column];
          const vb = b[subquery.orderBy.column];
          const cmp = va < vb ? -1 : va > vb ? 1 : 0;
          return subquery.orderBy.direction === 'DESC' ? -cmp : cmp;
        });
      }

      for (const innerRow of matching) {
        results.push({ ...outerRow, ...innerRow });
      }
    }

    return results;
  }

  _getTable(name) {
    return this._tables.get(name) || [];
  }
}
