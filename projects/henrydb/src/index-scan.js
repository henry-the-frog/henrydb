// index-scan.js — Extracted from db.js (2026-04-23)
// Index scan optimization: BTreeTable PK lookup, secondary index scan, composite index prefix matching

import { BTreeTable } from './btree-table.js';
import { BPlusTree } from './btree.js';

/**
 * Try to use an index for scanning instead of full table scan.
 * Handles: PK equality, PK range, secondary index equality/range, composite prefix.
 * @param {object} db - Database instance
 * @param {object} table - Table object
 * @param {object} where - WHERE clause AST
 * @param {string} tableAlias - Table alias
 * @returns {Array|null} Matching rows or null if index scan not possible
 */
export function
tryIndexScan(db, table, where, tableAlias) {
  if (!where) return null;

  // Fast path: BTreeTable PK equality lookup — O(log n) without secondary index
  if (where.type === 'COMPARE' && where.op === 'EQ' && table.heap instanceof BTreeTable) {
    const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
    const literal = where.left.type === 'literal' ? where.left : (where.right.type === 'literal' ? where.right : null);
    if (colRef && literal) {
      const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
      const pkColNames = table.heap.pkIndices.map(i => table.schema[i]?.name);
      if (pkColNames.length === 1 && pkColNames[0] === colName) {
        // Direct B+tree lookup — no secondary index needed
        const values = table.heap.findByPK(literal.value);
        if (values) {
          const row = db._valuesToRow(values, table.schema, tableAlias);
          return { rows: [row], residual: null, btreeLookup: true };
        }
        return { rows: [], residual: null, btreeLookup: true };
      }
    }
  }

  // Simple equality: col = literal where col is indexed
  if (where.type === 'COMPARE' && where.op === 'EQ') {
    const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
    const literal = where.left.type === 'literal' ? where.left : (where.right.type === 'literal' ? where.right : null);
    if (colRef && literal) {
      const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
      const index = table.indexes.get(colName);
      if (index) {
        // Hash index: use get() for equality lookup — may return array of rids
        // B+tree index: use range() for equality
        let entries;
        if (index._isHash) {
          const val = index.get(literal.value);
          if (val !== undefined) {
            // Hash index stores arrays of rids for non-unique indexes
            const rids = Array.isArray(val) ? val : [val];
            entries = rids.map(rid => ({ key: literal.value, value: rid }));
          } else {
            entries = [];
          }
        } else {
          entries = index.range(literal.value, literal.value);
        }
        const rows = [];
        for (const entry of entries) {
          const rid = entry.value;
          // Check for index-only scan possibility
          if (rid.includedValues && db._requestedColumns) {
            const neededCols = db._requestedColumns;
            const indexCols = new Set([colName, ...Object.keys(rid.includedValues)]);
            const allCovered = neededCols.every(c => indexCols.has(c));
            if (allCovered) {
              // Index-only scan: build row from index data
              const row = {};
              row[colName] = literal.value;
              if (tableAlias) row[`${tableAlias}.${colName}`] = literal.value;
              for (const [k, v] of Object.entries(rid.includedValues)) {
                row[k] = v;
                if (tableAlias) row[`${tableAlias}.${k}`] = v;
              }
              rows.push(row);
              continue;
            }
          }
          // Fall back to heap access
          const values = db._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
          if (values) {
            rows.push(db._valuesToRow(values, table.schema, tableAlias));
          }
        }
        // MVCC fallback: if index had entries but all heap lookups returned null
        // (invisible under current snapshot), fall back to full scan to find
        // the version visible to this transaction.
        if (rows.length === 0 && entries.length > 0) {
          const fallbackRows = [];
          for (const { pageId, slotIdx, values } of table.heap.scan()) {
            const row = db._valuesToRow(values, table.schema, tableAlias);
            fallbackRows.push(row);
          }
          return { rows: fallbackRows, residual: where };
        }
        return { rows, residual: null, indexOnly: rows.length > 0 && rows[0]?.includedValues !== undefined };
      }
    }
  }

  // Range comparison: col > literal, col >= literal, col < literal, col <= literal
  if (where.type === 'COMPARE' && ['GT', 'GTE', 'LT', 'LTE'].includes(where.op)) {
    const colRef = where.left.type === 'column_ref' ? where.left : (where.right.type === 'column_ref' ? where.right : null);
    const literal = where.left.type === 'literal' ? where.left : (where.right.type === 'literal' ? where.right : null);
    if (colRef && literal) {
      const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
      const index = table.indexes.get(colName);
      if (index && !index._isHash && index.range) {
        // B+tree scan — iterate all index entries and filter by comparison
        const isColLeft = where.left.type === 'column_ref';
        const rows = [];
        for (const entry of index.scan()) {
          const val = entry.key;
          let passes;
          if (isColLeft) {
            switch (where.op) {
              case 'GT':  passes = val > literal.value; break;
              case 'GTE': passes = val >= literal.value; break;
              case 'LT':  passes = val < literal.value; break;
              case 'LTE': passes = val <= literal.value; break;
            }
          } else {
            switch (where.op) {
              case 'GT':  passes = val < literal.value; break;
              case 'GTE': passes = val <= literal.value; break;
              case 'LT':  passes = val > literal.value; break;
              case 'LTE': passes = val >= literal.value; break;
            }
          }
          if (!passes) continue;
          const rid = entry.value;
          const values = db._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
          if (values) {
            rows.push(db._valuesToRow(values, table.schema, tableAlias));
          }
        }
        return { rows, residual: null };
      }
      
      // Try composite index prefix matching: WHERE a = val using index (a, b, c)
      if (!index) {
        const compositeHit = db._tryCompositeIndexPrefix(table, tableAlias, [{ col: colName, value: literal.value }]);
        if (compositeHit) return compositeHit;
      }
    }
  }

  // AND: try to combine conditions for composite index prefix matching
  if (where.type === 'AND') {
    const eqConditions = db._extractEqualityConditions(where, tableAlias);
    if (eqConditions.length >= 2) {
      const compositeHit = db._tryCompositeIndexPrefix(table, tableAlias, eqConditions);
      if (compositeHit) return compositeHit;
    }
  }

  // BETWEEN: col BETWEEN lo AND hi
  if (where.type === 'BETWEEN') {
    const colRef = (where.left?.type === 'column_ref') ? where.left : (where.expr?.type === 'column_ref' ? where.expr : null);
    if (colRef) {
      const colName = colRef.name.includes('.') ? colRef.name.split('.').pop() : colRef.name;
      const index = table.indexes.get(colName);
      if (index && !index._isHash && where.low?.type === 'literal' && where.high?.type === 'literal') {
        let lo = where.low.value, hi = where.high.value;
        if (where.symmetric && lo > hi) { const tmp = lo; lo = hi; hi = tmp; }
        const entries = index.range(lo, hi);
        const rows = [];
        for (const entry of entries) {
          const rid = entry.value;
          const values = db._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
          if (values) {
            rows.push(db._valuesToRow(values, table.schema, tableAlias));
          }
        }
        return { rows, residual: null };
      }
    }
  }

  // IN list: col IN (val1, val2, ...)
  if (where.type === 'IN_LIST' && where.left?.type === 'column_ref') {
    const colName = where.left.name.includes('.') ? where.left.name.split('.').pop() : where.left.name;
    const index = table.indexes.get(colName);
    if (index && where.values.every(v => v.type === 'literal')) {
      const rows = [];
      const seen = new Set(); // dedup by pageId+slotIdx
      for (const val of where.values) {
        let entries;
        if (index._isHash) {
          const found = index.get(val.value);
          if (found !== undefined) {
            const rids = Array.isArray(found) ? found : [found];
            entries = rids.map(rid => ({ key: val.value, value: rid }));
          } else {
            entries = [];
          }
        } else {
          entries = index.range(val.value, val.value);
        }
        for (const entry of entries) {
          const rid = entry.value;
          const key = `${rid.pageId}:${rid.slotIdx}`;
          if (seen.has(key)) continue;
          seen.add(key);
          const values = db._heapGetFollowHot(table.heap, rid.pageId, rid.slotIdx);
          if (values) {
            rows.push(db._valuesToRow(values, table.schema, tableAlias));
          }
        }
      }
      return { rows, residual: null };
    }
  }

  // AND: try to use index on one side, residual on the other
  if (where.type === 'AND') {
    const leftScan = tryIndexScan(db, table, where.left, tableAlias);
    if (leftScan) {
      return { rows: leftScan.rows, residual: where.right };
    }
    const rightScan = tryIndexScan(db, table, where.right, tableAlias);
    if (rightScan) {
      return { rows: rightScan.rows, residual: where.left };
    }
  }

  // OR: if both sides can use indexes, union the results (bitmap OR)
  if (where.type === 'OR') {
    const leftScan = tryIndexScan(db, table, where.left, tableAlias);
    const rightScan = tryIndexScan(db, table, where.right, tableAlias);
    if (leftScan && rightScan) {
      // Union: deduplicate by row identity
      const seen = new Set();
      const rows = [];
      for (const row of leftScan.rows) {
        // Use all column values as key for dedup
        const key = JSON.stringify(Object.entries(row).filter(([k]) => !k.includes('.')).sort());
        if (!seen.has(key)) {
          seen.add(key);
          rows.push(row);
        }
      }
      for (const row of rightScan.rows) {
        const key = JSON.stringify(Object.entries(row).filter(([k]) => !k.includes('.')).sort());
        if (!seen.has(key)) {
          seen.add(key);
          rows.push(row);
        }
      }
      // Apply residuals from both sides (already handled within each scan)
      const leftResidual = leftScan.residual;
      const rightResidual = rightScan.residual;
      // If either side had a residual, we need to re-evaluate the original OR condition
      // on the unioned rows to ensure correctness
      if (leftResidual || rightResidual) {
        return { rows, residual: where };
      }
      return { rows, residual: null };
    }
  }

  return null;
}
