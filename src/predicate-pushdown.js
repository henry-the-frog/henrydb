// predicate-pushdown.js — Push predicates into the storage layer using zone maps
// Instead of scanning all rows and filtering, check zone maps first to skip
// entire pages of data that can't contain matches.

import { ZoneMap, ZoneMapIndex } from './zone-maps.js';

/**
 * PushdownScanner — scans a table with zone-map-based page skipping.
 */
export class PushdownScanner {
  constructor(data, schema, zoneMapIndex) {
    this.data = data; // { columnName: array[] }
    this.schema = schema;
    this.zoneMapIndex = zoneMapIndex;
    this.stats = { totalPages: 0, skippedPages: 0, scannedRows: 0, matchedRows: 0 };
  }

  /**
   * Scan with predicate pushdown.
   * @param {Object} predicate - { column, op, value }
   * @param {string[]} projectColumns - columns to include in output
   * @param {number} limit - max rows to return
   */
  scan(predicate, projectColumns, limit = Infinity) {
    const zm = this.zoneMapIndex.getZoneMap(predicate.column);
    
    if (zm) {
      // Use zone map to skip pages
      return this._scanWithZoneMap(predicate, projectColumns, limit, zm);
    }
    
    // No zone map: full scan
    return this._fullScan(predicate, projectColumns, limit);
  }

  /**
   * Multi-predicate scan: push down multiple predicates.
   * Returns only rows matching ALL predicates.
   */
  scanMulti(predicates, projectColumns, limit = Infinity) {
    // Start with the most selective zone-mappable predicate
    let candidatePages = null;

    for (const pred of predicates) {
      const zm = this.zoneMapIndex.getZoneMap(pred.column);
      if (!zm) continue;

      const pages = zm.getMatchingPages(this._toZoneMapPredicate(pred));
      
      if (!candidatePages) {
        candidatePages = new Set(pages.map(p => p.offset));
      } else {
        const newSet = new Set(pages.map(p => p.offset));
        // Intersect
        for (const offset of candidatePages) {
          if (!newSet.has(offset)) candidatePages.delete(offset);
        }
      }
    }

    if (candidatePages && candidatePages.size === 0) {
      // All pages skipped — no matches possible
      return [];
    }

    // Scan candidate pages with all predicates
    const colData = this.data;
    const totalRows = colData[predicates[0].column]?.length || 0;
    const pageSize = this.zoneMapIndex.getZoneMap(predicates[0].column)?.pageSize || 1024;
    const results = [];

    for (let offset = 0; offset < totalRows && results.length < limit; offset += pageSize) {
      if (candidatePages && !candidatePages.has(offset)) {
        this.stats.skippedPages++;
        continue;
      }

      this.stats.totalPages++;
      const end = Math.min(offset + pageSize, totalRows);

      for (let i = offset; i < end && results.length < limit; i++) {
        this.stats.scannedRows++;
        
        let match = true;
        for (const pred of predicates) {
          const val = colData[pred.column][i];
          if (!this._evalPredicate(val, pred.op, pred.value)) {
            match = false;
            break;
          }
        }

        if (match) {
          this.stats.matchedRows++;
          const row = {};
          for (const col of projectColumns) {
            row[col] = colData[col]?.[i];
          }
          results.push(row);
        }
      }
    }

    return results;
  }

  _scanWithZoneMap(predicate, projectColumns, limit, zm) {
    const { column, op, value } = predicate;
    const zoneMapPred = this._toZoneMapPredicate(predicate);
    const candidatePages = zm.getMatchingPages(zoneMapPred);

    this.stats.totalPages = zm.pageCount;
    this.stats.skippedPages = zm.pageCount - candidatePages.length;

    const colData = this.data[column];
    const results = [];

    for (const page of candidatePages) {
      for (let i = page.offset; i < page.offset + page.count && results.length < limit; i++) {
        this.stats.scannedRows++;
        if (this._evalPredicate(colData[i], op, value)) {
          this.stats.matchedRows++;
          const row = {};
          for (const col of projectColumns) {
            row[col] = this.data[col]?.[i];
          }
          results.push(row);
        }
      }
    }

    return results;
  }

  _fullScan(predicate, projectColumns, limit) {
    const { column, op, value } = predicate;
    const colData = this.data[column];
    const results = [];

    for (let i = 0; i < colData.length && results.length < limit; i++) {
      this.stats.scannedRows++;
      if (this._evalPredicate(colData[i], op, value)) {
        this.stats.matchedRows++;
        const row = {};
        for (const col of projectColumns) {
          row[col] = this.data[col]?.[i];
        }
        results.push(row);
      }
    }

    return results;
  }

  _evalPredicate(val, op, target) {
    switch (op) {
      case 'EQ': return val === target;
      case 'NE': return val !== target;
      case 'GT': return val > target;
      case 'GE': return val >= target;
      case 'LT': return val < target;
      case 'LE': return val <= target;
      default: return true;
    }
  }

  _toZoneMapPredicate(pred) {
    switch (pred.op) {
      case 'EQ': return (min, max) => pred.value >= min && pred.value <= max;
      case 'GT': return (min, max) => max > pred.value;
      case 'GE': return (min, max) => max >= pred.value;
      case 'LT': return (min, max) => min < pred.value;
      case 'LE': return (min, max) => min <= pred.value;
      case 'NE': return () => true; // Can't skip for NE
      default: return () => true;
    }
  }

  getStats() {
    return { ...this.stats };
  }
}
