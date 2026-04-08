// zone-maps.js — Zone maps (min/max per data page) for skip-scan optimization
// Track min/max value per page (chunk of rows). When scanning with a predicate,
// skip entire pages that can't contain matching rows.
//
// Used by: Parquet (row group statistics), DuckDB (zone maps), SQL Server (columnstore segment elimination).

const DEFAULT_PAGE_SIZE = 1024;

/**
 * ZoneMap — tracks min/max per page for a numeric column.
 */
export class ZoneMap {
  constructor(pageSize = DEFAULT_PAGE_SIZE) {
    this.pageSize = pageSize;
    this.pages = []; // { min, max, offset, count }
    this._currentPage = null;
    this._totalRows = 0;
  }

  /**
   * Append a value to the zone map.
   * Automatically starts new pages when pageSize is reached.
   */
  push(value) {
    if (!this._currentPage || this._currentPage.count >= this.pageSize) {
      this._currentPage = { min: Infinity, max: -Infinity, offset: this._totalRows, count: 0 };
      this.pages.push(this._currentPage);
    }

    if (value != null) {
      const v = typeof value === 'number' ? value : value;
      if (v < this._currentPage.min) this._currentPage.min = v;
      if (v > this._currentPage.max) this._currentPage.max = v;
    }
    this._currentPage.count++;
    this._totalRows++;
  }

  /**
   * Get pages that might contain rows matching a predicate.
   * Returns array of { offset, count } for pages that can't be skipped.
   */
  getMatchingPages(predicate) {
    return this.pages.filter(page => predicate(page.min, page.max));
  }

  /**
   * Equality scan: which pages might contain value?
   */
  pagesForEquals(value) {
    return this.getMatchingPages((min, max) => value >= min && value <= max);
  }

  /**
   * Range scan: which pages might overlap [lo, hi]?
   */
  pagesForRange(lo, hi) {
    return this.getMatchingPages((min, max) => max >= lo && min <= hi);
  }

  /**
   * Greater-than scan: which pages might contain values > threshold?
   */
  pagesForGT(threshold) {
    return this.getMatchingPages((min, max) => max > threshold);
  }

  /**
   * Less-than scan: which pages might contain values < threshold?
   */
  pagesForLT(threshold) {
    return this.getMatchingPages((min, max) => min < threshold);
  }

  /**
   * Scan with skip: given data array and predicate, use zone map to skip pages.
   * Returns matching indices.
   */
  scanWithSkip(data, filterPredicate, zoneMapPredicate) {
    const candidatePages = this.getMatchingPages(zoneMapPredicate);
    const result = [];

    for (const page of candidatePages) {
      for (let i = page.offset; i < page.offset + page.count; i++) {
        if (filterPredicate(data[i])) result.push(i);
      }
    }

    return new Uint32Array(result);
  }

  /**
   * Statistics about skip effectiveness.
   */
  skipStats(zoneMapPredicate) {
    const totalPages = this.pages.length;
    const candidatePages = this.getMatchingPages(zoneMapPredicate).length;
    const skippedPages = totalPages - candidatePages;
    return {
      totalPages,
      candidatePages,
      skippedPages,
      skipRate: totalPages > 0 ? (skippedPages / totalPages * 100).toFixed(1) + '%' : '0%',
      totalRows: this._totalRows,
      candidateRows: candidatePages * this.pageSize,
    };
  }

  get totalRows() { return this._totalRows; }
  get pageCount() { return this.pages.length; }
}

/**
 * ZoneMapIndex — tracks zone maps for multiple columns of a table.
 */
export class ZoneMapIndex {
  constructor(schema, pageSize = DEFAULT_PAGE_SIZE) {
    this.schema = schema;
    this.pageSize = pageSize;
    this._zoneMaps = new Map();
    
    for (const col of schema) {
      if (col.type === 'INT' || col.type === 'FLOAT' || col.type === 'REAL' || col.type === 'BIGINT') {
        this._zoneMaps.set(col.name, new ZoneMap(pageSize));
      }
    }
  }

  /**
   * Add a row of values to all zone maps.
   */
  addRow(values) {
    for (const [name, zm] of this._zoneMaps) {
      zm.push(values[name]);
    }
  }

  /**
   * Get the zone map for a specific column.
   */
  getZoneMap(columnName) {
    return this._zoneMaps.get(columnName);
  }

  /**
   * Get candidate pages for a WHERE clause on a specific column.
   */
  getCandidatePages(columnName, operator, value) {
    const zm = this._zoneMaps.get(columnName);
    if (!zm) return null; // No zone map for this column

    switch (operator) {
      case 'EQ': return zm.pagesForEquals(value);
      case 'GT': return zm.pagesForGT(value);
      case 'GE': return zm.pagesForGT(value - 1);
      case 'LT': return zm.pagesForLT(value);
      case 'LE': return zm.pagesForLT(value + 1);
      default: return null;
    }
  }

  get columns() { return [...this._zoneMaps.keys()]; }
}
