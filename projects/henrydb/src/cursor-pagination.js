// cursor-pagination.js — Keyset pagination for large result sets
// Unlike OFFSET pagination, keyset uses the last seen key to fetch next page.
// Advantages: O(1) page fetch (no offset scan), stable under inserts/deletes.

export class CursorPaginator {
  constructor(data, options = {}) {
    this._data = data;
    this.pageSize = options.pageSize || 20;
    this.orderBy = options.orderBy || 'id';
    this.direction = options.direction || 'ASC';
  }

  /**
   * Get first page.
   */
  first() {
    const sorted = this._sort();
    const items = sorted.slice(0, this.pageSize);
    return this._buildPage(items, sorted);
  }

  /**
   * Get next page after cursor.
   */
  after(cursor) {
    const sorted = this._sort();
    const startIdx = sorted.findIndex(r => this._getCursorValue(r) > cursor);
    if (startIdx < 0) return this._buildPage([], sorted);
    const items = sorted.slice(startIdx, startIdx + this.pageSize);
    return this._buildPage(items, sorted);
  }

  /**
   * Get previous page before cursor.
   */
  before(cursor) {
    const sorted = this._sort();
    let endIdx = sorted.findIndex(r => this._getCursorValue(r) >= cursor);
    if (endIdx < 0) endIdx = sorted.length;
    const startIdx = Math.max(0, endIdx - this.pageSize);
    const items = sorted.slice(startIdx, endIdx);
    return this._buildPage(items, sorted);
  }

  _sort() {
    const dir = this.direction === 'DESC' ? -1 : 1;
    return [...this._data].sort((a, b) => {
      const va = a[this.orderBy], vb = b[this.orderBy];
      return va < vb ? -dir : va > vb ? dir : 0;
    });
  }

  _getCursorValue(row) { return row[this.orderBy]; }

  _buildPage(items, sorted) {
    const firstItem = items[0];
    const lastItem = items[items.length - 1];
    const firstCursor = firstItem ? this._getCursorValue(firstItem) : null;
    const lastCursor = lastItem ? this._getCursorValue(lastItem) : null;

    // Determine if there are more pages
    const lastIdx = lastItem ? sorted.indexOf(lastItem) : -1;
    const firstIdx = firstItem ? sorted.indexOf(firstItem) : 0;

    return {
      items,
      pageInfo: {
        startCursor: firstCursor,
        endCursor: lastCursor,
        hasNextPage: lastIdx >= 0 && lastIdx < sorted.length - 1,
        hasPreviousPage: firstIdx > 0,
      },
      totalCount: sorted.length,
    };
  }
}
