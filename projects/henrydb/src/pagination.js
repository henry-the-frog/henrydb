// pagination.js — Query result pagination strategies for HenryDB
// OFFSET/LIMIT, keyset pagination, cursor-based API pagination.

/**
 * OffsetPaginator — traditional OFFSET/LIMIT pagination.
 * Simple but O(n) for large offsets.
 */
export class OffsetPaginator {
  paginate(rows, page, pageSize) {
    const offset = (page - 1) * pageSize;
    const slice = rows.slice(offset, offset + pageSize);
    return {
      data: slice,
      page,
      pageSize,
      totalRows: rows.length,
      totalPages: Math.ceil(rows.length / pageSize),
      hasNextPage: offset + pageSize < rows.length,
      hasPrevPage: page > 1,
    };
  }
}

/**
 * KeysetPaginator — keyset (seek) pagination using WHERE conditions.
 * O(1) for any page — efficient for large datasets.
 * Requires a sorted, unique column (typically primary key or timestamp).
 */
export class KeysetPaginator {
  constructor(sortColumn, sortDirection = 'asc') {
    this.sortColumn = sortColumn;
    this.sortDirection = sortDirection;
  }

  /**
   * Get next page after a cursor value.
   */
  paginate(rows, cursor, pageSize) {
    let filtered = rows;

    if (cursor !== null && cursor !== undefined) {
      if (this.sortDirection === 'asc') {
        filtered = rows.filter(r => r[this.sortColumn] > cursor);
      } else {
        filtered = rows.filter(r => r[this.sortColumn] < cursor);
      }
    }

    // Sort
    filtered.sort((a, b) => {
      const va = a[this.sortColumn];
      const vb = b[this.sortColumn];
      return this.sortDirection === 'asc'
        ? (va < vb ? -1 : va > vb ? 1 : 0)
        : (va > vb ? -1 : va < vb ? 1 : 0);
    });

    const data = filtered.slice(0, pageSize);
    const nextCursor = data.length > 0 ? data[data.length - 1][this.sortColumn] : null;
    const hasMore = filtered.length > pageSize;

    return {
      data,
      cursor: nextCursor,
      hasMore,
      pageSize,
    };
  }
}

/**
 * CursorPaginator — opaque cursor-based pagination for APIs.
 * Encodes cursor position in base64, supports forward and backward.
 */
export class CursorPaginator {
  constructor(sortColumn = 'id') {
    this.sortColumn = sortColumn;
  }

  /**
   * Paginate with first/after or last/before.
   */
  paginate(rows, options = {}) {
    const { first, after, last, before } = options;
    let sorted = [...rows].sort((a, b) => {
      const va = a[this.sortColumn];
      const vb = b[this.sortColumn];
      return va < vb ? -1 : va > vb ? 1 : 0;
    });

    let startIdx = 0;
    let endIdx = sorted.length;

    // Apply after cursor
    if (after) {
      const afterVal = this._decodeCursor(after);
      startIdx = sorted.findIndex(r => r[this.sortColumn] > afterVal);
      if (startIdx < 0) startIdx = sorted.length;
    }

    // Apply before cursor
    if (before) {
      const beforeVal = this._decodeCursor(before);
      const idx = sorted.findIndex(r => r[this.sortColumn] >= beforeVal);
      if (idx >= 0) endIdx = idx;
    }

    let slice = sorted.slice(startIdx, endIdx);

    // Apply first/last
    if (first !== undefined) {
      slice = slice.slice(0, first);
    } else if (last !== undefined) {
      slice = slice.slice(-last);
    }

    // Build edges with cursors
    const edges = slice.map(node => ({
      node,
      cursor: this._encodeCursor(node[this.sortColumn]),
    }));

    const firstEdge = edges.length > 0 ? edges[0] : null;
    const lastEdge = edges.length > 0 ? edges[edges.length - 1] : null;

    return {
      edges,
      pageInfo: {
        hasNextPage: lastEdge
          ? sorted.some(r => r[this.sortColumn] > this._decodeCursor(lastEdge.cursor))
          : false,
        hasPreviousPage: firstEdge
          ? sorted.some(r => r[this.sortColumn] < this._decodeCursor(firstEdge.cursor))
          : false,
        startCursor: firstEdge?.cursor || null,
        endCursor: lastEdge?.cursor || null,
      },
      totalCount: rows.length,
    };
  }

  _encodeCursor(value) {
    return Buffer.from(JSON.stringify(value)).toString('base64');
  }

  _decodeCursor(cursor) {
    return JSON.parse(Buffer.from(cursor, 'base64').toString());
  }
}
