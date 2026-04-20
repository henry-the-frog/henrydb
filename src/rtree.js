// rtree.js — R-tree spatial index for 2D range and nearest-neighbor queries
// Each node contains a bounding box (MBR) that encloses all children.
// Insertions pick the subtree with least enlargement.

/**
 * Rect — axis-aligned bounding rectangle for R-tree queries.
 */
export class Rect {
  constructor(minX, minY, maxX, maxY) {
    this.minX = minX;
    this.minY = minY;
    this.maxX = maxX;
    this.maxY = maxY;
  }

  static point(x, y) {
    return new Rect(x, y, x, y);
  }

  contains(other) {
    return this.minX <= other.minX && this.minY <= other.minY &&
           this.maxX >= other.maxX && this.maxY >= other.maxY;
  }

  intersects(other) {
    return this.minX <= other.maxX && this.maxX >= other.minX &&
           this.minY <= other.maxY && this.maxY >= other.minY;
  }
}

class RTreeNode {
  constructor(isLeaf = true, maxEntries = 4) {
    this.isLeaf = isLeaf;
    this.maxEntries = maxEntries;
    this.entries = []; // leaf: {bbox, data}. internal: {bbox, child}
  }

  get mbr() {
    if (this.entries.length === 0) return { minX: 0, minY: 0, maxX: 0, maxY: 0 };
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const e of this.entries) {
      if (e.bbox.minX < minX) minX = e.bbox.minX;
      if (e.bbox.minY < minY) minY = e.bbox.minY;
      if (e.bbox.maxX > maxX) maxX = e.bbox.maxX;
      if (e.bbox.maxY > maxY) maxY = e.bbox.maxY;
    }
    return { minX, minY, maxX, maxY };
  }
}

export class RTree {
  constructor(maxEntries = 9) {
    this.maxEntries = maxEntries;
    this.root = new RTreeNode(true, maxEntries);
    this._size = 0;
  }

  /**
   * Insert a point or rectangle with associated data.
   */
  insert(bbox, data) {
    if (typeof bbox.x !== 'undefined') {
      bbox = { minX: bbox.x, minY: bbox.y, maxX: bbox.x, maxY: bbox.y };
    }
    const entry = { bbox, data };
    this._insert(entry, this.root);
    this._size++;
  }

  _insert(entry, node) {
    if (node.isLeaf) {
      node.entries.push(entry);
      if (node.entries.length > this.maxEntries) this._splitNode(node);
      return;
    }
    // Choose subtree with least enlargement
    let best = 0, bestEnlargement = Infinity;
    for (let i = 0; i < node.entries.length; i++) {
      const enl = this._enlargement(node.entries[i].bbox, entry.bbox);
      if (enl < bestEnlargement) { bestEnlargement = enl; best = i; }
    }
    this._insert(entry, node.entries[best].child);
    node.entries[best].bbox = node.entries[best].child.mbr;
  }

  _splitNode(node) {
    // Simple split: sort by X midpoint and split in half
    const sorted = [...node.entries].sort((a, b) => {
      const ax = (a.bbox.minX + a.bbox.maxX) / 2;
      const bx = (b.bbox.minX + b.bbox.maxX) / 2;
      return ax - bx;
    });
    const mid = Math.ceil(sorted.length / 2);
    
    if (node === this.root) {
      const left = new RTreeNode(node.isLeaf, this.maxEntries);
      const right = new RTreeNode(node.isLeaf, this.maxEntries);
      left.entries = sorted.slice(0, mid);
      right.entries = sorted.slice(mid);
      node.isLeaf = false;
      node.entries = [
        { bbox: left.mbr, child: left },
        { bbox: right.mbr, child: right },
      ];
    }
  }

  /**
   * Search: find all entries whose bboxes intersect the query rectangle.
   */
  search(queryBbox) {
    const results = [];
    this._search(this.root, queryBbox, results);
    return results;
  }

  _search(node, query, results) {
    for (const entry of node.entries) {
      if (!this._intersects(entry.bbox, query)) continue;
      if (node.isLeaf) {
        results.push(entry.data);
      } else {
        this._search(entry.child, query, results);
      }
    }
  }

  /**
   * Nearest neighbor search (brute force within R-tree).
   */
  nearest(point, k = 1) {
    const all = [];
    this._collectAll(this.root, all);
    all.sort((a, b) => {
      const da = this._pointDist(point, a.bbox);
      const db = this._pointDist(point, b.bbox);
      return da - db;
    });
    return all.slice(0, k).map(e => ({
      data: e.data,
      distance: Math.sqrt(this._pointDist(point, e.bbox)),
    }));
  }

  _collectAll(node, results) {
    for (const entry of node.entries) {
      if (node.isLeaf) results.push(entry);
      else this._collectAll(entry.child, results);
    }
  }

  _intersects(a, b) {
    return a.minX <= b.maxX && a.maxX >= b.minX && a.minY <= b.maxY && a.maxY >= b.minY;
  }

  _enlargement(a, b) {
    const merged = {
      minX: Math.min(a.minX, b.minX), minY: Math.min(a.minY, b.minY),
      maxX: Math.max(a.maxX, b.maxX), maxY: Math.max(a.maxY, b.maxY),
    };
    return this._area(merged) - this._area(a);
  }

  _area(bbox) {
    return (bbox.maxX - bbox.minX) * (bbox.maxY - bbox.minY);
  }

  _pointDist(point, bbox) {
    const dx = Math.max(bbox.minX - point.x, 0, point.x - bbox.maxX);
    const dy = Math.max(bbox.minY - point.y, 0, point.y - bbox.maxY);
    return dx * dx + dy * dy;
  }

  get size() { return this._size; }
}
