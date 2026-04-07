// rtree.js — R-tree spatial index for HenryDB
// Stores rectangles (bounding boxes) for efficient spatial queries.
// Used in PostGIS, SQLite R*Tree, and other spatial databases.

class Rect {
  constructor(minX, minY, maxX, maxY) {
    this.minX = minX;
    this.minY = minY;
    this.maxX = maxX;
    this.maxY = maxY;
  }

  area() {
    return (this.maxX - this.minX) * (this.maxY - this.minY);
  }

  enlargement(other) {
    const merged = Rect.merge(this, other);
    return merged.area() - this.area();
  }

  overlaps(other) {
    return !(this.maxX < other.minX || this.minX > other.maxX ||
             this.maxY < other.minY || this.minY > other.maxY);
  }

  contains(other) {
    return this.minX <= other.minX && this.maxX >= other.maxX &&
           this.minY <= other.minY && this.maxY >= other.maxY;
  }

  static merge(a, b) {
    return new Rect(
      Math.min(a.minX, b.minX), Math.min(a.minY, b.minY),
      Math.max(a.maxX, b.maxX), Math.max(a.maxY, b.maxY)
    );
  }

  static point(x, y) {
    return new Rect(x, y, x, y);
  }
}

class RTreeNode {
  constructor(isLeaf = true) {
    this.isLeaf = isLeaf;
    this.entries = []; // { rect, data } for leaves, { rect, child } for internal
    this.rect = null; // Bounding box of all entries
  }

  updateBounds() {
    if (this.entries.length === 0) { this.rect = null; return; }
    this.rect = this.entries[0].rect;
    for (let i = 1; i < this.entries.length; i++) {
      this.rect = Rect.merge(this.rect, this.entries[i].rect);
    }
  }
}

/**
 * R-tree spatial index.
 * Supports insert, search (overlap), and contains queries.
 */
export class RTree {
  constructor(maxEntries = 9, minEntries = 4) {
    this._maxEntries = maxEntries;
    this._minEntries = minEntries;
    this._root = new RTreeNode(true);
    this._size = 0;
  }

  /**
   * Insert a rectangle with associated data.
   */
  insert(rect, data) {
    const entry = { rect, data };
    this._insert(entry, this._root);
    this._size++;
    
    // Handle root split
    if (this._root.entries.length > this._maxEntries) {
      const newRoot = new RTreeNode(false);
      const [node1, node2] = this._split(this._root);
      newRoot.entries.push({ rect: node1.rect, child: node1 });
      newRoot.entries.push({ rect: node2.rect, child: node2 });
      newRoot.updateBounds();
      this._root = newRoot;
    }
  }

  /**
   * Search for all entries whose bounding box overlaps the query rectangle.
   */
  search(queryRect) {
    const results = [];
    this._search(this._root, queryRect, results);
    return results;
  }

  /**
   * Search for entries within a radius of a point.
   */
  searchRadius(x, y, radius) {
    const queryRect = new Rect(x - radius, y - radius, x + radius, y + radius);
    const candidates = this.search(queryRect);
    
    // Filter to actual distance
    return candidates.filter(entry => {
      const cx = (entry.rect.minX + entry.rect.maxX) / 2;
      const cy = (entry.rect.minY + entry.rect.maxY) / 2;
      const dist = Math.sqrt((cx - x) ** 2 + (cy - y) ** 2);
      return dist <= radius;
    });
  }

  get size() { return this._size; }

  _insert(entry, node) {
    if (node.isLeaf) {
      node.entries.push(entry);
      node.updateBounds();
      return;
    }

    // Choose subtree with least enlargement
    let bestIdx = 0;
    let bestEnlargement = Infinity;
    for (let i = 0; i < node.entries.length; i++) {
      const enlargement = node.entries[i].rect.enlargement(entry.rect);
      if (enlargement < bestEnlargement) {
        bestEnlargement = enlargement;
        bestIdx = i;
      }
    }

    const child = node.entries[bestIdx].child;
    this._insert(entry, child);
    node.entries[bestIdx].rect = child.rect;
    node.updateBounds();

    // Split child if overflow
    if (child.entries.length > this._maxEntries) {
      const [node1, node2] = this._split(child);
      node.entries[bestIdx] = { rect: node1.rect, child: node1 };
      node.entries.push({ rect: node2.rect, child: node2 });
      node.updateBounds();
    }
  }

  _search(node, queryRect, results) {
    if (node.isLeaf) {
      for (const entry of node.entries) {
        if (entry.rect.overlaps(queryRect)) {
          results.push(entry);
        }
      }
      return;
    }

    for (const entry of node.entries) {
      if (entry.rect.overlaps(queryRect)) {
        this._search(entry.child, queryRect, results);
      }
    }
  }

  /**
   * Split a node into two using linear split heuristic.
   */
  _split(node) {
    const entries = [...node.entries];
    
    // Pick two seeds that are most distant
    let seed1 = 0, seed2 = 1;
    let maxDist = 0;
    for (let i = 0; i < entries.length; i++) {
      for (let j = i + 1; j < entries.length; j++) {
        const dist = Rect.merge(entries[i].rect, entries[j].rect).area();
        if (dist > maxDist) {
          maxDist = dist;
          seed1 = i;
          seed2 = j;
        }
      }
    }

    const node1 = new RTreeNode(node.isLeaf);
    const node2 = new RTreeNode(node.isLeaf);
    
    node1.entries.push(entries[seed1]);
    node2.entries.push(entries[seed2]);

    // Distribute remaining entries
    for (let i = 0; i < entries.length; i++) {
      if (i === seed1 || i === seed2) continue;
      
      node1.updateBounds();
      node2.updateBounds();
      
      const enlarge1 = node1.rect ? node1.rect.enlargement(entries[i].rect) : 0;
      const enlarge2 = node2.rect ? node2.rect.enlargement(entries[i].rect) : 0;
      
      if (enlarge1 < enlarge2) node1.entries.push(entries[i]);
      else node2.entries.push(entries[i]);
    }

    node1.updateBounds();
    node2.updateBounds();
    return [node1, node2];
  }
}

export { Rect };
