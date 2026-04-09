// gist-index.js — GiST (Generalized Search Tree) index for HenryDB
// Extensible tree index supporting containment, overlap, and nearest-neighbor queries.
// Used for range types, geometric types, full-text search, etc.

/**
 * GiSTNode — internal or leaf node of the GiST tree.
 */
class GiSTNode {
  constructor(isLeaf = false) {
    this.isLeaf = isLeaf;
    this.entries = []; // { key, child (node) } or { key, data } for leaf
    this.parent = null;
  }
}

/**
 * GiST — Generalized Search Tree.
 * Requires an operator class that defines:
 * - consistent(entry, query): could subtree contain matches?
 * - union(entries): compute bounding key for a set of entries
 * - penalty(entry, newEntry): insertion cost
 * - picksplit(entries): divide entries into two groups
 * - same(a, b): equality check
 * - distance(entry, query): for nearest-neighbor (optional)
 */
export class GiSTIndex {
  constructor(opClass, options = {}) {
    this.opClass = opClass;
    this.maxEntries = options.maxEntries || 50; // M
    this.minEntries = options.minEntries || Math.floor(this.maxEntries * 0.4);
    this.root = new GiSTNode(true);
    this._size = 0;
  }

  /**
   * Insert a key-data pair.
   */
  insert(key, data) {
    const leaf = this._chooseLeaf(this.root, key);
    leaf.entries.push({ key, data });
    this._size++;

    if (leaf.entries.length > this.maxEntries) {
      this._split(leaf);
    }
  }

  /**
   * Search for entries matching a query using the consistent predicate.
   */
  search(query) {
    const results = [];
    this._searchNode(this.root, query, results);
    return results;
  }

  /**
   * Remove an entry by key and data.
   */
  remove(key, data) {
    return this._removeFromNode(this.root, key, data);
  }

  /**
   * Nearest neighbor search (requires distance function in opClass).
   */
  nearestNeighbor(query, k = 1) {
    if (!this.opClass.distance) {
      throw new Error('Operator class does not support distance/nearest-neighbor');
    }

    const results = [];
    this._nnSearch(this.root, query, results, k);
    results.sort((a, b) => a.distance - b.distance);
    return results.slice(0, k);
  }

  get size() { return this._size; }

  // --- Internal ---

  _searchNode(node, query, results) {
    for (const entry of node.entries) {
      if (this.opClass.consistent(entry.key, query)) {
        if (node.isLeaf) {
          results.push({ key: entry.key, data: entry.data });
        } else {
          this._searchNode(entry.child, query, results);
        }
      }
    }
  }

  _chooseLeaf(node, key) {
    if (node.isLeaf) return node;

    // Find entry with minimum penalty for inserting key
    let bestEntry = node.entries[0];
    let bestPenalty = Infinity;

    for (const entry of node.entries) {
      const penalty = this.opClass.penalty(entry.key, key);
      if (penalty < bestPenalty) {
        bestPenalty = penalty;
        bestEntry = entry;
      }
    }

    // Update bounding key
    bestEntry.key = this.opClass.union([bestEntry.key, key]);
    return this._chooseLeaf(bestEntry.child, key);
  }

  _split(node) {
    const [group1, group2] = this.opClass.picksplit(node.entries);

    if (node === this.root) {
      // Split root: create two new children
      const left = new GiSTNode(node.isLeaf);
      const right = new GiSTNode(node.isLeaf);
      left.entries = group1;
      right.entries = group2;

      if (!node.isLeaf) {
        for (const e of left.entries) if (e.child) e.child.parent = left;
        for (const e of right.entries) if (e.child) e.child.parent = right;
      }

      const leftKey = this.opClass.union(group1.map(e => e.key));
      const rightKey = this.opClass.union(group2.map(e => e.key));

      node.isLeaf = false;
      node.entries = [
        { key: leftKey, child: left },
        { key: rightKey, child: right },
      ];
      left.parent = node;
      right.parent = node;
    } else {
      // Split non-root: replace node's entries with group1, create sibling with group2
      node.entries = group1;
      
      const sibling = new GiSTNode(node.isLeaf);
      sibling.entries = group2;
      sibling.parent = node.parent;

      if (!node.isLeaf) {
        for (const e of sibling.entries) if (e.child) e.child.parent = sibling;
      }

      const siblingKey = this.opClass.union(group2.map(e => e.key));
      const nodeKey = this.opClass.union(group1.map(e => e.key));

      // Update parent
      if (node.parent) {
        const parentEntry = node.parent.entries.find(e => e.child === node);
        if (parentEntry) parentEntry.key = nodeKey;
        node.parent.entries.push({ key: siblingKey, child: sibling });

        if (node.parent.entries.length > this.maxEntries) {
          this._split(node.parent);
        }
      }
    }
  }

  _removeFromNode(node, key, data) {
    for (let i = 0; i < node.entries.length; i++) {
      const entry = node.entries[i];
      if (node.isLeaf) {
        if (this.opClass.same(entry.key, key) && entry.data === data) {
          node.entries.splice(i, 1);
          this._size--;
          return true;
        }
      } else {
        if (this.opClass.consistent(entry.key, key)) {
          if (this._removeFromNode(entry.child, key, data)) {
            // Update bounding key
            if (entry.child.entries.length > 0) {
              entry.key = this.opClass.union(entry.child.entries.map(e => e.key));
            }
            return true;
          }
        }
      }
    }
    return false;
  }

  _nnSearch(node, query, results, k) {
    const entries = node.entries.map(e => ({
      ...e,
      distance: this.opClass.distance(e.key, query),
    }));
    entries.sort((a, b) => a.distance - b.distance);

    for (const entry of entries) {
      if (results.length >= k && entry.distance > results[results.length - 1].distance) {
        break; // Prune: can't be closer
      }

      if (node.isLeaf) {
        results.push({ key: entry.key, data: entry.data, distance: entry.distance });
        results.sort((a, b) => a.distance - b.distance);
        if (results.length > k) results.pop();
      } else {
        this._nnSearch(entry.child, query, results, k);
      }
    }
  }
}

/**
 * Range operator class — for range type containment/overlap queries.
 */
export const RangeOpClass = {
  consistent(entryKey, query) {
    // Does the range entryKey overlap with query range?
    if (query.op === 'contains') {
      return entryKey[0] <= query.value && entryKey[1] >= query.value;
    }
    if (query.op === 'overlaps') {
      return entryKey[0] <= query.range[1] && entryKey[1] >= query.range[0];
    }
    // Default: check overlap
    if (Array.isArray(query)) {
      return entryKey[0] <= query[1] && entryKey[1] >= query[0];
    }
    return entryKey[0] <= query && entryKey[1] >= query;
  },

  union(keys) {
    let min = Infinity, max = -Infinity;
    for (const k of keys) {
      if (Array.isArray(k)) {
        if (k[0] < min) min = k[0];
        if (k[1] > max) max = k[1];
      } else {
        if (k < min) min = k;
        if (k > max) max = k;
      }
    }
    return [min, max];
  },

  penalty(existing, newKey) {
    const union = RangeOpClass.union([existing, newKey]);
    const existingSize = existing[1] - existing[0];
    const unionSize = union[1] - union[0];
    return unionSize - existingSize;
  },

  picksplit(entries) {
    // Sort by start of range, split in half
    const sorted = [...entries].sort((a, b) => {
      const aStart = Array.isArray(a.key) ? a.key[0] : a.key;
      const bStart = Array.isArray(b.key) ? b.key[0] : b.key;
      return aStart - bStart;
    });
    const mid = Math.ceil(sorted.length / 2);
    return [sorted.slice(0, mid), sorted.slice(mid)];
  },

  same(a, b) {
    return a[0] === b[0] && a[1] === b[1];
  },

  distance(key, query) {
    const mid = (key[0] + key[1]) / 2;
    const qMid = Array.isArray(query) ? (query[0] + query[1]) / 2 : query;
    return Math.abs(mid - qMid);
  },
};

/**
 * Point2D operator class — for 2D geometric point queries.
 */
export const Point2DOpClass = {
  consistent(bbox, query) {
    if (query.op === 'contains_point') {
      return bbox[0] <= query.x && bbox[1] <= query.y &&
             bbox[2] >= query.x && bbox[3] >= query.y;
    }
    if (query.op === 'intersects_box') {
      return bbox[0] <= query.x2 && bbox[2] >= query.x1 &&
             bbox[1] <= query.y2 && bbox[3] >= query.y1;
    }
    return true;
  },

  union(keys) {
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const k of keys) {
      if (k.length === 4) {
        if (k[0] < minX) minX = k[0];
        if (k[1] < minY) minY = k[1];
        if (k[2] > maxX) maxX = k[2];
        if (k[3] > maxY) maxY = k[3];
      } else if (k.length === 2) {
        if (k[0] < minX) minX = k[0];
        if (k[1] < minY) minY = k[1];
        if (k[0] > maxX) maxX = k[0];
        if (k[1] > maxY) maxY = k[1];
      }
    }
    return [minX, minY, maxX, maxY];
  },

  penalty(existing, newKey) {
    const union = Point2DOpClass.union([existing, newKey]);
    const existArea = (existing[2] - existing[0]) * (existing[3] - existing[1]);
    const unionArea = (union[2] - union[0]) * (union[3] - union[1]);
    return unionArea - existArea;
  },

  picksplit(entries) {
    const sorted = [...entries].sort((a, b) => a.key[0] - b.key[0]);
    const mid = Math.ceil(sorted.length / 2);
    return [sorted.slice(0, mid), sorted.slice(mid)];
  },

  same(a, b) {
    return a[0] === b[0] && a[1] === b[1] &&
           (a.length <= 2 || (a[2] === b[2] && a[3] === b[3]));
  },

  distance(key, query) {
    const cx = key.length === 4 ? (key[0] + key[2]) / 2 : key[0];
    const cy = key.length === 4 ? (key[1] + key[3]) / 2 : key[1];
    const qx = query.x ?? query[0];
    const qy = query.y ?? query[1];
    return Math.sqrt((cx - qx) ** 2 + (cy - qy) ** 2);
  },
};
