// r-tree.js — R-tree spatial index
// Indexes 2D rectangles for spatial queries.
// Used in: PostGIS, Oracle Spatial, SQLite R*tree.

class RTreeNode {
  constructor(isLeaf = true) {
    this.isLeaf = isLeaf;
    this.entries = [];
    this.bbox = null;
  }
}

function bboxArea(b) { return (b.maxX - b.minX) * (b.maxY - b.minY); }
function bboxEnlargement(b, entry) {
  const newArea = (Math.max(b.maxX, entry.maxX) - Math.min(b.minX, entry.minX)) *
                  (Math.max(b.maxY, entry.maxY) - Math.min(b.minY, entry.minY));
  return newArea - bboxArea(b);
}
function bboxUnion(a, b) {
  if (!a) return b; if (!b) return a;
  return {
    minX: Math.min(a.minX, b.minX), minY: Math.min(a.minY, b.minY),
    maxX: Math.max(a.maxX, b.maxX), maxY: Math.max(a.maxY, b.maxY),
  };
}
function bboxOverlaps(a, b) {
  return a.minX <= b.maxX && a.maxX >= b.minX && a.minY <= b.maxY && a.maxY >= b.minY;
}
function recalcBBox(node) {
  let bbox = null;
  for (const e of node.entries) bbox = bboxUnion(bbox, e.bbox);
  return bbox;
}

export class RTree {
  constructor(maxEntries = 9) {
    this.maxEntries = maxEntries;
    this._root = new RTreeNode(true);
    this._size = 0;
  }

  get size() { return this._size; }

  insert(minX, minY, maxX, maxY, data) {
    const bbox = { minX, minY, maxX, maxY };
    const entry = { bbox, data };

    // Walk down, recording path
    const path = [];
    let node = this._root;
    while (!node.isLeaf) {
      let best = null, bestEnl = Infinity, bestArea = Infinity;
      for (const e of node.entries) {
        const enl = bboxEnlargement(e.bbox, bbox);
        const area = bboxArea(e.bbox);
        if (enl < bestEnl || (enl === bestEnl && area < bestArea)) {
          best = e; bestEnl = enl; bestArea = area;
        }
      }
      path.push({ node, chosen: best });
      node = best.child;
    }

    // Insert into leaf
    node.entries.push(entry);
    node.bbox = recalcBBox(node);

    // Split upward if needed
    let splitNode = null;
    if (node.entries.length > this.maxEntries) {
      splitNode = this._splitNode(node);
    }

    // Adjust path upward
    for (let i = path.length - 1; i >= 0; i--) {
      const { node: parent, chosen } = path[i];
      chosen.bbox = node.bbox; // Update bbox of child entry
      parent.bbox = recalcBBox(parent);

      if (splitNode) {
        parent.entries.push({ bbox: splitNode.bbox, child: splitNode });
        parent.bbox = recalcBBox(parent);
        splitNode = null;
        if (parent.entries.length > this.maxEntries) {
          splitNode = this._splitNode(parent);
        }
      }
      node = parent;
    }

    // Root split
    if (splitNode) {
      const newRoot = new RTreeNode(false);
      newRoot.entries = [
        { bbox: this._root.bbox, child: this._root },
        { bbox: splitNode.bbox, child: splitNode },
      ];
      newRoot.bbox = bboxUnion(this._root.bbox, splitNode.bbox);
      this._root = newRoot;
    }

    this._size++;
  }

  search(minX, minY, maxX, maxY) {
    const query = { minX, minY, maxX, maxY };
    const results = [];
    this._search(this._root, query, results);
    return results;
  }

  searchPoint(x, y) { return this.search(x, y, x, y); }

  _search(node, query, results) {
    if (!node || !node.bbox || !bboxOverlaps(node.bbox, query)) return;
    if (node.isLeaf) {
      for (const e of node.entries) {
        if (bboxOverlaps(e.bbox, query)) results.push(e);
      }
    } else {
      for (const e of node.entries) {
        if (bboxOverlaps(e.bbox, query)) this._search(e.child, query, results);
      }
    }
  }

  _splitNode(node) {
    node.entries.sort((a, b) => a.bbox.minX - b.bbox.minX);
    const mid = Math.ceil(node.entries.length / 2);
    const newNode = new RTreeNode(node.isLeaf);
    newNode.entries = node.entries.splice(mid);
    node.bbox = recalcBBox(node);
    newNode.bbox = recalcBBox(newNode);
    return newNode;
  }
}
