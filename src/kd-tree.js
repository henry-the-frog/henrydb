// kd-tree.js — k-d tree for multidimensional point search
// Supports nearest-neighbor, range search, and k-nearest neighbors.

export class KDTree {
  constructor(dimensions = 2) {
    this.dimensions = dimensions;
    this.root = null;
    this._size = 0;
  }

  /** Build from array of points [{x, y, ...data}] */
  static build(points, dimensions = 2) {
    const tree = new KDTree(dimensions);
    tree._size = points.length;
    tree.root = tree._buildRecursive(points.map(p => ({ ...p })), 0);
    return tree;
  }

  _buildRecursive(points, depth) {
    if (points.length === 0) return null;
    const dim = depth % this.dimensions;
    const key = dim === 0 ? 'x' : dim === 1 ? 'y' : `d${dim}`;
    
    points.sort((a, b) => (a[key] || 0) - (b[key] || 0));
    const mid = Math.floor(points.length / 2);
    
    return {
      point: points[mid],
      left: this._buildRecursive(points.slice(0, mid), depth + 1),
      right: this._buildRecursive(points.slice(mid + 1), depth + 1),
      dim,
    };
  }

  insert(point) {
    this.root = this._insert(this.root, point, 0);
    this._size++;
  }

  _insert(node, point, depth) {
    if (!node) return { point, left: null, right: null, dim: depth % this.dimensions };
    const key = node.dim === 0 ? 'x' : node.dim === 1 ? 'y' : `d${node.dim}`;
    if ((point[key] || 0) < (node.point[key] || 0)) node.left = this._insert(node.left, point, depth + 1);
    else node.right = this._insert(node.right, point, depth + 1);
    return node;
  }

  /** Nearest neighbor search */
  nearest(target) {
    let best = { point: null, dist: Infinity };
    this._nearestRecursive(this.root, target, best);
    return best.point;
  }

  _nearestRecursive(node, target, best) {
    if (!node) return;
    const d = this._distance(node.point, target);
    if (d < best.dist) { best.point = node.point; best.dist = d; }
    
    const key = node.dim === 0 ? 'x' : node.dim === 1 ? 'y' : `d${node.dim}`;
    const diff = (target[key] || 0) - (node.point[key] || 0);
    
    const near = diff < 0 ? node.left : node.right;
    const far = diff < 0 ? node.right : node.left;
    
    this._nearestRecursive(near, target, best);
    if (diff * diff < best.dist) this._nearestRecursive(far, target, best);
  }

  /** Range search: find all points within a bounding box */
  rangeSearch(min, max) {
    const results = [];
    this._rangeRecursive(this.root, min, max, results);
    return results;
  }

  _rangeRecursive(node, min, max, results) {
    if (!node) return;
    const p = node.point;
    if ((p.x || 0) >= (min.x || 0) && (p.x || 0) <= (max.x || 0) &&
        (p.y || 0) >= (min.y || 0) && (p.y || 0) <= (max.y || 0)) {
      results.push(p);
    }
    
    const key = node.dim === 0 ? 'x' : 'y';
    if ((min[key] || 0) <= (node.point[key] || 0)) this._rangeRecursive(node.left, min, max, results);
    if ((max[key] || 0) >= (node.point[key] || 0)) this._rangeRecursive(node.right, min, max, results);
  }

  /** K-nearest neighbors */
  kNearest(target, k) {
    const heap = []; // max-heap by distance
    this._kNearestRecursive(this.root, target, k, heap);
    return heap.sort((a, b) => a.dist - b.dist).map(h => h.point);
  }

  _kNearestRecursive(node, target, k, heap) {
    if (!node) return;
    const d = this._distance(node.point, target);
    
    if (heap.length < k) {
      heap.push({ point: node.point, dist: d });
      heap.sort((a, b) => b.dist - a.dist); // max at front
    } else if (d < heap[0].dist) {
      heap[0] = { point: node.point, dist: d };
      heap.sort((a, b) => b.dist - a.dist);
    }
    
    const key = node.dim === 0 ? 'x' : 'y';
    const diff = (target[key] || 0) - (node.point[key] || 0);
    const near = diff < 0 ? node.left : node.right;
    const far = diff < 0 ? node.right : node.left;
    
    this._kNearestRecursive(near, target, k, heap);
    if (heap.length < k || diff * diff < heap[0].dist) {
      this._kNearestRecursive(far, target, k, heap);
    }
  }

  _distance(a, b) {
    const dx = (a.x || 0) - (b.x || 0);
    const dy = (a.y || 0) - (b.y || 0);
    return dx * dx + dy * dy; // Squared Euclidean
  }

  get size() { return this._size; }
}
