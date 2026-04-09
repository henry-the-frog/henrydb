// kd-tree.js — K-Dimensional Tree for spatial search
// Partitions space by alternating dimensions. O(log n) nearest neighbor.
// Used in: PostGIS (spatial queries), machine learning (KNN), ray tracing.

class KDNode {
  constructor(point, value, axis) {
    this.point = point;
    this.value = value;
    this.axis = axis;
    this.left = null;
    this.right = null;
  }
}

export class KDTree {
  constructor(k = 2) {
    this._k = k;
    this._root = null;
    this._size = 0;
  }

  get size() { return this._size; }

  /** Insert a point. */
  insert(point, value) {
    this._root = this._insert(this._root, point, value, 0);
    this._size++;
  }

  /** Build from array of {point, value} for balanced tree. */
  static build(points, k = 2) {
    const tree = new KDTree(k);
    tree._root = tree._buildBalanced(points.slice(), 0);
    tree._size = points.length;
    return tree;
  }

  /** Find nearest neighbor to query point. */
  nearest(query) {
    let best = { point: null, value: undefined, dist: Infinity };
    this._nearest(this._root, query, best);
    return best.point ? { point: best.point, value: best.value, distance: Math.sqrt(best.dist) } : null;
  }

  /** Find all points within radius. */
  rangeSearch(center, radius) {
    const results = [];
    const r2 = radius * radius;
    this._rangeSearch(this._root, center, r2, results);
    return results.map(r => ({ ...r, distance: Math.sqrt(r.dist) }));
  }

  /** K nearest neighbors. */
  knn(query, k) {
    const results = [];
    this._knn(this._root, query, k, results);
    results.sort((a, b) => a.dist - b.dist);
    return results.slice(0, k).map(r => ({ point: r.point, value: r.value, distance: Math.sqrt(r.dist) }));
  }

  _insert(node, point, value, depth) {
    if (!node) return new KDNode(point, value, depth % this._k);
    const axis = depth % this._k;
    if (point[axis] < node.point[axis]) node.left = this._insert(node.left, point, value, depth + 1);
    else node.right = this._insert(node.right, point, value, depth + 1);
    return node;
  }

  _buildBalanced(points, depth) {
    if (points.length === 0) return null;
    const axis = depth % this._k;
    points.sort((a, b) => a.point[axis] - b.point[axis]);
    const mid = Math.floor(points.length / 2);
    const node = new KDNode(points[mid].point, points[mid].value, axis);
    node.left = this._buildBalanced(points.slice(0, mid), depth + 1);
    node.right = this._buildBalanced(points.slice(mid + 1), depth + 1);
    return node;
  }

  _nearest(node, query, best) {
    if (!node) return;
    const d = this._dist2(node.point, query);
    if (d < best.dist) { best.dist = d; best.point = node.point; best.value = node.value; }
    
    const axis = node.axis;
    const diff = query[axis] - node.point[axis];
    const near = diff < 0 ? node.left : node.right;
    const far = diff < 0 ? node.right : node.left;
    
    this._nearest(near, query, best);
    if (diff * diff < best.dist) this._nearest(far, query, best);
  }

  _rangeSearch(node, center, r2, results) {
    if (!node) return;
    const d = this._dist2(node.point, center);
    if (d <= r2) results.push({ point: node.point, value: node.value, dist: d });
    
    const axis = node.axis;
    const diff = center[axis] - node.point[axis];
    if (diff - Math.sqrt(r2) <= 0) this._rangeSearch(node.left, center, r2, results);
    if (diff + Math.sqrt(r2) >= 0) this._rangeSearch(node.right, center, r2, results);
  }

  _knn(node, query, k, results) {
    if (!node) return;
    const d = this._dist2(node.point, query);
    results.push({ point: node.point, value: node.value, dist: d });
    results.sort((a, b) => a.dist - b.dist);
    if (results.length > k * 2) results.length = k * 2; // Rough prune
    
    const axis = node.axis;
    const diff = query[axis] - node.point[axis];
    const near = diff < 0 ? node.left : node.right;
    const far = diff < 0 ? node.right : node.left;
    
    this._knn(near, query, k, results);
    const worstDist = results.length >= k ? results[k - 1].dist : Infinity;
    if (diff * diff < worstDist) this._knn(far, query, k, results);
  }

  _dist2(a, b) {
    let sum = 0;
    for (let i = 0; i < this._k; i++) sum += (a[i] - b[i]) ** 2;
    return sum;
  }
}
