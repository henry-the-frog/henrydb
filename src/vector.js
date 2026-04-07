// vector.js — Vector similarity search for HenryDB
// Supports cosine similarity, Euclidean distance, and dot product.
// Inspired by pgvector.

/**
 * Vector Store for nearest-neighbor search.
 */
export class VectorStore {
  constructor(dimensions) {
    this._dimensions = dimensions;
    this._vectors = []; // [{ id, vector, metadata }]
  }

  /**
   * Add a vector.
   */
  add(id, vector, metadata = {}) {
    if (vector.length !== this._dimensions) {
      throw new Error(`Expected ${this._dimensions} dimensions, got ${vector.length}`);
    }
    this._vectors.push({ id, vector, metadata });
  }

  /**
   * Find K nearest neighbors using cosine similarity.
   */
  nearestCosine(queryVector, k = 5) {
    const scored = this._vectors.map(v => ({
      id: v.id,
      metadata: v.metadata,
      similarity: cosineSimilarity(queryVector, v.vector),
    }));
    return scored.sort((a, b) => b.similarity - a.similarity).slice(0, k);
  }

  /**
   * Find K nearest neighbors using Euclidean distance.
   */
  nearestEuclidean(queryVector, k = 5) {
    const scored = this._vectors.map(v => ({
      id: v.id,
      metadata: v.metadata,
      distance: euclideanDistance(queryVector, v.vector),
    }));
    return scored.sort((a, b) => a.distance - b.distance).slice(0, k);
  }

  /**
   * Find K nearest by dot product (for normalized vectors).
   */
  nearestDotProduct(queryVector, k = 5) {
    const scored = this._vectors.map(v => ({
      id: v.id,
      metadata: v.metadata,
      score: dotProduct(queryVector, v.vector),
    }));
    return scored.sort((a, b) => b.score - a.score).slice(0, k);
  }

  /**
   * Filter by metadata then search.
   */
  filteredSearch(queryVector, filter, k = 5) {
    const filtered = this._vectors.filter(v => filter(v.metadata));
    const scored = filtered.map(v => ({
      id: v.id,
      metadata: v.metadata,
      similarity: cosineSimilarity(queryVector, v.vector),
    }));
    return scored.sort((a, b) => b.similarity - a.similarity).slice(0, k);
  }

  get size() { return this._vectors.length; }
}

function dotProduct(a, b) {
  let sum = 0;
  for (let i = 0; i < a.length; i++) sum += a[i] * b[i];
  return sum;
}

function magnitude(v) {
  return Math.sqrt(dotProduct(v, v));
}

function cosineSimilarity(a, b) {
  const dot = dotProduct(a, b);
  const magA = magnitude(a);
  const magB = magnitude(b);
  if (magA === 0 || magB === 0) return 0;
  return dot / (magA * magB);
}

function euclideanDistance(a, b) {
  let sum = 0;
  for (let i = 0; i < a.length; i++) sum += (a[i] - b[i]) ** 2;
  return Math.sqrt(sum);
}

export { cosineSimilarity, euclideanDistance, dotProduct };
