// inverted-index.js — Inverted index with TF-IDF scoring
// Maps terms → list of (docId, positions). Enables full-text search.
// TF-IDF: Term Frequency * Inverse Document Frequency.

export class InvertedIndex {
  constructor() {
    this._index = new Map(); // term → Map<docId, {count, positions}>
    this._docs = new Map(); // docId → { length, fieldLengthSum }
    this._docCount = 0;
    this._avgDocLength = 0;
  }

  /**
   * Index a document.
   */
  addDocument(docId, text) {
    const tokens = this._tokenize(text);
    this._docs.set(docId, { length: tokens.length });
    this._docCount++;

    for (let pos = 0; pos < tokens.length; pos++) {
      const term = tokens[pos];
      if (!this._index.has(term)) this._index.set(term, new Map());
      const postings = this._index.get(term);
      if (!postings.has(docId)) postings.set(docId, { count: 0, positions: [] });
      const entry = postings.get(docId);
      entry.count++;
      entry.positions.push(pos);
    }

    // Update avg doc length
    let totalLength = 0;
    for (const doc of this._docs.values()) totalLength += doc.length;
    this._avgDocLength = totalLength / this._docCount;
  }

  /**
   * Search for documents matching a query.
   * Returns results sorted by TF-IDF score.
   */
  search(query, limit = 10) {
    const queryTerms = this._tokenize(query);
    const scores = new Map(); // docId → score

    for (const term of queryTerms) {
      const postings = this._index.get(term);
      if (!postings) continue;

      const df = postings.size; // Document frequency
      const idf = Math.log((this._docCount - df + 0.5) / (df + 0.5) + 1); // BM25 IDF

      for (const [docId, entry] of postings) {
        const tf = entry.count;
        const docLength = this._docs.get(docId).length;
        
        // BM25 scoring
        const k1 = 1.2, b = 0.75;
        const tfNorm = (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * docLength / this._avgDocLength));
        const score = idf * tfNorm;

        scores.set(docId, (scores.get(docId) || 0) + score);
      }
    }

    return [...scores.entries()]
      .sort((a, b) => b[1] - a[1])
      .slice(0, limit)
      .map(([docId, score]) => ({ docId, score: Math.round(score * 1000) / 1000 }));
  }

  /**
   * Boolean AND search: documents containing ALL terms.
   */
  searchAnd(query) {
    const terms = this._tokenize(query);
    if (terms.length === 0) return [];

    let result = null;
    for (const term of terms) {
      const postings = this._index.get(term);
      if (!postings) return [];
      const docIds = new Set(postings.keys());
      result = result ? new Set([...result].filter(id => docIds.has(id))) : docIds;
    }

    return [...(result || [])];
  }

  /**
   * Phrase search: documents containing exact phrase.
   */
  searchPhrase(phrase) {
    const terms = this._tokenize(phrase);
    if (terms.length === 0) return [];

    // Get posting lists for all terms
    const postingLists = terms.map(t => this._index.get(t));
    if (postingLists.some(p => !p)) return [];

    // Find docs that have all terms
    const candidates = this.searchAnd(phrase);
    const results = [];

    for (const docId of candidates) {
      const positions = postingLists.map(p => p.get(docId)?.positions || []);
      // Check if terms appear consecutively
      for (const startPos of positions[0]) {
        let match = true;
        for (let i = 1; i < positions.length; i++) {
          if (!positions[i].includes(startPos + i)) { match = false; break; }
        }
        if (match) { results.push(docId); break; }
      }
    }

    return results;
  }

  /**
   * Get term frequency across all documents.
   */
  getTermInfo(term) {
    const t = term.toLowerCase();
    const postings = this._index.get(t);
    if (!postings) return null;
    return {
      term: t,
      documentFrequency: postings.size,
      totalOccurrences: [...postings.values()].reduce((s, e) => s + e.count, 0),
    };
  }

  _tokenize(text) {
    return text.toLowerCase().split(/\W+/).filter(t => t.length > 0);
  }

  get termCount() { return this._index.size; }
  get documentCount() { return this._docCount; }

  getStats() {
    return {
      terms: this._index.size,
      documents: this._docCount,
      avgDocLength: this._avgDocLength.toFixed(1),
    };
  }
}
