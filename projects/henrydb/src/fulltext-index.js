// fulltext-index.js — Full-text search with inverted index and TF-IDF ranking
//
// Implements a basic full-text search engine:
//   1. Tokenization: split text into words, lowercase, remove punctuation
//   2. Inverted Index: maps term → [{docId, positions, tf}]
//   3. TF-IDF Ranking: term frequency * inverse document frequency
//   4. Boolean queries: AND (all terms must match), OR (any term matches)
//
// Architecture:
//   - Each "document" is a row's text column(s)
//   - docId is the row's rid (pageId, slotIdx)
//   - The index is built on INSERT and updated on DELETE
//
// Stop words are excluded from indexing for performance.

const STOP_WORDS = new Set([
  'a', 'an', 'and', 'are', 'as', 'at', 'be', 'by', 'for', 'from',
  'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'or', 'she',
  'that', 'the', 'to', 'was', 'were', 'will', 'with', 'this', 'but',
  'not', 'they', 'their', 'there', 'have', 'had', 'do', 'does', 'did',
  'been', 'being', 'would', 'could', 'should', 'may', 'might', 'can',
  'shall', 'if', 'so', 'no', 'all', 'any', 'some', 'my', 'your', 'our',
  'we', 'you', 'i', 'me', 'him', 'her', 'us', 'them',
]);

/**
 * Tokenize text into terms.
 * - Lowercase
 * - Split on non-alphanumeric
 * - Remove stop words
 * - Minimum length 2
 */
export function tokenize(text) {
  if (!text || typeof text !== 'string') return [];
  return text
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length >= 2 && !STOP_WORDS.has(w));
}

/**
 * PostingEntry — One document's entry for a term.
 */
class PostingEntry {
  constructor(docId, tf, positions) {
    this.docId = docId;    // Row identifier (e.g., rid number)
    this.tf = tf;          // Term frequency in this document
    this.positions = positions; // Array of word positions
  }
}

/**
 * FullTextIndex — Inverted index with TF-IDF ranking.
 */
export class FullTextIndex {
  constructor(name) {
    this.name = name;
    this._index = new Map();       // term → PostingEntry[]
    this._docLengths = new Map();  // docId → document length (in terms)
    this._docCount = 0;
    this._totalTerms = 0;
  }

  /**
   * Index a document.
   * @param {number|string} docId - Document identifier
   * @param {string} text - Document text
   */
  addDocument(docId, text) {
    const terms = tokenize(text);
    this._docLengths.set(docId, terms.length);
    this._docCount++;
    this._totalTerms += terms.length;

    // Count term frequencies and positions
    const termData = new Map(); // term → { count, positions }
    for (let i = 0; i < terms.length; i++) {
      const term = terms[i];
      if (!termData.has(term)) {
        termData.set(term, { count: 0, positions: [] });
      }
      const data = termData.get(term);
      data.count++;
      data.positions.push(i);
    }

    // Add to inverted index
    for (const [term, data] of termData) {
      if (!this._index.has(term)) {
        this._index.set(term, []);
      }
      this._index.get(term).push(new PostingEntry(docId, data.count, data.positions));
    }
  }

  /**
   * Remove a document from the index.
   */
  removeDocument(docId) {
    for (const [term, postings] of this._index) {
      const idx = postings.findIndex(p => p.docId === docId);
      if (idx !== -1) {
        postings.splice(idx, 1);
        if (postings.length === 0) this._index.delete(term);
      }
    }
    const docLen = this._docLengths.get(docId) || 0;
    this._totalTerms -= docLen;
    this._docLengths.delete(docId);
    this._docCount--;
  }

  /**
   * Search for documents matching a query.
   * Returns results sorted by TF-IDF relevance score.
   * 
   * @param {string} query - Search query (space-separated terms)
   * @param {Object} options
   * @param {string} options.mode - 'AND' (all terms) or 'OR' (any term), default 'OR'
   * @param {number} options.limit - Max results (default: 100)
   * @returns {Array<{docId, score, matchedTerms}>}
   */
  search(query, options = {}) {
    const mode = (options.mode || 'OR').toUpperCase();
    const limit = options.limit || 100;
    
    const queryTerms = tokenize(query);
    if (queryTerms.length === 0) return [];

    // Calculate IDF for each query term
    const termIDFs = new Map();
    for (const term of queryTerms) {
      const df = this._index.has(term) ? this._index.get(term).length : 0;
      if (df > 0) {
        // IDF = log(N / df), using log10 like BM25
        termIDFs.set(term, Math.log10(this._docCount / df));
      }
    }

    // Score each document
    const scores = new Map(); // docId → {score, matchedTerms}
    
    for (const term of queryTerms) {
      if (!this._index.has(term)) continue;
      
      const idf = termIDFs.get(term) || 0;
      
      for (const posting of this._index.get(term)) {
        // TF = term frequency / document length (normalized)
        const docLen = this._docLengths.get(posting.docId) || 1;
        const tf = posting.tf / docLen;
        
        const tfidf = tf * idf;
        
        if (!scores.has(posting.docId)) {
          scores.set(posting.docId, { score: 0, matchedTerms: new Set(), tf: 0 });
        }
        const entry = scores.get(posting.docId);
        entry.score += tfidf;
        entry.matchedTerms.add(term);
      }
    }

    // Filter by mode
    let results = [...scores.entries()].map(([docId, data]) => ({
      docId,
      score: data.score,
      matchedTerms: [...data.matchedTerms],
    }));

    if (mode === 'AND') {
      // All query terms must match
      results = results.filter(r => r.matchedTerms.length === queryTerms.length);
    }

    // Sort by score descending
    results.sort((a, b) => b.score - a.score);
    
    return results.slice(0, limit);
  }

  /**
   * Get document frequency for a term.
   */
  getDF(term) {
    const normalized = term.toLowerCase();
    return this._index.has(normalized) ? this._index.get(normalized).length : 0;
  }

  /**
   * Get all unique terms in the index.
   */
  get terms() {
    return [...this._index.keys()];
  }

  /**
   * Get statistics.
   */
  getStats() {
    return {
      documents: this._docCount,
      uniqueTerms: this._index.size,
      totalTerms: this._totalTerms,
      avgDocLength: this._docCount > 0 ? this._totalTerms / this._docCount : 0,
    };
  }
}
