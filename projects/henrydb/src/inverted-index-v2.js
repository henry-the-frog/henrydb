// inverted-index-v2.js — Generalized inverted index for search
// Maps terms to sorted posting lists with position info.
export class InvertedIndex {
  constructor() { this._index = new Map(); this._docCount = 0; }

  addDocument(docId, text) {
    const terms = text.toLowerCase().split(/\W+/).filter(Boolean);
    for (let pos = 0; pos < terms.length; pos++) {
      const term = terms[pos];
      if (!this._index.has(term)) this._index.set(term, []);
      const postings = this._index.get(term);
      const last = postings[postings.length - 1];
      if (last && last.docId === docId) last.positions.push(pos);
      else postings.push({ docId, positions: [pos], tf: 0 });
    }
    // Update term frequencies
    for (const postings of this._index.values()) {
      for (const p of postings) p.tf = p.positions.length;
    }
    this._docCount++;
  }

  search(term) { return this._index.get(term.toLowerCase()) || []; }
  
  searchPhrase(phrase) {
    const terms = phrase.toLowerCase().split(/\W+/);
    if (terms.length === 0) return [];
    
    const firstDocs = this.search(terms[0]);
    return firstDocs.filter(d => {
      return d.positions.some(startPos => {
        return terms.every((t, i) => {
          const posting = this.search(t).find(p => p.docId === d.docId);
          return posting && posting.positions.includes(startPos + i);
        });
      });
    }).map(d => d.docId);
  }

  get termCount() { return this._index.size; }
  get docCount() { return this._docCount; }
}
