// join-ordering.js — Selinger-style cost-based join ordering
// Dynamic programming over subsets to find optimal join order.

export class JoinOrderer {
  constructor() {
    this._memo = new Map();
  }

  /**
   * Find optimal join order for a set of relations.
   * @param {Array<{name, rows, cost}>} relations
   * @param {Array<{left, right, selectivity}>} predicates
   * @returns {{order, totalCost}}
   */
  optimize(relations, predicates) {
    this._memo.clear();
    const n = relations.length;
    const relMap = new Map(relations.map((r, i) => [r.name, { ...r, idx: i }]));
    
    // Initialize single-relation costs
    for (const rel of relations) {
      const key = 1 << relMap.get(rel.name).idx;
      this._memo.set(key, {
        cost: rel.cost || rel.rows,
        rows: rel.rows,
        plan: { type: 'scan', table: rel.name, rows: rel.rows },
      });
    }

    // Build up from pairs to full set
    const fullSet = (1 << n) - 1;
    for (let size = 2; size <= n; size++) {
      for (let set = 0; set <= fullSet; set++) {
        if (this._popcount(set) !== size) continue;
        
        // Try all ways to split set into two non-empty subsets
        for (let sub = (set - 1) & set; sub > 0; sub = (sub - 1) & set) {
          const comp = set ^ sub;
          if (comp === 0 || sub > comp) continue; // Avoid duplicates
          
          const left = this._memo.get(sub);
          const right = this._memo.get(comp);
          if (!left || !right) continue;

          const sel = this._findSelectivity(sub, comp, relations, predicates, relMap);
          const joinRows = Math.max(1, Math.round(left.rows * right.rows * sel));
          const joinCost = left.cost + right.cost + left.rows + right.rows;
          
          const existing = this._memo.get(set);
          if (!existing || joinCost < existing.cost) {
            this._memo.set(set, {
              cost: joinCost,
              rows: joinRows,
              plan: {
                type: 'join',
                left: left.plan,
                right: right.plan,
                cost: joinCost,
                rows: joinRows,
              },
            });
          }
        }
      }
    }

    const result = this._memo.get(fullSet);
    return result ? { order: result.plan, totalCost: result.cost } : null;
  }

  _findSelectivity(leftSet, rightSet, relations, predicates, relMap) {
    for (const pred of predicates) {
      const li = relMap.get(pred.left)?.idx;
      const ri = relMap.get(pred.right)?.idx;
      if (li == null || ri == null) continue;
      if (((leftSet >> li) & 1) && ((rightSet >> ri) & 1)) return pred.selectivity;
      if (((leftSet >> ri) & 1) && ((rightSet >> li) & 1)) return pred.selectivity;
    }
    return 1; // Cross join (no predicate)
  }

  _popcount(n) { let c = 0; while (n) { c += n & 1; n >>= 1; } return c; }
}
