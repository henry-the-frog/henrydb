// optimizer.js — Query optimizer: predicate pushdown, projection pushdown, sort-based group by

/**
 * Predicate Pushdown — push WHERE conditions below JOINs.
 * Given a join plan with a predicate, identify which predicates
 * can be pushed to individual table scans.
 */
export class PredicatePushdown {
  /**
   * Push predicates through a join plan.
   * @param {Object} plan - { type: 'join', left, right, predicate }
   * @param {Array} tableColumns - [{table, columns}]
   * @returns {Object} Modified plan with predicates pushed down
   */
  optimize(plan) {
    if (plan.type === 'join' && plan.predicate) {
      const preds = this._flattenAnd(plan.predicate);
      const leftPreds = [], rightPreds = [], joinPreds = [];
      
      for (const pred of preds) {
        const cols = this._getColumns(pred);
        const leftCols = plan.left.columns || [];
        const rightCols = plan.right.columns || [];
        
        const onLeft = cols.every(c => leftCols.includes(c));
        const onRight = cols.every(c => rightCols.includes(c));
        
        if (onLeft) leftPreds.push(pred);
        else if (onRight) rightPreds.push(pred);
        else joinPreds.push(pred);
      }

      return {
        ...plan,
        left: leftPreds.length > 0
          ? { ...plan.left, filter: this._buildAnd(leftPreds) }
          : plan.left,
        right: rightPreds.length > 0
          ? { ...plan.right, filter: this._buildAnd(rightPreds) }
          : plan.right,
        predicate: joinPreds.length > 0 ? this._buildAnd(joinPreds) : null,
      };
    }
    return plan;
  }

  _flattenAnd(expr) {
    if (!expr) return [];
    if (expr.type === 'AND') return [...this._flattenAnd(expr.left), ...this._flattenAnd(expr.right)];
    return [expr];
  }
  _buildAnd(preds) { return preds.length === 1 ? preds[0] : preds.reduce((a, b) => ({ type: 'AND', left: a, right: b })); }
  _getColumns(expr) {
    const cols = [];
    const walk = (e) => { if (!e) return; if (e.type === 'column') cols.push(e.name); for (const k of ['left', 'right', 'expr']) if (e[k]) walk(e[k]); };
    walk(expr);
    return cols;
  }
}

/**
 * Projection Pushdown — eliminate unused columns.
 */
export class ProjectionPushdown {
  optimize(plan, requiredColumns) {
    if (!plan) return plan;
    
    // Add columns needed by predicate
    if (plan.filter) requiredColumns = new Set([...requiredColumns, ...this._getColumns(plan.filter)]);
    
    return {
      ...plan,
      projectedColumns: [...requiredColumns],
      children: plan.children?.map(c => this.optimize(c, requiredColumns)),
    };
  }

  _getColumns(expr) {
    const cols = new Set();
    const walk = (e) => { if (!e) return; if (e.type === 'column') cols.add(e.name); for (const k of ['left', 'right', 'expr']) if (e[k]) walk(e[k]); };
    walk(expr);
    return cols;
  }
}

/**
 * Sort-based Group By — efficient when data is pre-sorted on group key.
 */
export class SortGroupBy {
  constructor(groupCols, aggregates) {
    this.groupCols = groupCols;
    this.aggregates = aggregates;
  }

  /** Process pre-sorted rows. */
  process(sortedRows) {
    if (sortedRows.length === 0) return [];
    
    const results = [];
    let currentKey = this._key(sortedRows[0]);
    let accumulators = this._initAccs();
    
    for (const row of sortedRows) {
      const key = this._key(row);
      if (key !== currentKey) {
        results.push(this._finalize(currentKey, accumulators, sortedRows[0]));
        currentKey = key;
        accumulators = this._initAccs();
      }
      this._accumulate(accumulators, row);
    }
    results.push(this._finalize(currentKey, accumulators, sortedRows[sortedRows.length - 1]));
    
    return results;
  }

  _key(row) { return this.groupCols.map(c => row[c]).join('|'); }
  
  _initAccs() { return this.aggregates.map(a => ({ sum: 0, count: 0, min: Infinity, max: -Infinity })); }
  
  _accumulate(accs, row) {
    for (let i = 0; i < this.aggregates.length; i++) {
      const v = row[this.aggregates[i].col];
      if (v == null) continue;
      accs[i].sum += v;
      accs[i].count++;
      if (v < accs[i].min) accs[i].min = v;
      if (v > accs[i].max) accs[i].max = v;
    }
  }

  _finalize(key, accs, sampleRow) {
    const result = {};
    const keyParts = key.split('|');
    for (let i = 0; i < this.groupCols.length; i++) result[this.groupCols[i]] = sampleRow[this.groupCols[i]];
    for (let i = 0; i < this.aggregates.length; i++) {
      const a = this.aggregates[i];
      switch (a.func) {
        case 'SUM': result[a.alias] = accs[i].sum; break;
        case 'COUNT': result[a.alias] = accs[i].count; break;
        case 'AVG': result[a.alias] = accs[i].count > 0 ? accs[i].sum / accs[i].count : null; break;
        case 'MIN': result[a.alias] = accs[i].min; break;
        case 'MAX': result[a.alias] = accs[i].max; break;
      }
    }
    return result;
  }
}
