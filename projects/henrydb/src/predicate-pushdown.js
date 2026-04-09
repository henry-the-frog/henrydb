// predicate-pushdown.js — Push filter predicates below joins
export function pushdownPredicate(plan) {
  if (plan.type === 'filter' && plan.child.type === 'join') {
    const join = plan.child;
    const leftCols = new Set(join.left.columns || []);
    const rightCols = new Set(join.right.columns || []);
    
    const leftPreds = [];
    const rightPreds = [];
    const remainPreds = [];
    
    for (const pred of plan.predicates || []) {
      if (pred.columns.every(c => leftCols.has(c))) leftPreds.push(pred);
      else if (pred.columns.every(c => rightCols.has(c))) rightPreds.push(pred);
      else remainPreds.push(pred);
    }
    
    let newJoin = { ...join };
    if (leftPreds.length) newJoin.left = { type: 'filter', predicates: leftPreds, child: join.left };
    if (rightPreds.length) newJoin.right = { type: 'filter', predicates: rightPreds, child: join.right };
    
    if (remainPreds.length) return { type: 'filter', predicates: remainPreds, child: newJoin };
    return newJoin;
  }
  return plan;
}
