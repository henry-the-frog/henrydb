// explain-pretty.js — Pretty-print EXPLAIN output as ASCII tree

/**
 * Format an EXPLAIN plan as an ASCII tree.
 * @param {Array} plan - Array of plan nodes from EXPLAIN
 * @returns {string} ASCII tree representation
 */
export function prettyExplain(plan) {
  if (!plan || plan.length === 0) return '(empty plan)';
  
  const lines = [];
  lines.push('Query Plan');
  lines.push('─'.repeat(50));
  
  for (let i = 0; i < plan.length; i++) {
    const node = plan[i];
    const isLast = i === plan.length - 1;
    const prefix = i === 0 ? '→ ' : isLast ? '└─ ' : '├─ ';
    const indent = i === 0 ? '' : '   ';
    
    let line = `${indent}${prefix}${node.operation || 'Unknown'}`;
    
    const details = [];
    if (node.table) details.push(`on ${node.table}`);
    if (node.index) details.push(`using ${node.index}`);
    if (node.estimated_rows !== undefined) details.push(`rows: ~${node.estimated_rows}`);
    if (node.actual_rows !== undefined) details.push(`actual: ${node.actual_rows}`);
    if (node.condition) details.push(`filter: ${node.condition}`);
    if (node.cost !== undefined) details.push(`cost: ${node.cost}`);
    if (node.time !== undefined) details.push(`time: ${node.time}ms`);
    
    if (details.length) line += ` (${details.join(', ')})`;
    lines.push(line);
  }
  
  lines.push('─'.repeat(50));
  return lines.join('\n');
}

/**
 * Format plan as markdown table.
 */
export function explainAsTable(plan) {
  if (!plan || plan.length === 0) return '| No plan |';
  
  const headers = ['#', 'Operation', 'Table', 'Index', 'Est. Rows', 'Details'];
  let md = `| ${headers.join(' | ')} |\n`;
  md += `| ${headers.map(() => '---').join(' | ')} |\n`;
  
  for (let i = 0; i < plan.length; i++) {
    const n = plan[i];
    const details = [];
    if (n.condition) details.push(n.condition);
    if (n.cost) details.push(`cost: ${n.cost}`);
    md += `| ${i + 1} | ${n.operation || '?'} | ${n.table || '-'} | ${n.index || '-'} | ${n.estimated_rows ?? '-'} | ${details.join('; ') || '-'} |\n`;
  }
  
  return md;
}

/**
 * Format plan as JSON (for programmatic use).
 */
export function explainAsJSON(plan) {
  return JSON.stringify(plan, null, 2);
}
