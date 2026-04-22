// plan-format.js — Plan formatting extracted from db.js
// Functions take 'db' as first parameter (database context)

import { explainPlan as volcanoExplainPlan } from './volcano-planner.js';
import { planToHTML } from './plan-html.js';

export function formatPlan(db, plan, format, stmt) {
  switch (format) {
    case 'json': {
      const json = JSON.stringify(plan, null, 2);
      return { type: 'PLAN', rows: [{ 'QUERY PLAN': json }] };
    }
    case 'yaml': {
      const yaml = planToYaml(db, plan);
      return { type: 'PLAN', rows: [{ 'QUERY PLAN': yaml }] };
    }
    case 'dot': {
      const dot = planToDot(db, plan);
      return { type: 'PLAN', rows: [{ 'QUERY PLAN': dot }] };
    }
    case 'text':
    default: {
      // Format like PostgreSQL's EXPLAIN output with cost estimates
      const lines = [];
      let indent = 0;
      let runningCost = 0.00;
      const SEQ_PAGE_COST = 1.0;
      const CPU_TUPLE_COST = 0.01;
      const CPU_OPERATOR_COST = 0.0025;
      const SORT_COST_FACTOR = 2.0; // n log n factor

      for (const step of plan) {
        const prefix = indent > 0 ? '  '.repeat(indent) + '->  ' : '';
        switch (step.operation) {
          case 'TABLE_SCAN': {
            const rows = step.estimated_rows || 0;
            const startCost = runningCost;
            runningCost += rows * (SEQ_PAGE_COST + CPU_TUPLE_COST);
            const filteredRows = step.filtered_estimate || rows;
            const eng = step.engine ? ` engine=${step.engine}` : '';
            lines.push(`${prefix}Seq Scan on ${step.table}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${filteredRows} width=32${eng})`);
            if (step.estimation_method) {
              lines.push(`  ${'  '.repeat(indent)}  Estimation: ${step.estimation_method}`);
            }
            indent++;
            break;
          }
          case 'INDEX_SCAN': {
            const rows = step.estimated_rows || 1;
            const startCost = runningCost + 0.5; // index startup cost
            runningCost = startCost + rows * CPU_TUPLE_COST;
            lines.push(`${prefix}Index Scan using ${step.index || 'idx'} on ${step.table}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${rows} width=32)`);
            if (step.estimation_method) {
              lines.push(`  ${'  '.repeat(indent)}  Estimation: ${step.estimation_method}`);
            }
            indent++;
            break;
          }
          case 'BTREE_PK_LOOKUP': {
            const rows = step.estimated_rows || 1;
            const startCost = runningCost + 0.25;
            runningCost = startCost + rows * CPU_TUPLE_COST;
            lines.push(`${prefix}BTree PK Lookup on ${step.table}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${rows} width=32 engine=btree)`);
            indent++;
            break;
          }
          case 'HASH_JOIN': {
            const rightRows = step.estimated_right_rows || 100;
            const startCost = runningCost;
            const hashBuildCost = rightRows * CPU_TUPLE_COST;
            runningCost += hashBuildCost + rightRows * CPU_OPERATOR_COST;
            const selfTag = step.selfJoin ? ' [Self-Join]' : '';
            lines.push(`${prefix}Hash ${step.type} Join${selfTag}  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${rightRows})`);
            lines.push(`  ${'  '.repeat(indent)}  Hash Cond: (${step.on})`);
            indent++;
            break;
          }
          case 'NESTED_LOOP_JOIN': {
            const startCost = runningCost;
            runningCost += (step.estimated_right_rows || 100) * CPU_TUPLE_COST * 10;
            lines.push(`${prefix}Nested Loop ${step.type} Join  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)})`);
            lines.push(`  ${'  '.repeat(indent)}  Join Filter: ${step.on}`);
            indent++;
            break;
          }
          case 'JOIN_REORDER':
            lines.push(`${prefix}Join Reorder: ${step.original} → ${step.optimized}  (${step.reason})`);
            break;
          case 'FILTER': {
            const filterCost = CPU_OPERATOR_COST;
            runningCost += filterCost;
            lines.push(`${prefix}Filter: ${step.condition}`);
            break;
          }
          case 'HASH_GROUP_BY': {
            const startCost = runningCost;
            const groups = step.estimated_groups || 10;
            runningCost += groups * CPU_TUPLE_COST;
            lines.push(`${prefix}HashAggregate  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=${groups})`);
            lines.push(`  ${'  '.repeat(indent)}  Group Key: ${step.columns.join(', ')}`);
            break;
          }
          case 'AGGREGATE': {
            const startCost = runningCost;
            runningCost += CPU_TUPLE_COST;
            lines.push(`${prefix}Aggregate  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)} rows=1)`);
            break;
          }
          case 'SORT': {
            const startCost = runningCost;
            const prevRows = plan.find(p => p.estimated_rows)?.estimated_rows || 100;
            const sortWork = prevRows > 1 ? prevRows * Math.log2(prevRows) * CPU_OPERATOR_COST * SORT_COST_FACTOR : 0;
            runningCost += sortWork;
            lines.push(`${prefix}Sort  (keys: ${step.columns.join(', ')}; cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)})`);
            break;
          }
          case 'SORT_ELIMINATED':
            lines.push(`${prefix}Sort Eliminated  (keys: ${step.columns.join(', ')}, reason: ${step.reason})`);
            break;
          case 'LIMIT': {
            const startCost = runningCost;
            lines.push(`${prefix}Limit  (cost=${startCost.toFixed(2)}..${startCost.toFixed(2)} rows=${step.count})`);
            break;
          }
          case 'DISTINCT':
            lines.push(`${prefix}Unique  (cost=${runningCost.toFixed(2)}..${runningCost.toFixed(2)})`);
            break;
          case 'WINDOW_FUNCTION': {
            const startCost = runningCost;
            runningCost += CPU_TUPLE_COST * 2;
            lines.push(`${prefix}WindowAgg  (cost=${startCost.toFixed(2)}..${runningCost.toFixed(2)})`);
            break;
          }
          case 'CTE':
            lines.push(`${prefix}CTE Scan on ${step.name}${step.recursive ? ' (recursive)' : ''}`);
            indent++;
            break;
          case 'VIEW_SCAN':
            lines.push(`${prefix}Subquery Scan on ${step.view}`);
            indent++;
            break;
          default:
            lines.push(`${prefix}${step.operation}  ${JSON.stringify(step)}`);
        }
      }
      // Add Volcano plan tree if available
      try {
        const volcanoTree = volcanoExplainPlan(stmt, db.tables, db._indexes, db._tableStats);
        if (volcanoTree) {
          lines.push('');
          lines.push('Volcano Plan:');
          for (const line of volcanoTree.split('\n')) {
            lines.push('  ' + line);
          }
        }
      } catch (e) {
        // Volcano planner couldn't handle this query — skip
      }
      return { type: 'PLAN', plan, rows: lines.map(l => ({ 'QUERY PLAN': l })) };
    }
  }
}

export function planToYaml(db, plan, indent = 0) {
  const lines = [];
  const prefix = '  '.repeat(indent);
  if (Array.isArray(plan)) {
    for (const item of plan) {
      if (typeof item === 'object' && item !== null) {
        lines.push(`${prefix}-`);
        for (const [key, value] of Object.entries(item)) {
          if (Array.isArray(value)) {
            lines.push(`${prefix}  ${key}:`);
            for (const v of value) {
              lines.push(`${prefix}    - ${v}`);
            }
          } else {
            lines.push(`${prefix}  ${key}: ${value}`);
          }
        }
      } else {
        lines.push(`${prefix}- ${item}`);
      }
    }
  } else if (typeof plan === 'object' && plan !== null) {
    for (const [key, value] of Object.entries(plan)) {
      if (typeof value === 'object' && value !== null) {
        lines.push(`${prefix}${key}:`);
        lines.push(planToYaml(db, value, indent + 1));
      } else {
        lines.push(`${prefix}${key}: ${value}`);
      }
    }
  }
  return lines.join('\n');
}

export function planToDot(db, plan) {
  const lines = ['digraph QueryPlan {', '  rankdir=TB;', '  node [shape=record, fontname="Courier"];'];
  let nextId = 0;
  const nodes = Array.isArray(plan) ? plan : [plan];
  let prevId = null;
  for (const node of nodes) {
    const id = `n${nextId++}`;
    const op = node.operation || node.type || 'unknown';
    const details = Object.entries(node)
      .filter(([k]) => k !== 'operation' && k !== 'type')
      .map(([k, v]) => `${k}: ${Array.isArray(v) ? v.join(', ') : v}`)
      .join('\\n');
    const label = details ? `${op}|${details}` : op;
    lines.push(`  ${id} [label="{${label}}"];`);
    if (prevId !== null) {
      lines.push(`  ${prevId} -> ${id};`);
    }
    prevId = id;
  }
  lines.push('}');
  return lines.join('\n');
}

// Execute a recursive CTE: base UNION ALL recursive
