// plan-viz.js — Query plan visualization as DOT graph
// Converts a query plan tree to Graphviz DOT format for visualization.
// Each operator node shows its type, cost estimate, and row estimate.

/**
 * PlanVisualizer — convert query plans to DOT format.
 */
export class PlanVisualizer {
  constructor() {
    this._nextId = 0;
  }

  /**
   * Convert a plan tree to DOT graph string.
   * 
   * @param {Object} plan - Query plan node
   *   { type, table?, columns?, predicate?, cost?, rows?, children? }
   * @returns {string} DOT format string
   */
  toDot(plan) {
    this._nextId = 0;
    const lines = ['digraph QueryPlan {', '  rankdir=TB;', '  node [shape=record, fontname="Courier"];'];
    this._emitNode(plan, lines);
    lines.push('}');
    return lines.join('\n');
  }

  /**
   * Convert a plan tree to a text-based tree (for terminal display).
   */
  toText(plan, indent = 0) {
    const prefix = indent === 0 ? '' : '  '.repeat(indent - 1) + '└─ ';
    const lines = [];
    
    let label = plan.type;
    if (plan.table) label += ` (${plan.table})`;
    if (plan.predicate) label += ` [${plan.predicate}]`;
    if (plan.cost !== undefined) label += ` cost=${plan.cost}`;
    if (plan.rows !== undefined) label += ` rows=${plan.rows}`;
    if (plan.columns) label += ` cols=[${plan.columns.join(',')}]`;
    
    lines.push(prefix + label);
    
    if (plan.children) {
      for (const child of plan.children) {
        lines.push(...this.toText(child, indent + 1));
      }
    }
    
    return indent === 0 ? lines.join('\n') : lines;
  }

  /**
   * Convert a plan tree to JSON summary.
   */
  toJSON(plan) {
    const result = { type: plan.type };
    if (plan.table) result.table = plan.table;
    if (plan.predicate) result.predicate = plan.predicate;
    if (plan.cost !== undefined) result.cost = plan.cost;
    if (plan.rows !== undefined) result.rows = plan.rows;
    if (plan.columns) result.columns = plan.columns;
    if (plan.children) result.children = plan.children.map(c => this.toJSON(c));
    return result;
  }

  _emitNode(plan, lines) {
    const id = `n${this._nextId++}`;
    
    // Build label
    const parts = [plan.type];
    if (plan.table) parts.push(`table: ${plan.table}`);
    if (plan.predicate) parts.push(`where: ${plan.predicate}`);
    if (plan.cost !== undefined) parts.push(`cost: ${plan.cost}`);
    if (plan.rows !== undefined) parts.push(`rows: ${plan.rows}`);
    if (plan.columns) parts.push(`cols: ${plan.columns.join(', ')}`);
    if (plan.joinType) parts.push(`join: ${plan.joinType}`);
    if (plan.joinKey) parts.push(`on: ${plan.joinKey}`);
    if (plan.sortKey) parts.push(`sort: ${plan.sortKey}`);
    if (plan.groupKey) parts.push(`group: ${plan.groupKey}`);
    if (plan.limit !== undefined) parts.push(`limit: ${plan.limit}`);
    
    const label = parts.join('\\n');
    const color = this._nodeColor(plan.type);
    lines.push(`  ${id} [label="${label}", fillcolor="${color}", style=filled];`);

    if (plan.children) {
      for (const child of plan.children) {
        const childId = `n${this._nextId}`;
        this._emitNode(child, lines);
        lines.push(`  ${id} -> ${childId};`);
      }
    }

    return id;
  }

  _nodeColor(type) {
    const colors = {
      'SeqScan': '#FFE0B2',
      'IndexScan': '#C8E6C9',
      'Filter': '#BBDEFB',
      'HashJoin': '#F8BBD0',
      'MergeJoin': '#F8BBD0',
      'NestedLoop': '#F8BBD0',
      'Sort': '#E1BEE7',
      'Aggregate': '#FFE0B2',
      'HashAggregate': '#FFE0B2',
      'Projection': '#B2EBF2',
      'Limit': '#D7CCC8',
      'Materialize': '#CFD8DC',
    };
    return colors[type] || '#FFFFFF';
  }
}
