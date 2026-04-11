// query-plan.js — Tree-structured query plan nodes for HenryDB
//
// Models query execution as a tree of operator nodes, each with:
//   - Estimated cost and row count (from the optimizer)
//   - Actual cost and row count (from execution, for EXPLAIN ANALYZE)
//   - Child nodes forming the operator tree
//
// PostgreSQL-style output:
//   Hash Join  (cost=120.50..245.00 rows=1000) (actual rows=987 time=12.3ms)
//     ->  Seq Scan on orders  (cost=0.00..35.00 rows=2000) (actual rows=2000 time=3.1ms)
//           Filter: status = 'active'
//     ->  Hash
//           ->  Seq Scan on users  (cost=0.00..25.00 rows=500) (actual rows=500 time=1.2ms)

import { pushdownPredicates } from './pushdown.js';

/**
 * Base plan node. All operator nodes extend this.
 */
export class PlanNode {
  constructor(type, props = {}) {
    this.type = type;
    this.children = [];
    // Estimates (set during planning)
    this.estimatedRows = props.estimatedRows ?? null;
    this.estimatedCost = props.estimatedCost ?? null;
    this.startupCost = props.startupCost ?? 0;
    // Actuals (set during EXPLAIN ANALYZE execution)
    this.actualRows = null;
    this.actualTime = null;  // ms
    this.actualLoops = 1;
    // Extra properties
    this.properties = {};
  }

  addChild(node) {
    this.children.push(node);
    return this;
  }

  setActuals(rows, timeMs, loops = 1) {
    this.actualRows = rows;
    this.actualTime = timeMs;
    this.actualLoops = loops;
    return this;
  }

  setProp(key, value) {
    this.properties[key] = value;
    return this;
  }
}

// ===== Scan Nodes =====

export class SeqScanNode extends PlanNode {
  constructor(table, props = {}) {
    super('Seq Scan', props);
    this.table = table;
    this.engine = props.engine || 'heap';
    this.filter = props.filter || null;
    this.alias = props.alias || null;
  }
}

export class IndexScanNode extends PlanNode {
  constructor(table, indexName, props = {}) {
    super('Index Scan', props);
    this.table = table;
    this.indexName = indexName;
    this.indexCond = props.indexCond || null;
    this.engine = props.engine || 'heap';
    this.scanDirection = props.scanDirection || 'Forward';
  }
}

export class BTreePKLookupNode extends PlanNode {
  constructor(table, props = {}) {
    super('BTree PK Lookup', props);
    this.table = table;
  }
}

// ===== Join Nodes =====

export class HashJoinNode extends PlanNode {
  constructor(joinType, hashCond, props = {}) {
    super('Hash Join', props);
    this.joinType = joinType;   // INNER, LEFT, RIGHT, FULL
    this.hashCond = hashCond;   // e.g. "orders.user_id = users.id"
  }
}

export class NestedLoopNode extends PlanNode {
  constructor(joinType, props = {}) {
    super('Nested Loop', props);
    this.joinType = joinType;
  }
}

export class MergeJoinNode extends PlanNode {
  constructor(joinType, mergeCond, props = {}) {
    super('Merge Join', props);
    this.joinType = joinType;
    this.mergeCond = mergeCond;
  }
}

export class HashNode extends PlanNode {
  constructor(props = {}) {
    super('Hash', props);
    this.buckets = props.buckets || null;
    this.memoryUsage = props.memoryUsage || null;
  }
}

// ===== Aggregate / Group Nodes =====

export class AggregateNode extends PlanNode {
  constructor(strategy, props = {}) {
    super('Aggregate', props);
    this.strategy = strategy;  // 'Plain' | 'Sorted' | 'Hashed'
    this.groupKeys = props.groupKeys || [];
  }
}

export class WindowAggNode extends PlanNode {
  constructor(props = {}) {
    super('WindowAgg', props);
    this.functions = props.functions || [];
  }
}

// ===== Sort / Unique / Limit =====

export class SortNode extends PlanNode {
  constructor(sortKeys, props = {}) {
    super('Sort', props);
    this.sortKeys = sortKeys;  // [{column, direction}]
    this.sortMethod = props.sortMethod || null;  // 'quicksort' | 'top-N heapsort'
    this.memoryUsage = props.memoryUsage || null;
  }
}

export class UniqueNode extends PlanNode {
  constructor(props = {}) {
    super('Unique', props);
  }
}

export class LimitNode extends PlanNode {
  constructor(count, props = {}) {
    super('Limit', props);
    this.count = count;
  }
}

// ===== Filter =====

export class FilterNode extends PlanNode {
  constructor(condition, props = {}) {
    super('Filter', props);
    this.condition = condition;
  }
}

// ===== Subquery / CTE =====

export class CTEScanNode extends PlanNode {
  constructor(cteName, props = {}) {
    super('CTE Scan', props);
    this.cteName = cteName;
  }
}

export class SubqueryScanNode extends PlanNode {
  constructor(alias, props = {}) {
    super('Subquery Scan', props);
    this.alias = alias;
  }
}

// ===== Set Operations =====

export class AppendNode extends PlanNode {
  constructor(operation, props = {}) {
    super('Append', props);
    this.operation = operation; // UNION, INTERSECT, EXCEPT
  }
}

// ===== Plan Builder =====

/**
 * Build a plan tree from an AST and table metadata.
 */
export class PlanBuilder {
  constructor(database, options = {}) {
    this.db = database;
    // Hypothetical indexes: [{table, columns, name}]
    this.hypotheticalIndexes = options.hypotheticalIndexes || [];
  }

  /**
   * Build a plan tree for a SELECT statement.
   * @param {Object} ast - Parsed SELECT AST
   * @returns {PlanNode} Root of plan tree
   */
  buildPlan(ast) {
    if (ast.type !== 'SELECT') {
      return new PlanNode('Result', { estimatedRows: 1 });
    }

    // Try predicate pushdown for joins
    let workingAst = ast;
    let pushedCount = 0;
    if (ast.joins?.length && ast.where) {
      try {
        const { ast: pushedAst, pushed } = pushdownPredicates(ast);
        if (pushed > 0) {
          workingAst = pushedAst;
          pushedCount = pushed;
        }
      } catch (e) {
        // Pushdown failed — continue with original AST
      }
    }

    let node = this._buildScanNode(workingAst);
    node = this._addJoins(node, workingAst);
    node = this._addFilter(node, workingAst);
    node = this._addGroupBy(node, workingAst);
    node = this._addWindowFunctions(node, workingAst);
    node = this._addSort(node, workingAst);
    node = this._addDistinct(node, workingAst);
    node = this._addLimit(node, workingAst);

    if (pushedCount > 0) {
      node.setProp('predicatesPushed', pushedCount);
    }

    return node;
  }

  _buildScanNode(ast) {
    const tableName = ast.from?.table;
    if (!tableName) {
      return new PlanNode('Result', { estimatedRows: 1, estimatedCost: 0.01 });
    }

    const table = this.db.tables.get(tableName);
    if (!table) {
      return new SeqScanNode(tableName, { estimatedRows: 0, estimatedCost: 0 });
    }

    const rowCount = this._getRowCount(table);
    const pageCount = Math.max(1, Math.ceil(rowCount / 100)); // ~100 rows per page
    const engine = table.heap && table.heap.constructor.name === 'BTreeTable' ? 'btree' : 'heap';

    // Check if WHERE uses an indexed column
    if (ast.where && !ast.joins?.length) {
      const indexScan = this._checkIndexScan(table, ast.where, tableName);
      if (indexScan) {
        return indexScan;
      }
    }

    const cost = (pageCount * 1.0) + (rowCount * 0.01); // seq_page_cost + cpu_tuple_cost
    const scan = new SeqScanNode(tableName, {
      estimatedRows: rowCount,
      estimatedCost: cost,
      engine,
    });
    scan.alias = ast.from?.alias || null;

    // Show pushed-down filter from predicate pushdown
    if (ast.from?.filter) {
      scan.filter = this._conditionToString(ast.from.filter);
      const selectivity = this._estimateSelectivity(ast.from.filter);
      scan.estimatedRows = Math.max(1, Math.ceil(rowCount * selectivity));
    }

    return scan;
  }

  _checkIndexScan(table, where, tableName) {
    // Simple equality check on indexed column
    // Handle both AST formats
    const isEq = (where.type === 'binary' && where.operator === '=') ||
                 (where.type === 'COMPARE' && where.op === 'EQ');
    if (!isEq) return null;

    let colName = null;
    if (where.left?.type === 'column_ref') {
      colName = where.left.column || where.left.name;
    } else if (where.right?.type === 'column_ref') {
      colName = where.right.column || where.right.name;
    }
    // Strip table prefix if present
    if (colName && colName.includes('.')) colName = colName.split('.').pop();
    if (!colName) return null;

    // Check real indexes from indexCatalog
    if (this.db.indexCatalog) {
      for (const [idxName, meta] of this.db.indexCatalog) {
        if (meta.table !== tableName) continue;
        const idxCols = meta.columns || [meta.column];
        if (idxCols[0] === colName) {
          const selectivity = 1.0 / Math.max(1, this._getRowCount(table));
          const estRows = Math.max(1, Math.ceil(this._getRowCount(table) * selectivity));
          const cost = 4.0 + estRows * 0.01;
          return new IndexScanNode(tableName, idxName, {
            estimatedRows: estRows,
            estimatedCost: cost,
            indexCond: `${colName} = <value>`,
          });
        }
      }
    }

    // Check legacy _indexes
    const indexes = this.db._indexes?.get(tableName);
    if (indexes) {
      for (const [idxName, idx] of indexes) {
        if (idx.column === colName || idx.columns?.[0] === colName) {
          const selectivity = 1.0 / Math.max(1, this._getRowCount(table));
          const estRows = Math.max(1, Math.ceil(this._getRowCount(table) * selectivity));
          const cost = 4.0 + estRows * 0.01; // random_page_cost + cpu
          return new IndexScanNode(tableName, idxName, {
            estimatedRows: estRows,
            estimatedCost: cost,
            indexCond: `${colName} = <value>`,
            engine: table.heap?.constructor.name === 'BTreeTable' ? 'btree' : 'heap',
          });
        }
      }
    }

    // Check BTree PK
    if (table.heap && table.heap.constructor.name === 'BTreeTable') {
      const pkCol = table.columns?.[0]?.name || 'id';
      if (colName === pkCol) {
        const treeHeight = Math.max(2, Math.ceil(Math.log2(this._getRowCount(table) + 1) / Math.log2(100)));
        return new BTreePKLookupNode(tableName, {
          estimatedRows: 1,
          estimatedCost: treeHeight * 0.5,
        });
      }
    }

    // Check hypothetical indexes
    for (const hyp of this.hypotheticalIndexes) {
      if (hyp.table === tableName && hyp.columns[0] === colName) {
        const selectivity = 1.0 / Math.max(1, this._getRowCount(table));
        const estRows = Math.max(1, Math.ceil(this._getRowCount(table) * selectivity));
        const cost = 4.0 + estRows * 0.01;
        const node = new IndexScanNode(tableName, hyp.name || `hyp_${tableName}_${colName}`, {
          estimatedRows: estRows,
          estimatedCost: cost,
          indexCond: `${colName} = <value>`,
        });
        node.setProp('hypothetical', true);
        return node;
      }
    }

    return null;
  }

  _addJoins(scanNode, ast) {
    if (!ast.joins || ast.joins.length === 0) return scanNode;

    let current = scanNode;
    for (const join of ast.joins) {
      const joinTable = typeof join.table === 'string' ? join.table : (join.table?.table || join.table);
      const joinType = (join.type || 'INNER').toUpperCase().replace(/^JOIN$/, 'INNER');
      
      // Build scan for the join table
      const rightTable = this.db.tables.get(joinTable);
      const rightRows = rightTable ? this._getRowCount(rightTable) : 0;
      const rightPages = Math.max(1, Math.ceil(rightRows / 100));
      const rightEngine = rightTable?.heap?.constructor?.name === 'BTreeTable' ? 'btree' : 'heap';
      const rightScan = new SeqScanNode(joinTable, {
        estimatedRows: rightRows,
        estimatedCost: rightPages * 1.0 + rightRows * 0.01,
        engine: rightEngine,
      });
      rightScan.alias = join.alias || null;
      // Show pushed-down filter on right side
      if (join.filter) {
        rightScan.filter = this._conditionToString(join.filter);
        const sel = this._estimateSelectivity(join.filter);
        rightScan.estimatedRows = Math.max(1, Math.ceil(rightRows * sel));
      }

      // Detect equi-join for hash join
      const equiKey = join.on ? this._extractEquiJoinKey(join.on) : null;
      
      if (equiKey) {
        const hashCond = `${equiKey.left} = ${equiKey.right}`;
        const selectivity = 1.0 / Math.max(rightRows, 1);
        const joinRows = Math.max(1, Math.ceil(current.estimatedRows * rightRows * selectivity));
        const joinCost = (current.estimatedCost || 0) + (rightScan.estimatedCost || 0) + 
                        rightRows * 0.02 + joinRows * 0.01; // hash build + probe
        
        const hashNode = new HashNode({
          estimatedRows: rightRows,
          estimatedCost: rightRows * 0.02,
          buckets: Math.max(16, Math.ceil(rightRows / 4)),
        });
        hashNode.addChild(rightScan);
        
        const joinNode = new HashJoinNode(joinType, hashCond, {
          estimatedRows: joinRows,
          estimatedCost: joinCost,
        });
        joinNode.addChild(current);
        joinNode.addChild(hashNode);
        current = joinNode;
      } else {
        // Nested loop for non-equi joins
        const joinRows = Math.max(1, Math.ceil(current.estimatedRows * rightRows * 0.1));
        const joinCost = (current.estimatedCost || 0) + current.estimatedRows * (rightScan.estimatedCost || 0);
        
        const joinNode = new NestedLoopNode(joinType, {
          estimatedRows: joinRows,
          estimatedCost: joinCost,
        });
        joinNode.addChild(current);
        joinNode.addChild(rightScan);
        current = joinNode;
      }
    }
    return current;
  }

  _addFilter(node, ast) {
    // Filter is embedded in scan for simple cases
    // For joins or complex WHERE, add explicit filter node
    if (!ast.where) return node;
    if (node.type === 'Index Scan' || node.type === 'BTree PK Lookup') return node;
    if (node.type === 'Seq Scan' && !ast.joins?.length) {
      // Attach filter info to the scan node itself
      node.filter = this._conditionToString(ast.where);
      const selectivity = this._estimateSelectivity(ast.where);
      node.estimatedRows = Math.max(1, Math.ceil(node.estimatedRows * selectivity));
      return node;
    }
    // Complex case: add filter above joins
    if (ast.joins?.length && ast.where) {
      // Only add filter if WHERE has conditions beyond join conditions
      return node; // Join conditions already handled
    }
    return node;
  }

  _addGroupBy(node, ast) {
    if (!ast.groupBy && !ast.columns?.some(c => c.type === 'aggregate')) return node;
    
    const keys = ast.groupBy || [];
    const estGroups = keys.length > 0 ? 
      Math.min(node.estimatedRows, Math.ceil(Math.sqrt(node.estimatedRows))) : 1;
    
    const strategy = keys.length > 0 ? 'Hashed' : 'Plain';
    const aggNode = new AggregateNode(strategy, {
      estimatedRows: estGroups,
      estimatedCost: (node.estimatedCost || 0) + node.estimatedRows * 0.01,
      groupKeys: keys,
    });
    aggNode.addChild(node);

    // Add HAVING filter if present
    if (ast.having) {
      const filterNode = new FilterNode('HAVING condition', {
        estimatedRows: Math.max(1, Math.ceil(estGroups * 0.5)),
        estimatedCost: aggNode.estimatedCost + estGroups * 0.0025,
      });
      filterNode.addChild(aggNode);
      return filterNode;
    }

    return aggNode;
  }

  _addWindowFunctions(node, ast) {
    const windowCols = ast.columns?.filter(c => c.type === 'window') || [];
    if (windowCols.length === 0) return node;

    const winNode = new WindowAggNode({
      estimatedRows: node.estimatedRows,
      estimatedCost: (node.estimatedCost || 0) + node.estimatedRows * 0.05,
      functions: windowCols.map(c => c.func || c.name || 'unknown'),
    });
    winNode.addChild(node);
    return winNode;
  }

  _addSort(node, ast) {
    if (!ast.orderBy) return node;
    
    // Check if sort can be eliminated (BTree PK ordering)
    if (node.type === 'Seq Scan' && node.engine === 'btree') {
      const pk = this._getPKColumn(node.table);
      if (pk && ast.orderBy.length === 1 && ast.orderBy[0].column === pk) {
        node.setProp('sortEliminated', true);
        return node;
      }
    }

    const sortKeys = ast.orderBy.map(o => ({
      column: o.column || o.expr?.column || 'unknown',
      direction: (o.direction || 'ASC').toUpperCase(),
    }));
    
    const n = node.estimatedRows || 1;
    const sortCost = n * Math.log2(Math.max(2, n)) * 0.01; // O(n log n)
    const sortNode = new SortNode(sortKeys, {
      estimatedRows: node.estimatedRows,
      estimatedCost: (node.estimatedCost || 0) + sortCost,
    });
    sortNode.addChild(node);
    return sortNode;
  }

  _addDistinct(node, ast) {
    if (!ast.distinct) return node;
    
    const uniqueNode = new UniqueNode({
      estimatedRows: Math.ceil(node.estimatedRows * 0.8), // rough estimate
      estimatedCost: (node.estimatedCost || 0) + node.estimatedRows * 0.005,
    });
    uniqueNode.addChild(node);
    return uniqueNode;
  }

  _addLimit(node, ast) {
    if (ast.limit == null) return node;
    
    const limitCount = typeof ast.limit === 'number' ? ast.limit : ast.limit.value || ast.limit;
    const limitNode = new LimitNode(limitCount, {
      estimatedRows: Math.min(node.estimatedRows, limitCount),
      estimatedCost: (node.estimatedCost || 0) * (Math.min(node.estimatedRows, limitCount) / Math.max(1, node.estimatedRows)),
    });
    limitNode.addChild(node);
    return limitNode;
  }

  // ===== Helpers =====

  _getRowCount(table) {
    if (table.heap?._rowCount != null) return table.heap._rowCount;
    if (typeof table.heap?.count === 'function') return table.heap.count();
    if (table.heap?.rows) return table.heap.rows.length || table.heap.rows.size || 0;
    if (table.rows) return Array.isArray(table.rows) ? table.rows.length : table.rows.size || 0;
    return 0;
  }

  _extractEquiJoinKey(on) {
    // Handle both AST formats:
    // Format 1: { type: 'binary', operator: '=', left/right: { type: 'column_ref', table, column } }
    // Format 2: { type: 'COMPARE', op: 'EQ', left/right: { type: 'column_ref', name: 'table.col' } }
    const isBinaryEq = (on.type === 'binary' && on.operator === '=');
    const isCompareEq = (on.type === 'COMPARE' && on.op === 'EQ');
    if (isBinaryEq || isCompareEq) {
      const left = this._colRefToString(on.left);
      const right = this._colRefToString(on.right);
      if (left && right) return { left, right };
    }
    return null;
  }

  _colRefToString(node) {
    if (!node || node.type !== 'column_ref') return null;
    if (node.name) return node.name; // Format 2: 'table.column'
    if (node.table) return `${node.table}.${node.column}`;
    return node.column || null;
  }

  _conditionToString(where) {
    if (!where) return '';
    if (where.type === 'binary') {
      const left = where.left?.column || where.left?.name || where.left?.value || '?';
      const right = where.right?.column || where.right?.name || where.right?.value || '?';
      return `${left} ${where.operator} ${right}`;
    }
    if (where.type === 'COMPARE') {
      const ops = { EQ: '=', NE: '!=', LT: '<', GT: '>', LE: '<=', GE: '>=' };
      const left = where.left?.name || where.left?.column || where.left?.value || '?';
      const right = where.right?.name || where.right?.column || where.right?.value || '?';
      return `${left} ${ops[where.op] || where.op} ${right}`;
    }
    if (where.type === 'AND') return `(${this._conditionToString(where.left)} AND ${this._conditionToString(where.right)})`;
    if (where.type === 'OR') return `(${this._conditionToString(where.left)} OR ${this._conditionToString(where.right)})`;
    if (where.type === 'BETWEEN') return `${where.column?.name || '?'} BETWEEN ${where.low?.value || '?'} AND ${where.high?.value || '?'}`;
    if (where.type === 'IN') return `${where.column?.name || '?'} IN (...)`;
    if (where.type === 'IS_NULL') return `${where.column?.name || '?'} IS NULL`;
    if (where.type === 'LIKE') return `${where.column?.name || '?'} LIKE '${where.pattern || '?'}'`;
    return 'complex condition';
  }

  _estimateSelectivity(where) {
    if (!where) return 1.0;
    if (where.type === 'binary' || where.type === 'COMPARE') {
      const op = where.operator || where.op;
      switch (op) {
        case '=': case 'EQ': return 0.1;
        case '<': case '>': case '<=': case '>=': case 'LT': case 'GT': case 'LE': case 'GE': return 0.33;
        case '!=': case '<>': case 'NE': return 0.9;
        case 'LIKE': case 'ILIKE': return 0.25;
        default: return 0.5;
      }
    }
    if (where.type === 'AND') return this._estimateSelectivity(where.left) * this._estimateSelectivity(where.right);
    if (where.type === 'OR') {
      const sl = this._estimateSelectivity(where.left);
      const sr = this._estimateSelectivity(where.right);
      return sl + sr - sl * sr;
    }
    if (where.type === 'BETWEEN') return 0.25;
    if (where.type === 'IN') return 0.15;
    if (where.type === 'IS_NULL') return 0.05;
    if (where.type === 'LIKE') return 0.25;
    return 0.5;
  }

  _getPKColumn(tableName) {
    const table = this.db.tables.get(tableName);
    if (!table) return null;
    const pkCol = table.columns?.find(c => c.primaryKey);
    return pkCol?.name || null;
  }
}

// ===== Plan Formatter =====

/**
 * Format a plan tree as PostgreSQL-style text output.
 */
export class PlanFormatter {
  /**
   * Format plan tree as indented text.
   * @param {PlanNode} root
   * @param {Object} options - { analyze: boolean, verbose: boolean }
   * @returns {string[]} Lines of formatted output
   */
  static format(root, options = {}) {
    const lines = [];
    PlanFormatter._formatNode(root, lines, 0, true, options);
    return lines;
  }

  static _formatNode(node, lines, depth, isLast, options) {
    const indent = depth === 0 ? '' : '  '.repeat(depth - 1) + '->  ';
    
    // Build the main line
    let line = indent + PlanFormatter._nodeLabel(node);
    
    // Cost/rows estimate
    const costs = [];
    if (node.estimatedCost != null) {
      const startup = node.startupCost || 0;
      costs.push(`cost=${startup.toFixed(2)}..${node.estimatedCost.toFixed(2)}`);
    }
    if (node.estimatedRows != null) {
      costs.push(`rows=${node.estimatedRows}`);
    }
    if (costs.length > 0) {
      line += `  (${costs.join(' ')})`;
    }

    // Actuals (EXPLAIN ANALYZE only)
    if (options.analyze && node.actualRows != null) {
      const actualParts = [`actual rows=${node.actualRows}`];
      if (node.actualTime != null) {
        actualParts.push(`time=${node.actualTime.toFixed(3)}ms`);
      }
      if (node.actualLoops > 1) {
        actualParts.push(`loops=${node.actualLoops}`);
      }
      line += `  (${actualParts.join(' ')})`;
    }

    lines.push(line);

    // Extra info lines (indented below the node)
    const extraIndent = '  '.repeat(depth) + (depth > 0 ? '      ' : '  ');
    
    if (node instanceof SeqScanNode && node.filter) {
      lines.push(`${extraIndent}Filter: ${node.filter}`);
    }
    if (node instanceof IndexScanNode && node.indexCond) {
      lines.push(`${extraIndent}Index Cond: ${node.indexCond}`);
    }
    if (node instanceof HashJoinNode && node.hashCond) {
      lines.push(`${extraIndent}Hash Cond: ${node.hashCond}`);
    }
    if (node instanceof SortNode && node.sortKeys?.length) {
      const keys = node.sortKeys.map(k => `${k.column} ${k.direction}`).join(', ');
      lines.push(`${extraIndent}Sort Key: ${keys}`);
      if (options.analyze && node.sortMethod) {
        lines.push(`${extraIndent}Sort Method: ${node.sortMethod}  Memory: ${node.memoryUsage || '?'}kB`);
      }
    }
    if (node instanceof AggregateNode && node.groupKeys?.length) {
      lines.push(`${extraIndent}Group Key: ${node.groupKeys.join(', ')}`);
    }
    if (node instanceof HashNode && options.analyze && node.buckets) {
      lines.push(`${extraIndent}Buckets: ${node.buckets}  Memory Usage: ${node.memoryUsage || '?'}kB`);
    }

    // Recurse children
    for (let i = 0; i < node.children.length; i++) {
      const isChildLast = i === node.children.length - 1;
      PlanFormatter._formatNode(node.children[i], lines, depth + 1, isChildLast, options);
    }
  }

  static _nodeLabel(node) {
    switch (node.type) {
      case 'Seq Scan': {
        const n = node;
        const target = n.alias ? `${n.table} ${n.alias}` : n.table;
        return `Seq Scan on ${target}`;
      }
      case 'Index Scan': {
        const n = node;
        const target = n.table;
        return `Index Scan using ${n.indexName} on ${target}`;
      }
      case 'BTree PK Lookup':
        return `Index Only Scan on ${node.table}`;
      case 'Hash Join':
        return node.joinType === 'INNER' ? 'Hash Join' : `Hash ${node.joinType} Join`;
      case 'Nested Loop':
        return node.joinType === 'INNER' ? 'Nested Loop' : `Nested Loop ${node.joinType}`;
      case 'Merge Join':
        return node.joinType === 'INNER' ? 'Merge Join' : `Merge ${node.joinType} Join`;
      case 'Hash':
        return 'Hash';
      case 'Aggregate':
        return node.strategy ? `${node.strategy}Aggregate` : 'Aggregate';
      case 'WindowAgg':
        return 'WindowAgg';
      case 'Sort':
        return 'Sort';
      case 'Unique':
        return 'Unique';
      case 'Limit':
        return `Limit`;
      case 'Filter':
        return `Filter: ${node.condition}`;
      case 'CTE Scan':
        return `CTE Scan on ${node.cteName}`;
      case 'Subquery Scan':
        return `Subquery Scan on ${node.alias}`;
      case 'Append':
        return `Append (${node.operation})`;
      default:
        return node.type;
    }
  }

  /**
   * Format as JSON (for EXPLAIN FORMAT JSON).
   */
  static toJSON(root) {
    return PlanFormatter._nodeToJSON(root);
  }

  static _nodeToJSON(node) {
    const result = {
      'Node Type': node.type,
    };
    
    if (node.table) result['Relation Name'] = node.table;
    if (node.alias) result['Alias'] = node.alias;
    if (node.indexName) result['Index Name'] = node.indexName;
    if (node.joinType) result['Join Type'] = node.joinType;
    if (node.hashCond) result['Hash Cond'] = node.hashCond;
    if (node.filter) result['Filter'] = node.filter;
    if (node.indexCond) result['Index Cond'] = node.indexCond;
    if (node.sortKeys) result['Sort Key'] = node.sortKeys.map(k => `${k.column} ${k.direction}`);
    if (node.groupKeys?.length) result['Group Key'] = node.groupKeys;
    if (node.estimatedCost != null) {
      result['Startup Cost'] = node.startupCost || 0;
      result['Total Cost'] = node.estimatedCost;
    }
    if (node.estimatedRows != null) result['Plan Rows'] = node.estimatedRows;
    if (node.actualRows != null) result['Actual Rows'] = node.actualRows;
    if (node.actualTime != null) result['Actual Total Time'] = node.actualTime;
    if (node.actualLoops > 1) result['Actual Loops'] = node.actualLoops;
    
    if (node.children.length > 0) {
      result['Plans'] = node.children.map(c => PlanFormatter._nodeToJSON(c));
    }
    
    return result;
  }
}
