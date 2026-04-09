// cost-optimizer.js — Cost-based query optimization for HenryDB
//
// Estimates the cost of different access paths and chooses the cheapest:
//   - Full table scan: reads all pages
//   - Index scan (B+tree): traverses tree + random heap accesses
//   - Index scan (Hash): O(1) lookup + heap access
//   - BTreeTable PK lookup: O(log n) direct access
//
// Cost model (inspired by PostgreSQL):
//   - seq_page_cost = 1.0 (cost of reading a sequential page)
//   - random_page_cost = 4.0 (cost of a random page read — 4x sequential)
//   - cpu_tuple_cost = 0.01 (cost of processing one row)
//   - cpu_index_tuple_cost = 0.005 (cost of processing one index entry)
//   - cpu_operator_cost = 0.0025 (cost of evaluating one operator)

import { BTreeTable } from './btree-table.js';
import { estimateSelectivity } from './table-stats.js';

const SEQ_PAGE_COST = 1.0;
const RANDOM_PAGE_COST = 4.0;
const CPU_TUPLE_COST = 0.01;
const CPU_INDEX_TUPLE_COST = 0.005;
const CPU_OPERATOR_COST = 0.0025;

/**
 * Estimate the cost of a full sequential scan.
 */
function seqScanCost(rowCount, pageCount) {
  return (pageCount * SEQ_PAGE_COST) + (rowCount * CPU_TUPLE_COST);
}

/**
 * Estimate the cost of a full scan with a filter.
 */
function filterScanCost(rowCount, pageCount, filterOperators = 1) {
  return seqScanCost(rowCount, pageCount) + (rowCount * filterOperators * CPU_OPERATOR_COST);
}

/**
 * Estimate the cost of a B+tree index scan.
 * @param {number} rowCount - Total rows in table
 * @param {number} pageCount - Total pages in table
 * @param {number} selectivity - Fraction of rows matching (0-1)
 * @param {number} treeHeight - Height of B+tree (default: log(rowCount))
 */
function btreeIndexScanCost(rowCount, pageCount, selectivity, treeHeight) {
  const matchingRows = Math.max(1, Math.ceil(rowCount * selectivity));
  const indexPages = treeHeight || Math.max(1, Math.ceil(Math.log2(rowCount) / Math.log2(100)));
  
  // Cost = traverse index + random heap accesses for matching rows
  const indexTraversalCost = indexPages * RANDOM_PAGE_COST;
  const heapAccessCost = matchingRows * RANDOM_PAGE_COST; // Each row requires random page read
  const cpuCost = matchingRows * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST);
  
  // For high selectivity, index scan is worse because of random I/O
  // The heap access cost is reduced if matching rows are on the same page
  const pageFraction = Math.min(1, matchingRows / (pageCount || 1));
  const effectiveHeapPages = Math.ceil(pageCount * pageFraction);
  
  return indexTraversalCost + (effectiveHeapPages * RANDOM_PAGE_COST) + cpuCost;
}

/**
 * Estimate the cost of a hash index scan.
 */
function hashIndexScanCost(matchingRows) {
  // Hash: O(1) index access + random heap accesses
  const indexCost = RANDOM_PAGE_COST; // Single hash lookup
  const heapCost = matchingRows * RANDOM_PAGE_COST;
  const cpuCost = matchingRows * (CPU_INDEX_TUPLE_COST + CPU_TUPLE_COST);
  
  return indexCost + heapCost + cpuCost;
}

/**
 * Estimate the cost of a BTreeTable direct PK lookup.
 */
function btreePKLookupCost(treeHeight) {
  // Clustered B-tree: top levels are likely in cache, so cheaper than random I/O
  // Use sequential page cost for cached upper levels
  const cachedLevels = Math.max(0, (treeHeight || 3) - 1);
  const leafLevel = 1;
  return (cachedLevels * SEQ_PAGE_COST) + (leafLevel * RANDOM_PAGE_COST) + CPU_TUPLE_COST;
}

/**
 * AccessPath — Represents one way to access data.
 */
class AccessPath {
  constructor(type, cost, details = {}) {
    this.type = type;         // 'seq_scan' | 'index_scan' | 'hash_scan' | 'btree_pk_lookup'
    this.cost = cost;
    this.details = details;   // Additional info (index name, selectivity, etc.)
  }
}

/**
 * CostOptimizer — Chooses the cheapest access path for a query.
 */
export class CostOptimizer {
  /**
   * Find the cheapest access path for a single-table scan with a WHERE clause.
   * @param {Object} tableInfo - {schema, heap, indexes, indexMeta}
   * @param {Object} where - Parsed WHERE AST
   * @param {Object} tableStats - Stats from analyzeTable (optional)
   * @returns {AccessPath}
   */
  choosePath(tableInfo, where, tableStats) {
    const { schema, heap, indexes } = tableInfo;
    const rowCount = heap.rowCount || 0;
    const pageCount = heap.pageCount || 1;
    
    const paths = [];
    
    // Path 1: Full sequential scan (always available)
    if (where) {
      paths.push(new AccessPath('seq_scan', filterScanCost(rowCount, pageCount), {
        estimatedRows: rowCount,
        description: 'Full scan + filter',
      }));
    } else {
      paths.push(new AccessPath('seq_scan', seqScanCost(rowCount, pageCount), {
        estimatedRows: rowCount,
        description: 'Full scan (no filter)',
      }));
    }
    
    // Path 2+: Index scans (if WHERE has equality/range on indexed column)
    if (where && where.type === 'COMPARE' && where.op === 'EQ') {
      const colName = this._extractColumnName(where);
      const literalValue = this._extractLiteralValue(where);
      
      if (colName && literalValue !== undefined) {
        // Check for BTreeTable PK lookup
        if (heap instanceof BTreeTable) {
          const pkColNames = heap.pkIndices.map(i => schema[i]?.name);
          if (pkColNames.includes(colName)) {
            const height = Math.ceil(Math.log2(rowCount + 1) / Math.log2(heap.order));
            paths.push(new AccessPath('btree_pk_lookup', btreePKLookupCost(height), {
              estimatedRows: 1,
              description: 'BTreeTable direct PK lookup',
            }));
          }
        }
        
        // Check indexed columns
        if (indexes) {
          for (const [indexColName, index] of indexes) {
            if (indexColName === colName) {
              // Estimate selectivity
              let selectivity = 1 / Math.max(1, rowCount); // Default: 1 row
              if (tableStats) {
                const colStats = tableStats.columnStats?.get(colName);
                if (colStats) {
                  selectivity = estimateSelectivity(colStats, 'EQ', literalValue);
                }
              }
              
              const matchingRows = Math.max(1, Math.ceil(rowCount * selectivity));
              
              if (index._isHash) {
                paths.push(new AccessPath('hash_scan', hashIndexScanCost(matchingRows), {
                  index: indexColName,
                  selectivity,
                  estimatedRows: matchingRows,
                  description: 'Hash index equality scan',
                }));
              } else {
                paths.push(new AccessPath('index_scan', btreeIndexScanCost(rowCount, pageCount, selectivity), {
                  index: indexColName,
                  selectivity,
                  estimatedRows: matchingRows,
                  description: 'B+tree index scan',
                }));
              }
            }
          }
        }
      }
    }
    
    // Choose cheapest
    paths.sort((a, b) => a.cost - b.cost);
    return paths[0];
  }

  /**
   * Get all access paths with costs (for EXPLAIN).
   */
  allPaths(tableInfo, where, tableStats) {
    // Same logic as choosePath but return all
    const best = this.choosePath(tableInfo, where, tableStats);
    // For now return just the best (full implementation would return all)
    return [best];
  }

  _extractColumnName(where) {
    if (where.left?.type === 'column_ref') return where.left.name;
    if (where.right?.type === 'column_ref') return where.right.name;
    return null;
  }

  _extractLiteralValue(where) {
    if (where.left?.type === 'literal') return where.left.value;
    if (where.right?.type === 'literal') return where.right.value;
    return undefined;
  }
}

// Export cost functions for testing
export {
  seqScanCost,
  filterScanCost,
  btreeIndexScanCost,
  hashIndexScanCost,
  btreePKLookupCost,
  SEQ_PAGE_COST,
  RANDOM_PAGE_COST,
  CPU_TUPLE_COST,
};
