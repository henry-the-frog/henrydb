// table-inheritance.js — PostgreSQL-style table inheritance for HenryDB
// CREATE TABLE child INHERITS (parent)
// Queries on parent also return child rows.

/**
 * InheritanceManager — manages table inheritance hierarchy.
 */
export class InheritanceManager {
  constructor() {
    this._parents = new Map(); // table → parent table
    this._children = new Map(); // table → Set<child tables>
    this._tableColumns = new Map(); // table → {own: [], inherited: []}
  }

  /**
   * Register inheritance: child INHERITS (parent).
   */
  inherit(childTable, parentTable, childOwnColumns = []) {
    const child = childTable.toLowerCase();
    const parent = parentTable.toLowerCase();

    if (this._parents.has(child)) {
      throw new Error(`Table '${childTable}' already has a parent`);
    }

    this._parents.set(child, parent);

    if (!this._children.has(parent)) {
      this._children.set(parent, new Set());
    }
    this._children.get(parent).add(child);

    // Record column inheritance
    const parentCols = this._tableColumns.get(parent)?.own || [];
    const inheritedCols = [...parentCols];
    this._tableColumns.set(child, {
      own: childOwnColumns,
      inherited: inheritedCols,
      all: [...inheritedCols, ...childOwnColumns],
    });
  }

  /**
   * Register a table's own columns (for root tables).
   */
  registerTable(table, columns) {
    this._tableColumns.set(table.toLowerCase(), {
      own: columns,
      inherited: [],
      all: columns,
    });
  }

  /**
   * Get parent table for a given table.
   */
  getParent(table) {
    return this._parents.get(table.toLowerCase()) || null;
  }

  /**
   * Get direct children of a table.
   */
  getChildren(table) {
    const children = this._children.get(table.toLowerCase());
    return children ? [...children] : [];
  }

  /**
   * Get all descendants (recursive).
   */
  getAllDescendants(table) {
    const descendants = [];
    const queue = [table.toLowerCase()];
    while (queue.length > 0) {
      const current = queue.shift();
      const children = this._children.get(current);
      if (children) {
        for (const child of children) {
          descendants.push(child);
          queue.push(child);
        }
      }
    }
    return descendants;
  }

  /**
   * Get all tables to query (table + all descendants).
   * Used when querying a parent: SELECT * FROM parent includes child rows.
   */
  getQueryTargets(table) {
    return [table.toLowerCase(), ...this.getAllDescendants(table)];
  }

  /**
   * Get all columns for a table (own + inherited).
   */
  getColumns(table) {
    const info = this._tableColumns.get(table.toLowerCase());
    return info ? info.all : [];
  }

  /**
   * Get the complete inheritance chain for a table.
   */
  getInheritanceChain(table) {
    const chain = [];
    let current = table.toLowerCase();
    while (current) {
      chain.unshift(current);
      current = this._parents.get(current) || null;
    }
    return chain;
  }

  /**
   * Check if childTable inherits from parentTable (direct or indirect).
   */
  isDescendantOf(childTable, parentTable) {
    let current = childTable.toLowerCase();
    const target = parentTable.toLowerCase();
    while (current) {
      if (current === target) return true;
      current = this._parents.get(current) || null;
    }
    return false;
  }

  /**
   * Remove inheritance relationship.
   */
  noInherit(childTable) {
    const child = childTable.toLowerCase();
    const parent = this._parents.get(child);
    if (!parent) return false;

    this._parents.delete(child);
    const siblings = this._children.get(parent);
    if (siblings) siblings.delete(child);

    // Clear inherited columns
    const info = this._tableColumns.get(child);
    if (info) {
      info.inherited = [];
      info.all = [...info.own];
    }

    return true;
  }

  /**
   * Get the inheritance tree as a printable structure.
   */
  getTree() {
    const roots = [];
    for (const table of this._tableColumns.keys()) {
      if (!this._parents.has(table)) {
        roots.push(this._buildTree(table));
      }
    }
    return roots;
  }

  _buildTree(table) {
    const children = this._children.get(table) || new Set();
    return {
      table,
      columns: this._tableColumns.get(table)?.own || [],
      children: [...children].map(c => this._buildTree(c)),
    };
  }
}
