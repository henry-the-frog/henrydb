// rule-system.js — PostgreSQL-compatible rule system for HenryDB
// CREATE RULE, ON SELECT/INSERT/UPDATE/DELETE, INSTEAD/ALSO actions.

/**
 * Rule — a rewrite rule on a table/view.
 */
class Rule {
  constructor(name, options) {
    this.name = name;
    this.table = options.table;
    this.event = options.event; // 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE'
    this.type = options.type || 'ALSO'; // 'INSTEAD' | 'ALSO'
    this.condition = options.condition || null; // WHERE clause function
    this.actions = options.actions || []; // Array of action functions/objects
    this.enabled = options.enabled !== false;
    this.createdAt = Date.now();
  }

  /**
   * Check if this rule applies to a given operation context.
   */
  matches(event, row = null) {
    if (this.event !== event) return false;
    if (!this.enabled) return false;
    if (this.condition && row && !this.condition(row)) return false;
    return true;
  }
}

/**
 * RuleManager — manages rewrite rules.
 */
export class RuleManager {
  constructor() {
    this._rules = new Map(); // table → [Rule]
    this._rulesByName = new Map(); // name → Rule
  }

  /**
   * CREATE RULE name AS ON event TO table [WHERE condition] DO [INSTEAD|ALSO] actions.
   */
  create(name, options) {
    const lowerName = name.toLowerCase();
    if (this._rulesByName.has(lowerName)) {
      if (options.orReplace) {
        this.drop(name);
      } else {
        throw new Error(`Rule '${name}' already exists`);
      }
    }

    const rule = new Rule(lowerName, {
      ...options,
      table: options.table.toLowerCase(),
    });

    const tableName = rule.table;
    if (!this._rules.has(tableName)) {
      this._rules.set(tableName, []);
    }
    this._rules.get(tableName).push(rule);
    this._rulesByName.set(lowerName, rule);

    return {
      name: rule.name,
      table: rule.table,
      event: rule.event,
      type: rule.type,
    };
  }

  /**
   * DROP RULE name ON table.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    const rule = this._rulesByName.get(lowerName);
    if (!rule) {
      if (ifExists) return false;
      throw new Error(`Rule '${name}' does not exist`);
    }

    const tableRules = this._rules.get(rule.table);
    if (tableRules) {
      const idx = tableRules.findIndex(r => r.name === lowerName);
      if (idx >= 0) tableRules.splice(idx, 1);
    }
    this._rulesByName.delete(lowerName);
    return true;
  }

  /**
   * Enable/disable a rule.
   */
  setEnabled(name, enabled) {
    const rule = this._rulesByName.get(name.toLowerCase());
    if (!rule) throw new Error(`Rule '${name}' does not exist`);
    rule.enabled = enabled;
  }

  /**
   * Get rules for an event on a table.
   */
  getRules(tableName, event, row = null) {
    const rules = this._rules.get(tableName.toLowerCase()) || [];
    return rules.filter(r => r.matches(event, row));
  }

  /**
   * Apply rules to an operation. Returns modified operation.
   */
  applyRules(tableName, event, originalOp, row = null) {
    const matchingRules = this.getRules(tableName, event, row);

    if (matchingRules.length === 0) {
      return { modified: false, operations: [originalOp] };
    }

    const insteadRules = matchingRules.filter(r => r.type === 'INSTEAD');
    const alsoRules = matchingRules.filter(r => r.type === 'ALSO');

    const operations = [];

    if (insteadRules.length > 0) {
      // INSTEAD replaces the original operation
      for (const rule of insteadRules) {
        for (const action of rule.actions) {
          if (typeof action === 'function') {
            const result = action(originalOp, row);
            if (result) operations.push(result);
          } else {
            operations.push(action);
          }
        }
      }
    } else {
      operations.push(originalOp);
    }

    // ALSO adds additional operations
    for (const rule of alsoRules) {
      for (const action of rule.actions) {
        if (typeof action === 'function') {
          const result = action(originalOp, row);
          if (result) operations.push(result);
        } else {
          operations.push(action);
        }
      }
    }

    return { modified: true, operations };
  }

  /**
   * List all rules for a table.
   */
  listForTable(tableName) {
    const rules = this._rules.get(tableName.toLowerCase()) || [];
    return rules.map(r => ({
      name: r.name,
      event: r.event,
      type: r.type,
      enabled: r.enabled,
    }));
  }

  /**
   * List all rules.
   */
  list() {
    return [...this._rulesByName.values()].map(r => ({
      name: r.name,
      table: r.table,
      event: r.event,
      type: r.type,
      enabled: r.enabled,
    }));
  }

  has(name) {
    return this._rulesByName.has(name.toLowerCase());
  }
}
