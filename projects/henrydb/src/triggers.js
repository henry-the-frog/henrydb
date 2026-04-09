// triggers.js — Enhanced trigger system with NEW/OLD row references
// CREATE TRIGGER name BEFORE/AFTER INSERT/UPDATE/DELETE ON table
// FOR EACH ROW EXECUTE FUNCTION trigger_fn();
// Trigger functions receive NEW and OLD as special variables.

import { PLParser, PLInterpreter } from './plsql.js';

/**
 * TriggerManager — manages database triggers with row-level execution.
 */
export class TriggerManager {
  constructor() {
    this._triggers = new Map(); // name → TriggerDef
    this._tableTriggers = new Map(); // table → [TriggerDef]
  }

  /**
   * Register a trigger.
   */
  createTrigger(options) {
    const { name, timing, event, table, forEach, functionBody, functionName, condition } = options;
    
    if (this._triggers.has(name.toLowerCase())) {
      throw new Error(`Trigger '${name}' already exists`);
    }

    const lowerTable = table.toLowerCase();
    let ast = null;
    if (functionBody) {
      const parser = new PLParser(functionBody);
      ast = parser.parse();
    }

    const trigger = {
      name,
      timing: timing.toUpperCase(),   // BEFORE | AFTER | INSTEAD OF
      event: event.toUpperCase(),     // INSERT | UPDATE | DELETE
      table: lowerTable,
      forEach: (forEach || 'ROW').toUpperCase(), // ROW | STATEMENT
      functionBody,
      functionName,
      ast,
      condition: condition || null,   // WHEN clause
      enabled: true,
      createdAt: Date.now(),
    };

    this._triggers.set(name.toLowerCase(), trigger);

    if (!this._tableTriggers.has(lowerTable)) {
      this._tableTriggers.set(lowerTable, []);
    }
    this._tableTriggers.get(lowerTable).push(trigger);

    return trigger;
  }

  /**
   * Drop a trigger.
   */
  dropTrigger(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    const trigger = this._triggers.get(lowerName);
    if (!trigger) {
      if (ifExists) return false;
      throw new Error(`Trigger '${name}' does not exist`);
    }

    this._triggers.delete(lowerName);
    const tableTriggers = this._tableTriggers.get(trigger.table);
    if (tableTriggers) {
      const idx = tableTriggers.findIndex(t => t.name.toLowerCase() === lowerName);
      if (idx >= 0) tableTriggers.splice(idx, 1);
    }
    return true;
  }

  /**
   * Enable/disable a trigger.
   */
  setEnabled(name, enabled) {
    const trigger = this._triggers.get(name.toLowerCase());
    if (!trigger) throw new Error(`Trigger '${name}' does not exist`);
    trigger.enabled = enabled;
  }

  /**
   * Fire triggers for a given timing/event/table combination.
   * 
   * @param {string} timing - BEFORE or AFTER
   * @param {string} event - INSERT, UPDATE, or DELETE
   * @param {string} table - Table name
   * @param {object} context - { newRow, oldRow, db }
   * @returns {object|null} Modified newRow (for BEFORE triggers), or null
   */
  fire(timing, event, table, context) {
    const lowerTable = table.toLowerCase();
    const triggers = this._tableTriggers.get(lowerTable) || [];
    
    let currentNew = context.newRow ? { ...context.newRow } : null;
    let suppressed = false;

    for (const trigger of triggers) {
      if (!trigger.enabled) continue;
      if (trigger.timing !== timing.toUpperCase()) continue;
      if (trigger.event !== event.toUpperCase()) continue;

      // Evaluate WHEN condition if present
      if (trigger.condition && !this._evalCondition(trigger.condition, currentNew, context.oldRow)) {
        continue;
      }

      if (trigger.forEach === 'ROW') {
        const result = this._executeTriggerFn(trigger, {
          newRow: currentNew,
          oldRow: context.oldRow,
          db: context.db,
          tgName: trigger.name,
          tgTable: trigger.table,
          tgTiming: trigger.timing,
          tgEvent: trigger.event,
        });

        if (result !== undefined && result !== null) {
          if (result === 'SUPPRESS') {
            suppressed = true;
            break;
          }
          // BEFORE triggers can modify NEW
          if (timing === 'BEFORE' && typeof result === 'object') {
            currentNew = result;
          }
        }
      } else {
        // STATEMENT-level triggers don't receive row data
        this._executeTriggerFn(trigger, {
          newRow: null,
          oldRow: null,
          db: context.db,
          tgName: trigger.name,
          tgTable: trigger.table,
          tgTiming: trigger.timing,
          tgEvent: trigger.event,
        });
      }
    }

    return { newRow: currentNew, suppressed };
  }

  _executeTriggerFn(trigger, context) {
    if (!trigger.ast) return null;

    const interp = new PLInterpreter(context.db);
    
    // Set up special variables: NEW, OLD, TG_*
    const params = {};
    if (context.newRow) {
      // Flatten NEW row into variables with new_ prefix
      for (const [col, val] of Object.entries(context.newRow)) {
        params[`new_${col}`] = val;
      }
      params.new = context.newRow;
    }
    if (context.oldRow) {
      for (const [col, val] of Object.entries(context.oldRow)) {
        params[`old_${col}`] = val;
      }
      params.old = context.oldRow;
    }
    params.tg_name = context.tgName;
    params.tg_table = context.tgTable;
    params.tg_when = context.tgTiming;
    params.tg_op = context.tgEvent;

    return interp.execute(trigger.ast, params);
  }

  _evalCondition(condition, newRow, oldRow) {
    // Simple condition evaluation
    // condition is like "NEW.amount > 100"
    try {
      const upper = condition.toUpperCase();
      let expr = condition;
      
      if (newRow) {
        for (const [col, val] of Object.entries(newRow)) {
          const regex = new RegExp(`\\bNEW\\.${col}\\b`, 'gi');
          expr = expr.replace(regex, typeof val === 'string' ? `'${val}'` : String(val));
        }
      }
      if (oldRow) {
        for (const [col, val] of Object.entries(oldRow)) {
          const regex = new RegExp(`\\bOLD\\.${col}\\b`, 'gi');
          expr = expr.replace(regex, typeof val === 'string' ? `'${val}'` : String(val));
        }
      }

      // Simple evaluation
      const compMatch = expr.match(/^(.+?)\s*(>=|<=|!=|<>|>|<|=)\s*(.+)$/);
      if (compMatch) {
        const left = parseFloat(compMatch[1]) || compMatch[1].replace(/'/g, '');
        const right = parseFloat(compMatch[3]) || compMatch[3].replace(/'/g, '');
        switch (compMatch[2]) {
          case '>': return left > right;
          case '<': return left < right;
          case '>=': return left >= right;
          case '<=': return left <= right;
          case '=': return left == right;
          case '!=': case '<>': return left != right;
        }
      }
      return true; // Default to true if can't evaluate
    } catch {
      return true;
    }
  }

  /**
   * List all triggers.
   */
  listTriggers(table = null) {
    const triggers = [];
    for (const trigger of this._triggers.values()) {
      if (table && trigger.table !== table.toLowerCase()) continue;
      triggers.push({
        name: trigger.name,
        timing: trigger.timing,
        event: trigger.event,
        table: trigger.table,
        forEach: trigger.forEach,
        enabled: trigger.enabled,
      });
    }
    return triggers;
  }

  hasTrigger(name) {
    return this._triggers.has(name.toLowerCase());
  }
}

/**
 * Parse a CREATE TRIGGER statement.
 * 
 * CREATE TRIGGER name
 *   BEFORE|AFTER INSERT|UPDATE|DELETE ON table
 *   [FOR EACH ROW|STATEMENT]
 *   [WHEN (condition)]
 *   EXECUTE FUNCTION fn_name() | AS $$ body $$
 */
export function parseCreateTrigger(sql) {
  const match = sql.match(
    /CREATE\s+TRIGGER\s+(\w+)\s+(BEFORE|AFTER|INSTEAD\s+OF)\s+(INSERT|UPDATE|DELETE)\s+ON\s+(\w+)/i
  );
  if (!match) throw new Error('Cannot parse CREATE TRIGGER');

  const name = match[1];
  const timing = match[2].toUpperCase().replace(/\s+/g, '_');
  const event = match[3].toUpperCase();
  const table = match[4];

  let forEach = 'ROW';
  if (/FOR\s+EACH\s+STATEMENT/i.test(sql)) forEach = 'STATEMENT';

  let condition = null;
  const whenMatch = sql.match(/WHEN\s*\((.+?)\)/i);
  if (whenMatch) condition = whenMatch[1];

  let functionBody = null;
  let functionName = null;
  const dollarMatch = sql.match(/\$\$\s*([\s\S]*?)\s*\$\$/);
  if (dollarMatch) {
    functionBody = dollarMatch[1];
  } else {
    const execMatch = sql.match(/EXECUTE\s+(?:FUNCTION|PROCEDURE)\s+(\w+)/i);
    if (execMatch) functionName = execMatch[1];
  }

  return { name, timing, event, table, forEach, condition, functionBody, functionName };
}
