// event-triggers.js — PostgreSQL-compatible event triggers for HenryDB
// CREATE EVENT TRIGGER, ddl_command_start/end, table_rewrite, sql_drop.

/**
 * Supported DDL events.
 */
const DDL_EVENTS = new Set([
  'ddl_command_start',
  'ddl_command_end',
  'table_rewrite',
  'sql_drop',
]);

/**
 * EventTrigger — a database-level trigger on DDL events.
 */
class EventTrigger {
  constructor(name, options) {
    this.name = name;
    this.event = options.event;
    this.handler = options.handler; // Function to execute
    this.tags = options.tags || null; // Filter: ['CREATE TABLE', 'DROP TABLE', ...]
    this.enabled = options.enabled !== false;
    this.createdAt = Date.now();
    this.fireCount = 0;
  }

  /**
   * Check if this trigger should fire for a given DDL command.
   */
  shouldFire(event, commandTag) {
    if (this.event !== event) return false;
    if (!this.enabled) return false;
    if (this.tags && !this.tags.includes(commandTag)) return false;
    return true;
  }
}

/**
 * EventTriggerManager — manages event triggers.
 */
export class EventTriggerManager {
  constructor() {
    this._triggers = new Map(); // name → EventTrigger
    this._byEvent = new Map(); // event → [EventTrigger]

    for (const event of DDL_EVENTS) {
      this._byEvent.set(event, []);
    }
  }

  /**
   * CREATE EVENT TRIGGER name ON event [WHEN TAG IN ('cmd1', 'cmd2')] EXECUTE handler.
   */
  create(name, options) {
    const lowerName = name.toLowerCase();
    if (this._triggers.has(lowerName)) {
      throw new Error(`Event trigger '${name}' already exists`);
    }
    if (!DDL_EVENTS.has(options.event)) {
      throw new Error(`Unknown event '${options.event}'. Valid: ${[...DDL_EVENTS].join(', ')}`);
    }

    const trigger = new EventTrigger(lowerName, options);
    this._triggers.set(lowerName, trigger);
    this._byEvent.get(trigger.event).push(trigger);

    return {
      name: trigger.name,
      event: trigger.event,
      tags: trigger.tags,
      enabled: trigger.enabled,
    };
  }

  /**
   * DROP EVENT TRIGGER name.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    const trigger = this._triggers.get(lowerName);
    if (!trigger) {
      if (ifExists) return false;
      throw new Error(`Event trigger '${name}' does not exist`);
    }

    const eventList = this._byEvent.get(trigger.event);
    const idx = eventList.findIndex(t => t.name === lowerName);
    if (idx >= 0) eventList.splice(idx, 1);
    this._triggers.delete(lowerName);
    return true;
  }

  /**
   * ALTER EVENT TRIGGER — enable/disable.
   */
  alter(name, options) {
    const trigger = this._triggers.get(name.toLowerCase());
    if (!trigger) throw new Error(`Event trigger '${name}' does not exist`);

    if ('enabled' in options) trigger.enabled = options.enabled;
    if (options.rename) {
      this._triggers.delete(trigger.name);
      trigger.name = options.rename.toLowerCase();
      this._triggers.set(trigger.name, trigger);
    }

    return { name: trigger.name, enabled: trigger.enabled };
  }

  /**
   * Fire event triggers for a DDL command.
   */
  fire(event, context = {}) {
    const triggers = this._byEvent.get(event) || [];
    const results = [];

    for (const trigger of triggers) {
      if (trigger.shouldFire(event, context.commandTag)) {
        trigger.fireCount++;
        try {
          const result = trigger.handler({
            event,
            commandTag: context.commandTag,
            objectType: context.objectType,
            objectName: context.objectName,
            schemaName: context.schemaName || 'public',
            ...context,
          });
          results.push({ trigger: trigger.name, result });
        } catch (err) {
          results.push({ trigger: trigger.name, error: err.message });
          // ddl_command_start errors should abort the command
          if (event === 'ddl_command_start') {
            throw new Error(`Event trigger '${trigger.name}' aborted: ${err.message}`);
          }
        }
      }
    }

    return results;
  }

  /**
   * Get all triggers for an event.
   */
  getTriggersForEvent(event) {
    return (this._byEvent.get(event) || []).map(t => ({
      name: t.name,
      tags: t.tags,
      enabled: t.enabled,
      fireCount: t.fireCount,
    }));
  }

  has(name) { return this._triggers.has(name.toLowerCase()); }

  list() {
    return [...this._triggers.values()].map(t => ({
      name: t.name,
      event: t.event,
      tags: t.tags,
      enabled: t.enabled,
      fireCount: t.fireCount,
    }));
  }
}
