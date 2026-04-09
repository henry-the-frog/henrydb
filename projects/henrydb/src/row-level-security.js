// row-level-security.js — Row-Level Security (RLS) for HenryDB
// CREATE POLICY, ALTER TABLE ENABLE ROW LEVEL SECURITY, security contexts.
// PostgreSQL-compatible RLS: policies filter rows per user/role.

/**
 * SecurityContext — represents the current user/session context.
 */
export class SecurityContext {
  constructor(options = {}) {
    this.currentUser = options.user || 'anonymous';
    this.currentRole = options.role || 'public';
    this.roles = new Set(options.roles || [options.role || 'public']);
    this.sessionVars = new Map(Object.entries(options.vars || {}));
    this.isSuperuser = options.superuser || false;
  }

  hasRole(role) {
    return this.roles.has(role);
  }

  getVar(name) {
    return this.sessionVars.get(name);
  }

  setVar(name, value) {
    this.sessionVars.set(name, value);
  }
}

/**
 * Policy — a single RLS policy definition.
 */
class Policy {
  constructor(options) {
    this.name = options.name;
    this.table = options.table.toLowerCase();
    this.command = (options.command || 'ALL').toUpperCase(); // ALL, SELECT, INSERT, UPDATE, DELETE
    this.roles = options.roles || ['public']; // Which roles this policy applies to
    this.using = options.using || null;     // Row visibility check (for SELECT/UPDATE/DELETE)
    this.withCheck = options.withCheck || null; // Row validity check (for INSERT/UPDATE)
    this.permissive = options.permissive !== false; // PERMISSIVE (default) or RESTRICTIVE
  }

  /**
   * Check if this policy applies to the given command and role.
   */
  appliesTo(command, role) {
    if (this.command !== 'ALL' && this.command !== command.toUpperCase()) return false;
    if (this.roles.includes('public')) return true;
    return this.roles.includes(role);
  }

  /**
   * Evaluate the USING clause against a row.
   */
  checkUsing(row, context) {
    if (!this.using) return true;
    return this.using(row, context);
  }

  /**
   * Evaluate the WITH CHECK clause against a row.
   */
  checkWithCheck(row, context) {
    if (!this.withCheck) return true;
    return this.withCheck(row, context);
  }
}

/**
 * RLSManager — manages row-level security policies.
 */
export class RLSManager {
  constructor() {
    this._enabledTables = new Set(); // tables with RLS enabled
    this._policies = new Map(); // table → [Policy]
    this._forceEnabled = new Set(); // tables with FORCE RLS (applies even to table owners)
  }

  /**
   * Enable RLS on a table.
   */
  enableRLS(table, options = {}) {
    const lowerTable = table.toLowerCase();
    this._enabledTables.add(lowerTable);
    if (options.force) {
      this._forceEnabled.add(lowerTable);
    }
    if (!this._policies.has(lowerTable)) {
      this._policies.set(lowerTable, []);
    }
  }

  /**
   * Disable RLS on a table.
   */
  disableRLS(table) {
    const lowerTable = table.toLowerCase();
    this._enabledTables.delete(lowerTable);
    this._forceEnabled.delete(lowerTable);
  }

  /**
   * Check if RLS is enabled on a table.
   */
  isEnabled(table) {
    return this._enabledTables.has(table.toLowerCase());
  }

  /**
   * Create a policy on a table.
   */
  createPolicy(options) {
    const table = options.table.toLowerCase();
    if (!this._enabledTables.has(table)) {
      throw new Error(`RLS is not enabled on table '${options.table}'`);
    }

    const policies = this._policies.get(table) || [];
    if (policies.some(p => p.name === options.name)) {
      throw new Error(`Policy '${options.name}' already exists on table '${options.table}'`);
    }

    const policy = new Policy(options);
    policies.push(policy);
    this._policies.set(table, policies);
    return policy;
  }

  /**
   * Drop a policy.
   */
  dropPolicy(table, name, ifExists = false) {
    const lowerTable = table.toLowerCase();
    const policies = this._policies.get(lowerTable);
    if (!policies) {
      if (ifExists) return false;
      throw new Error(`No policies on table '${table}'`);
    }

    const idx = policies.findIndex(p => p.name === name);
    if (idx < 0) {
      if (ifExists) return false;
      throw new Error(`Policy '${name}' does not exist on table '${table}'`);
    }

    policies.splice(idx, 1);
    return true;
  }

  /**
   * Filter rows based on RLS policies.
   * Returns only rows the current context is allowed to see.
   */
  filterRows(table, rows, command, context) {
    const lowerTable = table.toLowerCase();

    // RLS not enabled — return all rows
    if (!this._enabledTables.has(lowerTable)) return rows;

    // Superusers bypass RLS unless FORCE is set
    if (context.isSuperuser && !this._forceEnabled.has(lowerTable)) return rows;

    const policies = this._policies.get(lowerTable) || [];
    const applicablePolicies = policies.filter(p => p.appliesTo(command, context.currentRole));

    // If no policies apply, deny all rows (PostgreSQL behavior)
    if (applicablePolicies.length === 0) return [];

    // Separate permissive and restrictive policies
    const permissive = applicablePolicies.filter(p => p.permissive);
    const restrictive = applicablePolicies.filter(p => !p.permissive);

    return rows.filter(row => {
      // At least one permissive policy must pass (OR)
      const permissivePass = permissive.length === 0 || permissive.some(p => p.checkUsing(row, context));
      
      // All restrictive policies must pass (AND)
      const restrictivePass = restrictive.every(p => p.checkUsing(row, context));

      return permissivePass && restrictivePass;
    });
  }

  /**
   * Check if a row passes WITH CHECK for INSERT/UPDATE.
   */
  checkRow(table, row, command, context) {
    const lowerTable = table.toLowerCase();
    if (!this._enabledTables.has(lowerTable)) return true;
    if (context.isSuperuser && !this._forceEnabled.has(lowerTable)) return true;

    const policies = this._policies.get(lowerTable) || [];
    const applicable = policies.filter(p => p.appliesTo(command, context.currentRole));

    if (applicable.length === 0) return false;

    const permissive = applicable.filter(p => p.permissive);
    const restrictive = applicable.filter(p => !p.permissive);

    const permissivePass = permissive.length === 0 || permissive.some(p => p.checkWithCheck(row, context));
    const restrictivePass = restrictive.every(p => p.checkWithCheck(row, context));

    return permissivePass && restrictivePass;
  }

  /**
   * List policies on a table.
   */
  listPolicies(table = null) {
    const result = [];
    for (const [t, policies] of this._policies) {
      if (table && t !== table.toLowerCase()) continue;
      for (const p of policies) {
        result.push({
          name: p.name,
          table: t,
          command: p.command,
          roles: p.roles,
          permissive: p.permissive,
        });
      }
    }
    return result;
  }
}
