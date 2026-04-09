// rbac.js — Role-Based Access Control for HenryDB
// CREATE ROLE, GRANT, REVOKE, role hierarchies, privilege checking.

/**
 * Privilege types (PostgreSQL-compatible).
 */
const PRIVILEGES = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE', 'REFERENCES', 'TRIGGER', 'CREATE', 'CONNECT', 'TEMPORARY', 'EXECUTE', 'USAGE', 'ALL'];

/**
 * Role — a database role (user or group).
 */
class Role {
  constructor(name, options = {}) {
    this.name = name;
    this.superuser = options.superuser || false;
    this.createdb = options.createdb || false;
    this.createrole = options.createrole || false;
    this.login = options.login !== false; // Default: can login
    this.inherit = options.inherit !== false; // Default: inherit parent privs
    this.password = options.password || null;
    this.memberOf = new Set(); // Parent roles
    this.members = new Set(); // Child roles (roles that have this as parent)
    this.createdAt = Date.now();
  }
}

/**
 * GrantEntry — a privilege grant on an object.
 */
class GrantEntry {
  constructor(privilege, objectType, objectName, grantee, options = {}) {
    this.privilege = privilege;
    this.objectType = objectType; // TABLE, SCHEMA, FUNCTION, etc.
    this.objectName = objectName;
    this.grantee = grantee;
    this.withGrantOption = options.withGrantOption || false;
    this.grantedBy = options.grantedBy || null;
    this.grantedAt = Date.now();
  }
}

/**
 * RBACManager — manages roles and privileges.
 */
export class RBACManager {
  constructor() {
    this._roles = new Map(); // name → Role
    this._grants = []; // Array of GrantEntry
    
    // Create built-in roles
    this._createBuiltinRoles();
  }

  _createBuiltinRoles() {
    this._roles.set('public', new Role('public', { login: false }));
  }

  /**
   * CREATE ROLE name [WITH options].
   */
  createRole(name, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._roles.has(lowerName)) {
      throw new Error(`Role '${name}' already exists`);
    }
    const role = new Role(lowerName, options);
    this._roles.set(lowerName, role);
    return {
      name: lowerName,
      superuser: role.superuser,
      login: role.login,
    };
  }

  /**
   * DROP ROLE name.
   */
  dropRole(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._roles.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Role '${name}' does not exist`);
    }
    if (lowerName === 'public') throw new Error("Cannot drop role 'public'");

    const role = this._roles.get(lowerName);
    
    // Remove from parent roles
    for (const parentName of role.memberOf) {
      const parent = this._roles.get(parentName);
      if (parent) parent.members.delete(lowerName);
    }
    
    // Remove from child roles
    for (const childName of role.members) {
      const child = this._roles.get(childName);
      if (child) child.memberOf.delete(lowerName);
    }

    // Remove grants
    this._grants = this._grants.filter(g => g.grantee !== lowerName);
    
    this._roles.delete(lowerName);
    return true;
  }

  /**
   * ALTER ROLE name SET option.
   */
  alterRole(name, changes) {
    const role = this._roles.get(name.toLowerCase());
    if (!role) throw new Error(`Role '${name}' does not exist`);

    if ('superuser' in changes) role.superuser = changes.superuser;
    if ('createdb' in changes) role.createdb = changes.createdb;
    if ('createrole' in changes) role.createrole = changes.createrole;
    if ('login' in changes) role.login = changes.login;
    if ('password' in changes) role.password = changes.password;
    
    return role;
  }

  /**
   * GRANT privilege ON object TO role.
   */
  grant(privilege, objectType, objectName, grantee, options = {}) {
    const upperPriv = privilege.toUpperCase();
    const lowerGrantee = grantee.toLowerCase();
    
    if (!this._roles.has(lowerGrantee)) {
      throw new Error(`Role '${grantee}' does not exist`);
    }

    // Check for duplicate
    const existing = this._grants.find(g =>
      g.privilege === upperPriv &&
      g.objectType === objectType.toUpperCase() &&
      g.objectName === objectName.toLowerCase() &&
      g.grantee === lowerGrantee
    );

    if (existing) {
      if (options.withGrantOption) existing.withGrantOption = true;
      return existing;
    }

    const entry = new GrantEntry(
      upperPriv,
      objectType.toUpperCase(),
      objectName.toLowerCase(),
      lowerGrantee,
      options
    );
    this._grants.push(entry);
    return entry;
  }

  /**
   * GRANT role TO role (role membership).
   */
  grantRole(parentRole, childRole) {
    const parent = this._roles.get(parentRole.toLowerCase());
    const child = this._roles.get(childRole.toLowerCase());
    if (!parent) throw new Error(`Role '${parentRole}' does not exist`);
    if (!child) throw new Error(`Role '${childRole}' does not exist`);

    // Check for circular membership
    if (this._hasCircularMembership(parentRole.toLowerCase(), childRole.toLowerCase())) {
      throw new Error(`Circular role membership: '${childRole}' is already a member of '${parentRole}'`);
    }

    child.memberOf.add(parent.name);
    parent.members.add(child.name);
  }

  /**
   * REVOKE privilege ON object FROM role.
   */
  revoke(privilege, objectType, objectName, grantee) {
    const upperPriv = privilege.toUpperCase();
    const lowerGrantee = grantee.toLowerCase();

    const idx = this._grants.findIndex(g =>
      g.privilege === upperPriv &&
      g.objectType === objectType.toUpperCase() &&
      g.objectName === objectName.toLowerCase() &&
      g.grantee === lowerGrantee
    );

    if (idx >= 0) {
      this._grants.splice(idx, 1);
      return true;
    }
    return false;
  }

  /**
   * REVOKE role FROM role.
   */
  revokeRole(parentRole, childRole) {
    const parent = this._roles.get(parentRole.toLowerCase());
    const child = this._roles.get(childRole.toLowerCase());
    if (!parent || !child) return false;

    child.memberOf.delete(parent.name);
    parent.members.delete(child.name);
    return true;
  }

  /**
   * Check if a role has a specific privilege on an object.
   * Considers role inheritance.
   */
  hasPrivilege(roleName, privilege, objectType, objectName) {
    const lowerRole = roleName.toLowerCase();
    const role = this._roles.get(lowerRole);
    if (!role) return false;

    // Superusers bypass all checks
    if (role.superuser) return true;

    const upperPriv = privilege.toUpperCase();
    const upperType = objectType.toUpperCase();
    const lowerObj = objectName.toLowerCase();

    // Check direct grants
    if (this._hasDirectGrant(lowerRole, upperPriv, upperType, lowerObj)) return true;

    // Check 'public' grants
    if (this._hasDirectGrant('public', upperPriv, upperType, lowerObj)) return true;

    // Check inherited grants through role membership
    if (role.inherit) {
      const visited = new Set();
      return this._hasInheritedGrant(lowerRole, upperPriv, upperType, lowerObj, visited);
    }

    return false;
  }

  /**
   * Get all effective roles for a user (including inherited).
   */
  getEffectiveRoles(roleName) {
    const roles = new Set();
    this._collectRoles(roleName.toLowerCase(), roles);
    return [...roles];
  }

  /**
   * List all roles.
   */
  listRoles() {
    return [...this._roles.values()].map(r => ({
      name: r.name,
      superuser: r.superuser,
      login: r.login,
      createdb: r.createdb,
      memberOf: [...r.memberOf],
      members: [...r.members],
    }));
  }

  /**
   * List grants on an object.
   */
  listGrants(objectType = null, objectName = null) {
    let grants = this._grants;
    if (objectType) grants = grants.filter(g => g.objectType === objectType.toUpperCase());
    if (objectName) grants = grants.filter(g => g.objectName === objectName.toLowerCase());
    return grants.map(g => ({
      privilege: g.privilege,
      objectType: g.objectType,
      objectName: g.objectName,
      grantee: g.grantee,
      withGrantOption: g.withGrantOption,
    }));
  }

  hasRole(name) {
    return this._roles.has(name.toLowerCase());
  }

  _hasDirectGrant(role, privilege, objectType, objectName) {
    return this._grants.some(g =>
      g.grantee === role &&
      (g.privilege === privilege || g.privilege === 'ALL') &&
      g.objectType === objectType &&
      g.objectName === objectName
    );
  }

  _hasInheritedGrant(roleName, privilege, objectType, objectName, visited) {
    if (visited.has(roleName)) return false;
    visited.add(roleName);

    const role = this._roles.get(roleName);
    if (!role) return false;

    for (const parentName of role.memberOf) {
      if (this._hasDirectGrant(parentName, privilege, objectType, objectName)) return true;
      if (this._hasInheritedGrant(parentName, privilege, objectType, objectName, visited)) return true;
    }

    return false;
  }

  _collectRoles(roleName, roles) {
    if (roles.has(roleName)) return;
    roles.add(roleName);
    const role = this._roles.get(roleName);
    if (!role) return;
    for (const parent of role.memberOf) {
      this._collectRoles(parent, roles);
    }
  }

  _hasCircularMembership(parent, child) {
    const visited = new Set();
    const queue = [parent];
    while (queue.length > 0) {
      const current = queue.shift();
      if (current === child) return true;
      if (visited.has(current)) continue;
      visited.add(current);
      const role = this._roles.get(current);
      if (role) {
        for (const p of role.memberOf) queue.push(p);
      }
    }
    return false;
  }
}
