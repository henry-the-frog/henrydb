// rbac.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { RBACManager } from './rbac.js';

let rbac;

describe('RBACManager', () => {
  beforeEach(() => {
    rbac = new RBACManager();
  });

  test('CREATE ROLE', () => {
    const role = rbac.createRole('alice', { login: true });
    assert.equal(role.name, 'alice');
    assert.ok(rbac.hasRole('alice'));
  });

  test('CREATE ROLE superuser', () => {
    rbac.createRole('admin', { superuser: true });
    assert.ok(rbac.hasPrivilege('admin', 'SELECT', 'TABLE', 'anything'));
  });

  test('duplicate role throws', () => {
    rbac.createRole('alice');
    assert.throws(() => rbac.createRole('alice'), /already exists/);
  });

  test('DROP ROLE', () => {
    rbac.createRole('temp');
    rbac.dropRole('temp');
    assert.ok(!rbac.hasRole('temp'));
  });

  test('DROP ROLE IF EXISTS', () => {
    assert.equal(rbac.dropRole('nonexistent', true), false);
  });

  test('cannot drop public', () => {
    assert.throws(() => rbac.dropRole('public'), /Cannot drop/);
  });

  test('GRANT privilege on table', () => {
    rbac.createRole('alice');
    rbac.grant('SELECT', 'TABLE', 'users', 'alice');
    
    assert.ok(rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'users'));
    assert.ok(!rbac.hasPrivilege('alice', 'DELETE', 'TABLE', 'users'));
  });

  test('GRANT ALL', () => {
    rbac.createRole('alice');
    rbac.grant('ALL', 'TABLE', 'users', 'alice');
    
    assert.ok(rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'users'));
    assert.ok(rbac.hasPrivilege('alice', 'DELETE', 'TABLE', 'users'));
  });

  test('REVOKE privilege', () => {
    rbac.createRole('alice');
    rbac.grant('SELECT', 'TABLE', 'users', 'alice');
    rbac.revoke('SELECT', 'TABLE', 'users', 'alice');
    
    assert.ok(!rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'users'));
  });

  test('role inheritance: GRANT role TO role', () => {
    rbac.createRole('readers');
    rbac.createRole('alice');
    
    rbac.grant('SELECT', 'TABLE', 'users', 'readers');
    rbac.grantRole('readers', 'alice');
    
    assert.ok(rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'users'));
  });

  test('multi-level role inheritance', () => {
    rbac.createRole('base');
    rbac.createRole('mid');
    rbac.createRole('top');
    
    rbac.grant('SELECT', 'TABLE', 'data', 'base');
    rbac.grantRole('base', 'mid');
    rbac.grantRole('mid', 'top');
    
    assert.ok(rbac.hasPrivilege('top', 'SELECT', 'TABLE', 'data'));
  });

  test('REVOKE role', () => {
    rbac.createRole('readers');
    rbac.createRole('alice');
    
    rbac.grant('SELECT', 'TABLE', 'users', 'readers');
    rbac.grantRole('readers', 'alice');
    
    assert.ok(rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'users'));
    
    rbac.revokeRole('readers', 'alice');
    assert.ok(!rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'users'));
  });

  test('public role grants apply to all', () => {
    rbac.createRole('alice');
    rbac.grant('SELECT', 'TABLE', 'public_data', 'public');
    
    assert.ok(rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'public_data'));
  });

  test('no privilege by default', () => {
    rbac.createRole('alice');
    assert.ok(!rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'secret'));
  });

  test('getEffectiveRoles includes inherited', () => {
    rbac.createRole('base');
    rbac.createRole('mid');
    rbac.createRole('alice');
    rbac.grantRole('base', 'mid');
    rbac.grantRole('mid', 'alice');
    
    const roles = rbac.getEffectiveRoles('alice');
    assert.ok(roles.includes('alice'));
    assert.ok(roles.includes('mid'));
    assert.ok(roles.includes('base'));
  });

  test('listRoles', () => {
    rbac.createRole('alice');
    rbac.createRole('bob');
    const roles = rbac.listRoles();
    assert.ok(roles.length >= 3); // public + alice + bob
  });

  test('listGrants filtered', () => {
    rbac.createRole('alice');
    rbac.grant('SELECT', 'TABLE', 'users', 'alice');
    rbac.grant('INSERT', 'TABLE', 'users', 'alice');
    rbac.grant('SELECT', 'TABLE', 'orders', 'alice');
    
    const userGrants = rbac.listGrants('TABLE', 'users');
    assert.equal(userGrants.length, 2);
  });

  test('ALTER ROLE', () => {
    rbac.createRole('alice');
    rbac.alterRole('alice', { superuser: true });
    assert.ok(rbac.hasPrivilege('alice', 'DELETE', 'TABLE', 'anything'));
  });

  test('circular membership detected', () => {
    rbac.createRole('a');
    rbac.createRole('b');
    rbac.grantRole('a', 'b');
    assert.throws(() => rbac.grantRole('b', 'a'), /Circular/);
  });

  test('WITH GRANT OPTION', () => {
    rbac.createRole('alice');
    rbac.grant('SELECT', 'TABLE', 'users', 'alice', { withGrantOption: true });
    
    const grants = rbac.listGrants('TABLE', 'users');
    assert.ok(grants[0].withGrantOption);
  });

  test('case-insensitive', () => {
    rbac.createRole('Alice');
    assert.ok(rbac.hasRole('alice'));
    rbac.grant('SELECT', 'TABLE', 'USERS', 'ALICE');
    assert.ok(rbac.hasPrivilege('alice', 'SELECT', 'TABLE', 'users'));
  });
});
