// row-level-security.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { RLSManager, SecurityContext } from './row-level-security.js';

let rls;

const rows = [
  { id: 1, owner: 'alice', dept: 'engineering', salary: 90000 },
  { id: 2, owner: 'bob', dept: 'engineering', salary: 85000 },
  { id: 3, owner: 'carol', dept: 'marketing', salary: 70000 },
  { id: 4, owner: 'dave', dept: 'marketing', salary: 75000 },
  { id: 5, owner: 'eve', dept: 'sales', salary: 60000 },
];

describe('Row-Level Security', () => {
  beforeEach(() => {
    rls = new RLSManager();
  });

  test('no filtering when RLS not enabled', () => {
    const ctx = new SecurityContext({ user: 'alice' });
    const filtered = rls.filterRows('employees', rows, 'SELECT', ctx);
    assert.equal(filtered.length, 5);
  });

  test('deny all when RLS enabled but no policies', () => {
    rls.enableRLS('employees');
    const ctx = new SecurityContext({ user: 'alice' });
    const filtered = rls.filterRows('employees', rows, 'SELECT', ctx);
    assert.equal(filtered.length, 0);
  });

  test('owner-based policy: users see only their own rows', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'owner_policy',
      table: 'employees',
      command: 'SELECT',
      using: (row, ctx) => row.owner === ctx.currentUser,
    });

    const alice = new SecurityContext({ user: 'alice' });
    const filtered = rls.filterRows('employees', rows, 'SELECT', alice);
    assert.equal(filtered.length, 1);
    assert.equal(filtered[0].owner, 'alice');
  });

  test('department-based policy', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'dept_policy',
      table: 'employees',
      command: 'ALL',
      using: (row, ctx) => row.dept === ctx.getVar('department'),
    });

    const ctx = new SecurityContext({ user: 'manager', vars: { department: 'engineering' } });
    const filtered = rls.filterRows('employees', rows, 'SELECT', ctx);
    assert.equal(filtered.length, 2);
    assert.ok(filtered.every(r => r.dept === 'engineering'));
  });

  test('superuser bypasses RLS', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'deny_all',
      table: 'employees',
      using: () => false,
    });

    const superCtx = new SecurityContext({ user: 'admin', superuser: true });
    const filtered = rls.filterRows('employees', rows, 'SELECT', superCtx);
    assert.equal(filtered.length, 5);
  });

  test('FORCE RLS applies even to superuser', () => {
    rls.enableRLS('employees', { force: true });
    rls.createPolicy({
      name: 'restrictive',
      table: 'employees',
      using: (row) => row.salary < 80000,
    });

    const superCtx = new SecurityContext({ user: 'admin', superuser: true });
    const filtered = rls.filterRows('employees', rows, 'SELECT', superCtx);
    assert.ok(filtered.length < 5);
  });

  test('multiple permissive policies OR together', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'own_rows',
      table: 'employees',
      using: (row, ctx) => row.owner === ctx.currentUser,
    });
    rls.createPolicy({
      name: 'engineering_visible',
      table: 'employees',
      using: (row) => row.dept === 'engineering',
    });

    const ctx = new SecurityContext({ user: 'carol' });
    const filtered = rls.filterRows('employees', rows, 'SELECT', ctx);
    // carol sees her own row + all engineering rows
    assert.equal(filtered.length, 3);
  });

  test('restrictive policy ANDs with permissive', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'see_all',
      table: 'employees',
      permissive: true,
      using: () => true,
    });
    rls.createPolicy({
      name: 'salary_cap',
      table: 'employees',
      permissive: false, // RESTRICTIVE
      using: (row) => row.salary <= 80000,
    });

    const ctx = new SecurityContext({ user: 'anyone' });
    const filtered = rls.filterRows('employees', rows, 'SELECT', ctx);
    assert.ok(filtered.every(r => r.salary <= 80000));
    assert.equal(filtered.length, 3);
  });

  test('command-specific policies', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'select_own',
      table: 'employees',
      command: 'SELECT',
      using: (row, ctx) => row.owner === ctx.currentUser,
    });
    rls.createPolicy({
      name: 'delete_own',
      table: 'employees',
      command: 'DELETE',
      using: (row, ctx) => row.owner === ctx.currentUser,
    });

    const ctx = new SecurityContext({ user: 'alice' });
    
    const selectRows = rls.filterRows('employees', rows, 'SELECT', ctx);
    assert.equal(selectRows.length, 1);

    // INSERT has no policy → denied
    const insertRows = rls.filterRows('employees', rows, 'INSERT', ctx);
    assert.equal(insertRows.length, 0);
  });

  test('WITH CHECK for INSERT validation', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'insert_own',
      table: 'employees',
      command: 'INSERT',
      withCheck: (row, ctx) => row.owner === ctx.currentUser,
    });

    const ctx = new SecurityContext({ user: 'alice' });
    
    assert.ok(rls.checkRow('employees', { id: 6, owner: 'alice', dept: 'eng', salary: 100 }, 'INSERT', ctx));
    assert.ok(!rls.checkRow('employees', { id: 7, owner: 'bob', dept: 'eng', salary: 100 }, 'INSERT', ctx));
  });

  test('role-based policy', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'manager_see_all',
      table: 'employees',
      roles: ['manager'],
      using: () => true,
    });
    rls.createPolicy({
      name: 'employee_own',
      table: 'employees',
      roles: ['employee'],
      using: (row, ctx) => row.owner === ctx.currentUser,
    });

    const mgr = new SecurityContext({ user: 'boss', role: 'manager' });
    assert.equal(rls.filterRows('employees', rows, 'SELECT', mgr).length, 5);

    const emp = new SecurityContext({ user: 'alice', role: 'employee' });
    assert.equal(rls.filterRows('employees', rows, 'SELECT', emp).length, 1);
  });

  test('drop policy', () => {
    rls.enableRLS('employees');
    rls.createPolicy({ name: 'temp', table: 'employees', using: () => true });
    assert.equal(rls.listPolicies('employees').length, 1);
    rls.dropPolicy('employees', 'temp');
    assert.equal(rls.listPolicies('employees').length, 0);
  });

  test('disable RLS', () => {
    rls.enableRLS('employees');
    assert.ok(rls.isEnabled('employees'));
    rls.disableRLS('employees');
    assert.ok(!rls.isEnabled('employees'));
  });

  test('session variables in policy', () => {
    rls.enableRLS('employees');
    rls.createPolicy({
      name: 'tenant_isolation',
      table: 'employees',
      using: (row, ctx) => row.dept === ctx.getVar('tenant'),
    });

    const ctx = new SecurityContext({ user: 'user1', vars: { tenant: 'sales' } });
    const filtered = rls.filterRows('employees', rows, 'SELECT', ctx);
    assert.equal(filtered.length, 1);
    assert.equal(filtered[0].dept, 'sales');
  });

  test('listPolicies returns all or filtered', () => {
    rls.enableRLS('employees');
    rls.enableRLS('orders');
    rls.createPolicy({ name: 'p1', table: 'employees', using: () => true });
    rls.createPolicy({ name: 'p2', table: 'orders', using: () => true });

    assert.equal(rls.listPolicies().length, 2);
    assert.equal(rls.listPolicies('employees').length, 1);
  });

  test('SecurityContext roles', () => {
    const ctx = new SecurityContext({ user: 'alice', role: 'admin', roles: ['admin', 'user'] });
    assert.ok(ctx.hasRole('admin'));
    assert.ok(ctx.hasRole('user'));
    assert.ok(!ctx.hasRole('superadmin'));
  });
});
