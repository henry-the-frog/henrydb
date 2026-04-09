// table-inheritance.test.js
import { describe, test, beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { InheritanceManager } from './table-inheritance.js';

let im;

describe('InheritanceManager', () => {
  beforeEach(() => {
    im = new InheritanceManager();
    im.registerTable('vehicles', ['id', 'make', 'model', 'year']);
  });

  test('register table columns', () => {
    assert.deepEqual(im.getColumns('vehicles'), ['id', 'make', 'model', 'year']);
  });

  test('inherit creates parent-child relationship', () => {
    im.inherit('cars', 'vehicles', ['doors', 'trunk_size']);
    assert.equal(im.getParent('cars'), 'vehicles');
    assert.deepEqual(im.getChildren('vehicles'), ['cars']);
  });

  test('inherited table has all columns', () => {
    im.inherit('cars', 'vehicles', ['doors']);
    const cols = im.getColumns('cars');
    assert.ok(cols.includes('id'));
    assert.ok(cols.includes('make'));
    assert.ok(cols.includes('doors'));
  });

  test('getQueryTargets includes descendants', () => {
    im.inherit('cars', 'vehicles', ['doors']);
    im.inherit('trucks', 'vehicles', ['payload']);
    
    const targets = im.getQueryTargets('vehicles');
    assert.ok(targets.includes('vehicles'));
    assert.ok(targets.includes('cars'));
    assert.ok(targets.includes('trucks'));
    assert.equal(targets.length, 3);
  });

  test('multi-level inheritance', () => {
    im.inherit('cars', 'vehicles', ['doors']);
    im.registerTable('cars', ['id', 'make', 'model', 'year', 'doors']); // Re-register with full cols
    im.inherit('electric_cars', 'cars', ['battery_kwh']);
    
    const targets = im.getQueryTargets('vehicles');
    assert.ok(targets.includes('electric_cars'));
    assert.equal(targets.length, 3);
  });

  test('getAllDescendants recursive', () => {
    im.inherit('cars', 'vehicles', []);
    im.registerTable('cars', ['id', 'make', 'model', 'year']);
    im.inherit('sedans', 'cars', []);
    im.inherit('suvs', 'cars', []);
    
    const desc = im.getAllDescendants('vehicles');
    assert.equal(desc.length, 3);
    assert.ok(desc.includes('sedans'));
  });

  test('getInheritanceChain', () => {
    im.inherit('cars', 'vehicles', []);
    im.registerTable('cars', ['id', 'make', 'model', 'year']);
    im.inherit('sedans', 'cars', []);
    
    const chain = im.getInheritanceChain('sedans');
    assert.deepEqual(chain, ['vehicles', 'cars', 'sedans']);
  });

  test('isDescendantOf', () => {
    im.inherit('cars', 'vehicles', []);
    im.registerTable('cars', ['id', 'make', 'model', 'year']);
    im.inherit('sedans', 'cars', []);
    
    assert.ok(im.isDescendantOf('sedans', 'vehicles'));
    assert.ok(im.isDescendantOf('sedans', 'cars'));
    assert.ok(!im.isDescendantOf('vehicles', 'sedans'));
  });

  test('noInherit removes relationship', () => {
    im.inherit('cars', 'vehicles', ['doors']);
    im.noInherit('cars');
    
    assert.equal(im.getParent('cars'), null);
    assert.deepEqual(im.getChildren('vehicles'), []);
    assert.deepEqual(im.getQueryTargets('vehicles'), ['vehicles']);
  });

  test('independent subtrees', () => {
    im.registerTable('animals', ['id', 'name', 'species']);
    im.inherit('dogs', 'animals', ['breed']);
    
    assert.deepEqual(im.getQueryTargets('vehicles'), ['vehicles']);
    assert.equal(im.getQueryTargets('animals').length, 2);
  });

  test('getTree returns hierarchy', () => {
    im.inherit('cars', 'vehicles', ['doors']);
    im.inherit('trucks', 'vehicles', ['payload']);
    
    const tree = im.getTree();
    const vehicleTree = tree.find(t => t.table === 'vehicles');
    assert.ok(vehicleTree);
    assert.equal(vehicleTree.children.length, 2);
  });

  test('case-insensitive', () => {
    im.inherit('Cars', 'Vehicles', ['doors']);
    assert.equal(im.getParent('CARS'), 'vehicles');
  });
});
