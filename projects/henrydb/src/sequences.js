// sequences.js — PostgreSQL-compatible sequence generator for HenryDB
// CREATE SEQUENCE, nextval, currval, setval, SERIAL type support.

/**
 * Sequence — an auto-incrementing number generator.
 */
class Sequence {
  constructor(name, options = {}) {
    this.name = name;
    this.start = options.start ?? 1;
    this.increment = options.increment ?? 1;
    this.minValue = options.minValue ?? (this.increment > 0 ? 1 : -(2 ** 53));
    this.maxValue = options.maxValue ?? (this.increment > 0 ? (2 ** 53) : -1);
    this.cycle = options.cycle || false;
    this.cache = options.cache || 1;
    this.currentValue = this.start - this.increment; // Before first nextval
    this.isCalled = false;
    this.ownedBy = options.ownedBy || null; // table.column
    this.createdAt = Date.now();
  }

  /**
   * Get next value.
   */
  nextval() {
    const next = this.currentValue + this.increment;

    if (this.increment > 0 && next > this.maxValue) {
      if (this.cycle) {
        this.currentValue = this.minValue;
      } else {
        throw new Error(`Sequence '${this.name}' reached maximum value (${this.maxValue})`);
      }
    } else if (this.increment < 0 && next < this.minValue) {
      if (this.cycle) {
        this.currentValue = this.maxValue;
      } else {
        throw new Error(`Sequence '${this.name}' reached minimum value (${this.minValue})`);
      }
    } else {
      this.currentValue = next;
    }

    this.isCalled = true;
    return this.currentValue;
  }

  /**
   * Get current value (error if nextval not called).
   */
  currval() {
    if (!this.isCalled) {
      throw new Error(`currval of sequence '${this.name}' has not been called in this session`);
    }
    return this.currentValue;
  }

  /**
   * Set the current value.
   */
  setval(value, isCalled = true) {
    if (value < this.minValue || value > this.maxValue) {
      throw new Error(`Value ${value} is outside sequence range [${this.minValue}, ${this.maxValue}]`);
    }
    this.currentValue = value;
    this.isCalled = isCalled;
    return value;
  }

  /**
   * Restart the sequence.
   */
  restart(startWith = null) {
    this.currentValue = (startWith ?? this.start) - this.increment;
    this.isCalled = false;
  }

  getInfo() {
    return {
      name: this.name,
      currentValue: this.currentValue,
      start: this.start,
      increment: this.increment,
      minValue: this.minValue,
      maxValue: this.maxValue,
      cycle: this.cycle,
      isCalled: this.isCalled,
      ownedBy: this.ownedBy,
    };
  }
}

/**
 * SequenceManager — manages all sequences.
 */
export class SequenceManager {
  constructor() {
    this._sequences = new Map();
  }

  /**
   * CREATE SEQUENCE name [options].
   */
  create(name, options = {}) {
    const lowerName = name.toLowerCase();
    if (this._sequences.has(lowerName)) {
      if (options.ifNotExists) return this._sequences.get(lowerName).getInfo();
      throw new Error(`Sequence '${name}' already exists`);
    }
    const seq = new Sequence(lowerName, options);
    this._sequences.set(lowerName, seq);
    return seq.getInfo();
  }

  /**
   * DROP SEQUENCE name.
   */
  drop(name, ifExists = false) {
    const lowerName = name.toLowerCase();
    if (!this._sequences.has(lowerName)) {
      if (ifExists) return false;
      throw new Error(`Sequence '${name}' does not exist`);
    }
    this._sequences.delete(lowerName);
    return true;
  }

  nextval(name) {
    const seq = this._sequences.get(name.toLowerCase());
    if (!seq) throw new Error(`Sequence '${name}' does not exist`);
    return seq.nextval();
  }

  currval(name) {
    const seq = this._sequences.get(name.toLowerCase());
    if (!seq) throw new Error(`Sequence '${name}' does not exist`);
    return seq.currval();
  }

  setval(name, value, isCalled = true) {
    const seq = this._sequences.get(name.toLowerCase());
    if (!seq) throw new Error(`Sequence '${name}' does not exist`);
    return seq.setval(value, isCalled);
  }

  restart(name, startWith = null) {
    const seq = this._sequences.get(name.toLowerCase());
    if (!seq) throw new Error(`Sequence '${name}' does not exist`);
    seq.restart(startWith);
  }

  /**
   * Create a SERIAL-compatible sequence for a column.
   */
  createSerial(tableName, columnName) {
    const name = `${tableName}_${columnName}_seq`;
    return this.create(name, { start: 1, increment: 1, ownedBy: `${tableName}.${columnName}` });
  }

  has(name) {
    return this._sequences.has(name.toLowerCase());
  }

  list() {
    return [...this._sequences.values()].map(s => s.getInfo());
  }
}
