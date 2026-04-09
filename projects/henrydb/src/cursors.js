// cursors.js — Server-side cursors for HenryDB
// DECLARE cursor, FETCH FORWARD/BACKWARD/FIRST/LAST/ABSOLUTE/RELATIVE, CLOSE, SCROLL
// PostgreSQL-compatible cursor implementation.

/**
 * Cursor — a server-side cursor for iterating through query results.
 */
class Cursor {
  constructor(name, rows, options = {}) {
    this.name = name;
    this.rows = rows;
    this.position = 0; // Before first row (0-based internal, but logical position is "before first")
    this.isScrollable = options.scroll || false;
    this.holdable = options.hold || false; // WITH HOLD: survives transaction end
    this.isOpen = true;
    this.direction = 'forward'; // Last fetch direction
    this.fetchCount = 0;
    this.createdAt = Date.now();
  }

  /**
   * Fetch rows from the cursor.
   * @param {string} direction - NEXT, PRIOR, FIRST, LAST, ABSOLUTE n, RELATIVE n, FORWARD n, BACKWARD n, ALL
   * @param {number} count - number of rows to fetch (for FORWARD n / BACKWARD n)
   */
  fetch(direction = 'NEXT', count = 1) {
    if (!this.isOpen) throw new Error(`Cursor '${this.name}' is not open`);

    const upperDir = direction.toUpperCase();
    this.fetchCount++;

    switch (upperDir) {
      case 'NEXT':
      case 'FORWARD':
        return this._fetchForward(count);

      case 'PRIOR':
      case 'BACKWARD':
        if (!this.isScrollable) throw new Error(`Cursor '${this.name}' is not scrollable`);
        return this._fetchBackward(count);

      case 'FIRST':
        if (!this.isScrollable) throw new Error(`Cursor '${this.name}' is not scrollable`);
        this.position = 0;
        return this._fetchForward(1);

      case 'LAST':
        if (!this.isScrollable) throw new Error(`Cursor '${this.name}' is not scrollable`);
        this.position = this.rows.length - 1;
        return { rows: [this.rows[this.position]], count: 1 };

      case 'ABSOLUTE':
        if (!this.isScrollable) throw new Error(`Cursor '${this.name}' is not scrollable`);
        return this._fetchAbsolute(count);

      case 'RELATIVE':
        if (!this.isScrollable) throw new Error(`Cursor '${this.name}' is not scrollable`);
        return this._fetchRelative(count);

      case 'ALL':
        return this._fetchForward(this.rows.length - this.position);

      default:
        throw new Error(`Unknown fetch direction: ${direction}`);
    }
  }

  _fetchForward(n) {
    const result = [];
    for (let i = 0; i < n && this.position < this.rows.length; i++) {
      result.push(this.rows[this.position]);
      this.position++;
    }
    this.direction = 'forward';
    return { rows: result, count: result.length };
  }

  _fetchBackward(n) {
    const result = [];
    for (let i = 0; i < n && this.position > 0; i++) {
      this.position--;
      result.push(this.rows[this.position]);
    }
    this.direction = 'backward';
    return { rows: result, count: result.length };
  }

  _fetchAbsolute(n) {
    if (n > 0 && n <= this.rows.length) {
      this.position = n - 1;
      return { rows: [this.rows[this.position]], count: 1 };
    }
    if (n < 0) {
      const idx = this.rows.length + n;
      if (idx >= 0) {
        this.position = idx;
        return { rows: [this.rows[this.position]], count: 1 };
      }
    }
    return { rows: [], count: 0 };
  }

  _fetchRelative(n) {
    const newPos = this.position + n;
    if (newPos >= 0 && newPos < this.rows.length) {
      this.position = newPos;
      return { rows: [this.rows[this.position]], count: 1 };
    }
    return { rows: [], count: 0 };
  }

  close() {
    this.isOpen = false;
  }

  getInfo() {
    return {
      name: this.name,
      isOpen: this.isOpen,
      isScrollable: this.isScrollable,
      holdable: this.holdable,
      position: this.position,
      totalRows: this.rows.length,
      fetchCount: this.fetchCount,
      atStart: this.position === 0,
      atEnd: this.position >= this.rows.length,
    };
  }
}

/**
 * CursorManager — manages named cursors for a session.
 */
export class CursorManager {
  constructor() {
    this._cursors = new Map(); // name → Cursor
  }

  /**
   * DECLARE cursor FOR query.
   */
  declare(name, rows, options = {}) {
    if (this._cursors.has(name.toLowerCase())) {
      throw new Error(`Cursor '${name}' already exists`);
    }
    const cursor = new Cursor(name.toLowerCase(), rows, options);
    this._cursors.set(name.toLowerCase(), cursor);
    return cursor.getInfo();
  }

  /**
   * FETCH from cursor.
   */
  fetch(name, direction = 'NEXT', count = 1) {
    const cursor = this._cursors.get(name.toLowerCase());
    if (!cursor) throw new Error(`Cursor '${name}' does not exist`);
    return cursor.fetch(direction, count);
  }

  /**
   * MOVE cursor (like FETCH but doesn't return rows).
   */
  move(name, direction = 'NEXT', count = 1) {
    const cursor = this._cursors.get(name.toLowerCase());
    if (!cursor) throw new Error(`Cursor '${name}' does not exist`);
    cursor.fetch(direction, count);
    return { moved: true };
  }

  /**
   * CLOSE cursor.
   */
  close(name) {
    const cursor = this._cursors.get(name.toLowerCase());
    if (!cursor) throw new Error(`Cursor '${name}' does not exist`);
    cursor.close();
    this._cursors.delete(name.toLowerCase());
    return true;
  }

  /**
   * Close all cursors (session end).
   */
  closeAll() {
    const count = this._cursors.size;
    for (const cursor of this._cursors.values()) {
      cursor.close();
    }
    this._cursors.clear();
    return count;
  }

  /**
   * Close non-holdable cursors (transaction end).
   */
  closeNonHoldable() {
    let closed = 0;
    for (const [name, cursor] of [...this._cursors]) {
      if (!cursor.holdable) {
        cursor.close();
        this._cursors.delete(name);
        closed++;
      }
    }
    return closed;
  }

  has(name) {
    return this._cursors.has(name.toLowerCase());
  }

  getInfo(name) {
    const cursor = this._cursors.get(name.toLowerCase());
    if (!cursor) throw new Error(`Cursor '${name}' does not exist`);
    return cursor.getInfo();
  }

  listCursors() {
    return [...this._cursors.values()].map(c => c.getInfo());
  }
}
