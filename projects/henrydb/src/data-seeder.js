// data-seeder.js — Realistic test data generator for HenryDB
// Generates deterministic fake data for common patterns.

const FIRST_NAMES = ['Alice','Bob','Charlie','Diana','Eve','Frank','Grace','Hank','Ivy','Jack',
  'Kate','Leo','Maya','Noah','Olivia','Pete','Quinn','Ruby','Sam','Tara','Uma','Vic','Wendy','Xander','Yuki','Zara'];
const LAST_NAMES = ['Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez',
  'Hernandez','Lopez','Gonzalez','Wilson','Anderson','Thomas','Taylor','Moore','Jackson','Martin'];
const CITIES = ['New York','Los Angeles','Chicago','Houston','Phoenix','Philadelphia','San Antonio','San Diego',
  'Dallas','San Jose','Austin','Jacksonville','Fort Worth','Columbus','Indianapolis','Charlotte'];
const DOMAINS = ['gmail.com','yahoo.com','outlook.com','example.com','company.org','mail.io'];
const CATEGORIES = ['Electronics','Books','Clothing','Home','Sports','Food','Toys','Music','Health','Travel'];
const COLORS = ['Red','Blue','Green','Yellow','Purple','Orange','Black','White','Pink','Gray'];
const ADJECTIVES = ['Amazing','Great','Premium','Basic','Classic','Modern','Vintage','Deluxe','Super','Ultra'];
const NOUNS = ['Widget','Gadget','Device','Tool','Kit','Set','Pack','Bundle','Collection','Series'];
const STATUSES = ['active','inactive','pending','suspended','deleted'];
const LOREM = 'Lorem ipsum dolor sit amet consectetur adipiscing elit sed do eiusmod tempor incididunt ut labore et dolore magna aliqua'.split(' ');

/**
 * Seeded pseudo-random number generator (LCG).
 */
class PRNG {
  constructor(seed = 42) {
    this.state = seed;
  }
  next() {
    this.state = (this.state * 1664525 + 1013904223) & 0x7FFFFFFF;
    return this.state / 0x7FFFFFFF;
  }
  int(min, max) { return Math.floor(this.next() * (max - min + 1)) + min; }
  pick(arr) { return arr[this.int(0, arr.length - 1)]; }
  float(min, max, decimals = 2) { return +((this.next() * (max - min) + min).toFixed(decimals)); }
  bool(chance = 0.5) { return this.next() < chance; }
  date(startYear = 2020, endYear = 2026) {
    const y = this.int(startYear, endYear);
    const m = String(this.int(1, 12)).padStart(2, '0');
    const d = String(this.int(1, 28)).padStart(2, '0');
    return `${y}-${m}-${d}`;
  }
}

/**
 * DataSeeder — generate realistic test data for common schemas.
 */
export class DataSeeder {
  constructor(db, seed = 42) {
    this.db = db;
    this.rng = new PRNG(seed);
  }

  /**
   * Generate a person record.
   */
  person() {
    const first = this.rng.pick(FIRST_NAMES);
    const last = this.rng.pick(LAST_NAMES);
    return {
      name: `${first} ${last}`,
      email: `${first.toLowerCase()}.${last.toLowerCase()}@${this.rng.pick(DOMAINS)}`,
      age: this.rng.int(18, 75),
      city: this.rng.pick(CITIES),
      active: this.rng.bool(0.8),
      joined: this.rng.date(2020, 2026),
    };
  }

  /**
   * Generate a product record.
   */
  product() {
    return {
      name: `${this.rng.pick(ADJECTIVES)} ${this.rng.pick(NOUNS)}`,
      price: this.rng.float(1, 999),
      category: this.rng.pick(CATEGORIES),
      color: this.rng.pick(COLORS),
      stock: this.rng.int(0, 500),
      rating: this.rng.float(1, 5, 1),
    };
  }

  /**
   * Generate lorem ipsum text.
   */
  text(words = 10) {
    const result = [];
    for (let i = 0; i < words; i++) result.push(this.rng.pick(LOREM));
    return result.join(' ');
  }

  /**
   * Seed a users table with n records.
   */
  seedUsers(n = 100) {
    this.db.execute('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, city TEXT, active INTEGER, joined TEXT)');
    for (let i = 1; i <= n; i++) {
      const p = this.person();
      this.db.execute(`INSERT INTO users VALUES (${i}, '${p.name}', '${p.email}', ${p.age}, '${p.city}', ${p.active ? 1 : 0}, '${p.joined}')`);
    }
    this.db.execute('CREATE INDEX idx_users_city ON users(city)');
    this.db.execute('CREATE INDEX idx_users_age ON users(age)');
    return n;
  }

  /**
   * Seed a products table with n records.
   */
  seedProducts(n = 50) {
    this.db.execute('CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL, category TEXT, color TEXT, stock INTEGER, rating REAL)');
    for (let i = 1; i <= n; i++) {
      const p = this.product();
      this.db.execute(`INSERT INTO products VALUES (${i}, '${p.name}', ${p.price}, '${p.category}', '${p.color}', ${p.stock}, ${p.rating})`);
    }
    this.db.execute('CREATE INDEX idx_products_category ON products(category)');
    this.db.execute('CREATE INDEX idx_products_price ON products(price)');
    return n;
  }

  /**
   * Seed an orders table that references users and products.
   */
  seedOrders(n = 200, maxUserId = 100, maxProductId = 50) {
    this.db.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, product_id INTEGER, quantity INTEGER, total REAL, status TEXT, ordered_at TEXT)');
    for (let i = 1; i <= n; i++) {
      const userId = this.rng.int(1, maxUserId);
      const productId = this.rng.int(1, maxProductId);
      const qty = this.rng.int(1, 10);
      const total = this.rng.float(5, 5000);
      const status = this.rng.pick(['completed', 'pending', 'shipped', 'cancelled', 'returned']);
      const date = this.rng.date(2024, 2026);
      this.db.execute(`INSERT INTO orders VALUES (${i}, ${userId}, ${productId}, ${qty}, ${total}, '${status}', '${date}')`);
    }
    this.db.execute('CREATE INDEX idx_orders_user ON orders(user_id)');
    this.db.execute('CREATE INDEX idx_orders_product ON orders(product_id)');
    this.db.execute('CREATE INDEX idx_orders_status ON orders(status)');
    return n;
  }

  /**
   * Seed a complete e-commerce schema.
   */
  seedEcommerce(options = {}) {
    const users = this.seedUsers(options.users || 100);
    const products = this.seedProducts(options.products || 50);
    const orders = this.seedOrders(options.orders || 200, users, products);
    return { users, products, orders };
  }

  /**
   * Seed a generic table with random data.
   * @param {string} table - Table name
   * @param {Object} schema - Column definitions { name: 'text', age: 'int', score: 'real' }
   * @param {number} n - Number of rows
   */
  seedTable(table, schema, n = 100) {
    const cols = Object.entries(schema);
    const colDefs = cols.map(([name, type]) => {
      const sqlType = type === 'int' ? 'INTEGER' : type === 'real' ? 'REAL' : 'TEXT';
      return `${name} ${sqlType}`;
    }).join(', ');
    
    this.db.execute(`CREATE TABLE ${table} (id INTEGER PRIMARY KEY, ${colDefs})`);
    
    for (let i = 1; i <= n; i++) {
      const values = cols.map(([name, type]) => {
        if (type === 'int') return this.rng.int(0, 1000);
        if (type === 'real') return this.rng.float(0, 1000);
        if (type === 'name') return `'${this.rng.pick(FIRST_NAMES)} ${this.rng.pick(LAST_NAMES)}'`;
        if (type === 'city') return `'${this.rng.pick(CITIES)}'`;
        if (type === 'category') return `'${this.rng.pick(CATEGORIES)}'`;
        if (type === 'date') return `'${this.rng.date()}'`;
        if (type === 'bool') return this.rng.bool() ? 1 : 0;
        return `'${this.text(3)}'`;
      }).join(', ');
      this.db.execute(`INSERT INTO ${table} VALUES (${i}, ${values})`);
    }
    return n;
  }
}
