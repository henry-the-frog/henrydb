// data-generator.js — TPC-H style synthetic data generation
// Generates realistic relational data for benchmarks.

function randomInt(lo, hi) { return lo + Math.floor(Math.random() * (hi - lo + 1)); }
function randomChoice(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
function randomDate(yearLo, yearHi) {
  const y = randomInt(yearLo, yearHi);
  const m = String(randomInt(1, 12)).padStart(2, '0');
  const d = String(randomInt(1, 28)).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

const NATIONS = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan', 'China', 'Brazil', 'India', 'Australia'];
const SEGMENTS = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD'];
const PRIORITIES = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-NOT SPECIFIED', '5-LOW'];
const SHIP_MODES = ['AIR', 'MAIL', 'RAIL', 'SHIP', 'TRUCK', 'REG AIR', 'FOB'];

export class DataGenerator {
  /**
   * Generate customers.
   */
  static customers(n) {
    return Array.from({ length: n }, (_, i) => ({
      custkey: i + 1,
      name: `Customer#${String(i + 1).padStart(9, '0')}`,
      nation: randomChoice(NATIONS),
      segment: randomChoice(SEGMENTS),
      acctbal: (Math.random() * 20000 - 5000).toFixed(2) * 1,
      phone: `${randomInt(10, 34)}-${randomInt(100, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`,
    }));
  }

  /**
   * Generate orders.
   */
  static orders(n, numCustomers) {
    return Array.from({ length: n }, (_, i) => ({
      orderkey: i + 1,
      custkey: randomInt(1, numCustomers),
      orderstatus: randomChoice(['O', 'F', 'P']),
      totalprice: (Math.random() * 500000).toFixed(2) * 1,
      orderdate: randomDate(1992, 1998),
      orderpriority: randomChoice(PRIORITIES),
      clerk: `Clerk#${String(randomInt(1, 1000)).padStart(9, '0')}`,
    }));
  }

  /**
   * Generate line items (order details).
   */
  static lineItems(n, numOrders) {
    return Array.from({ length: n }, (_, i) => ({
      orderkey: randomInt(1, numOrders),
      linenumber: (i % 7) + 1,
      quantity: randomInt(1, 50),
      extendedprice: (Math.random() * 100000).toFixed(2) * 1,
      discount: (Math.random() * 0.1).toFixed(2) * 1,
      tax: (Math.random() * 0.08).toFixed(2) * 1,
      returnflag: randomChoice(['R', 'A', 'N']),
      linestatus: randomChoice(['O', 'F']),
      shipdate: randomDate(1992, 1998),
      shipmode: randomChoice(SHIP_MODES),
    }));
  }

  /**
   * Generate parts.
   */
  static parts(n) {
    const types = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO'];
    const containers = ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG', 'MED BOX', 'LG CASE', 'LG BOX'];
    return Array.from({ length: n }, (_, i) => ({
      partkey: i + 1,
      name: `Part#${i + 1}`,
      brand: `Brand#${randomInt(1, 5)}${randomInt(1, 5)}`,
      type: randomChoice(types),
      size: randomInt(1, 50),
      container: randomChoice(containers),
      retailprice: (900 + i / 10).toFixed(2) * 1,
    }));
  }

  /**
   * Generate suppliers.
   */
  static suppliers(n) {
    return Array.from({ length: n }, (_, i) => ({
      suppkey: i + 1,
      name: `Supplier#${String(i + 1).padStart(9, '0')}`,
      nation: randomChoice(NATIONS),
      acctbal: (Math.random() * 20000 - 5000).toFixed(2) * 1,
      phone: `${randomInt(10, 34)}-${randomInt(100, 999)}-${randomInt(100, 999)}-${randomInt(1000, 9999)}`,
    }));
  }
}
