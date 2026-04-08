// server-travel.test.js — Travel booking data model
import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import pg from 'pg';
import { HenryDBServer } from './server.js';

const { Client } = pg;
const PORT = 15586;

describe('Travel Booking', () => {
  let server;

  before(async () => {
    server = new HenryDBServer({ port: PORT });
    await server.start();

    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();
    
    await client.query('CREATE TABLE flights (id INTEGER, airline TEXT, departure TEXT, arrival TEXT, dep_time TEXT, arr_time TEXT, price REAL)');
    await client.query('CREATE TABLE hotels (id INTEGER, name TEXT, city TEXT, stars INTEGER, price_per_night REAL, rooms_available INTEGER)');
    await client.query('CREATE TABLE bookings (id INTEGER, traveler TEXT, flight_id INTEGER, hotel_id INTEGER, check_in TEXT, check_out TEXT, total REAL)');
    
    await client.query("INSERT INTO flights VALUES (1, 'United', 'DEN', 'SFO', '2026-04-15 08:00', '2026-04-15 10:00', 299.99)");
    await client.query("INSERT INTO flights VALUES (2, 'Delta', 'DEN', 'JFK', '2026-04-15 09:00', '2026-04-15 14:00', 399.99)");
    await client.query("INSERT INTO flights VALUES (3, 'Southwest', 'DEN', 'SFO', '2026-04-15 12:00', '2026-04-15 14:00', 199.99)");
    
    await client.query("INSERT INTO hotels VALUES (1, 'Grand Hotel', 'San Francisco', 4, 189.00, 15)");
    await client.query("INSERT INTO hotels VALUES (2, 'Budget Inn', 'San Francisco', 2, 79.00, 30)");
    await client.query("INSERT INTO hotels VALUES (3, 'Plaza NYC', 'New York', 5, 450.00, 5)");
    
    await client.query("INSERT INTO bookings VALUES (1, 'Alice', 1, 1, '2026-04-15', '2026-04-18', 866.99)");
    await client.query("INSERT INTO bookings VALUES (2, 'Bob', 2, 3, '2026-04-15', '2026-04-17', 1299.99)");
    await client.query("INSERT INTO bookings VALUES (3, 'Charlie', 3, 2, '2026-04-15', '2026-04-20', 594.99)");
    
    await client.end();
  });

  after(async () => {
    await server.stop();
  });

  it('search flights by route', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT * FROM flights WHERE departure = 'DEN' AND arrival = 'SFO' ORDER BY price");
    assert.strictEqual(result.rows.length, 2);
    assert.strictEqual(result.rows[0].airline, 'Southwest'); // Cheapest

    await client.end();
  });

  it('hotel availability by city', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query("SELECT name, stars, price_per_night, rooms_available FROM hotels WHERE city = 'San Francisco' ORDER BY stars DESC");
    assert.strictEqual(result.rows.length, 2);

    await client.end();
  });

  it('booking details with flight and hotel', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT b.traveler, f.airline, f.departure, f.arrival, h.name AS hotel, b.total FROM bookings b JOIN flights f ON b.flight_id = f.id JOIN hotels h ON b.hotel_id = h.id ORDER BY b.total DESC'
    );
    assert.strictEqual(result.rows.length, 3);

    await client.end();
  });

  it('revenue by airline', async () => {
    const client = new Client({ host: '127.0.0.1', port: PORT, user: 'test', database: 'test' });
    await client.connect();

    const result = await client.query(
      'SELECT f.airline, COUNT(b.id) AS bookings, SUM(f.price) AS flight_rev FROM bookings b JOIN flights f ON b.flight_id = f.id GROUP BY f.airline ORDER BY flight_rev DESC'
    );
    assert.ok(result.rows.length >= 2);

    await client.end();
  });
});
