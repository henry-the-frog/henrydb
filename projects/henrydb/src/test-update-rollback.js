import { TransactionalDatabase } from './transactional-db.js';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';

const dir = mkdtempSync(join(tmpdir(), 'tdb-'));
const db = TransactionalDatabase.open(dir);

db.execute("CREATE TABLE accounts (id INT, name TEXT, balance INT)");
db.execute("INSERT INTO accounts VALUES (1, 'Alice', 100)");
db.execute("INSERT INTO accounts VALUES (2, 'Bob', 200)");

console.log('Before:', db.execute("SELECT * FROM accounts ORDER BY id").rows);

const s = db.session();
s.begin();
s.execute("UPDATE accounts SET balance = 50 WHERE id = 1");
console.log('During tx:', s.execute("SELECT * FROM accounts ORDER BY id").rows);
s.rollback();

console.log('After rollback:', db.execute("SELECT * FROM accounts ORDER BY id").rows);

const afterRows = db.execute("SELECT * FROM accounts ORDER BY id").rows;
console.log('Alice balance:', afterRows[0]?.balance, afterRows[0]?.balance === 100 ? '✅' : '❌');
console.log('Bob balance:', afterRows[1]?.balance, afterRows[1]?.balance === 200 ? '✅' : '❌');

rmSync(dir, { recursive: true });
