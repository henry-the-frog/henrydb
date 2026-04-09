// scram-auth.test.js
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { hashPassword, SCRAMServer, SCRAMClient } from './scram-auth.js';

describe('SCRAM-SHA-256', () => {
  test('hashPassword generates salt and keys', () => {
    const entry = hashPassword('secret');
    assert.ok(entry.salt);
    assert.ok(entry.storedKey);
    assert.ok(entry.serverKey);
    assert.equal(entry.iterations, 4096);
  });

  test('different passwords produce different hashes', () => {
    const h1 = hashPassword('password1');
    const h2 = hashPassword('password2');
    assert.notEqual(h1.storedKey, h2.storedKey);
  });

  test('full SCRAM handshake succeeds', () => {
    const pwEntry = hashPassword('mypassword');
    
    const server = new SCRAMServer((username) => {
      if (username === 'alice') return pwEntry;
      return null;
    });

    const client = new SCRAMClient('alice', 'mypassword');

    // Step 1: Client sends first message
    const clientFirst = client.clientFirst();
    assert.ok(clientFirst.startsWith('n,,'));

    // Step 2: Server processes and responds
    const serverFirst = server.handleClientFirst('session1', clientFirst);
    assert.ok(serverFirst.includes('r='));
    assert.ok(serverFirst.includes('s='));
    assert.ok(serverFirst.includes('i='));

    // Step 3: Client processes and sends final
    const clientFinal = client.clientFinal(serverFirst);
    assert.ok(clientFinal.includes('p='));

    // Step 4: Server verifies and responds
    const result = server.handleClientFinal('session1', clientFinal);
    assert.ok(result.authenticated);
    assert.equal(result.username, 'alice');

    // Step 5: Client verifies server
    const verified = client.verifyServer(result.serverFinalMessage);
    assert.ok(verified);
  });

  test('wrong password fails authentication', () => {
    const pwEntry = hashPassword('correct');
    
    const server = new SCRAMServer((username) => {
      if (username === 'alice') return pwEntry;
      return null;
    });

    const client = new SCRAMClient('alice', 'wrong');
    const clientFirst = client.clientFirst();
    const serverFirst = server.handleClientFirst('session2', clientFirst);
    const clientFinal = client.clientFinal(serverFirst);

    assert.throws(() => {
      server.handleClientFinal('session2', clientFinal);
    }, /failed/);
  });

  test('unknown user fails', () => {
    const server = new SCRAMServer(() => null);
    const client = new SCRAMClient('unknown', 'pass');
    const clientFirst = client.clientFirst();

    assert.throws(() => {
      server.handleClientFirst('s1', clientFirst);
    }, /not found/);
  });

  test('multiple concurrent sessions', () => {
    const pwAlice = hashPassword('alice_pass');
    const pwBob = hashPassword('bob_pass');
    
    const server = new SCRAMServer((username) => {
      if (username === 'alice') return pwAlice;
      if (username === 'bob') return pwBob;
      return null;
    });

    const alice = new SCRAMClient('alice', 'alice_pass');
    const bob = new SCRAMClient('bob', 'bob_pass');

    const aliceFirst = alice.clientFirst();
    const bobFirst = bob.clientFirst();

    const aliceServerFirst = server.handleClientFirst('s_alice', aliceFirst);
    const bobServerFirst = server.handleClientFirst('s_bob', bobFirst);

    const aliceFinal = alice.clientFinal(aliceServerFirst);
    const bobFinal = bob.clientFinal(bobServerFirst);

    const aliceResult = server.handleClientFinal('s_alice', aliceFinal);
    const bobResult = server.handleClientFinal('s_bob', bobFinal);

    assert.ok(aliceResult.authenticated);
    assert.ok(bobResult.authenticated);
    assert.equal(aliceResult.username, 'alice');
    assert.equal(bobResult.username, 'bob');
  });

  test('nonce is unique per client', () => {
    const c1 = new SCRAMClient('user', 'pass');
    const c2 = new SCRAMClient('user', 'pass');
    assert.notEqual(c1.clientFirst(), c2.clientFirst());
  });
});
