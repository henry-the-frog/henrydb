// scram-auth.js — SCRAM-SHA-256 authentication for HenryDB
// PostgreSQL-compatible SASL authentication mechanism.

import crypto from 'node:crypto';

/**
 * Generate a SCRAM-SHA-256 salted password.
 */
export function scramSHA256(password, salt, iterations = 4096) {
  const saltedPassword = crypto.pbkdf2Sync(password, salt, iterations, 32, 'sha256');
  const clientKey = crypto.createHmac('sha256', saltedPassword).update('Client Key').digest();
  const storedKey = crypto.createHash('sha256').update(clientKey).digest();
  const serverKey = crypto.createHmac('sha256', saltedPassword).update('Server Key').digest();

  return {
    salt: salt.toString('base64'),
    iterations,
    storedKey: storedKey.toString('base64'),
    serverKey: serverKey.toString('base64'),
  };
}

/**
 * Create a new password entry for storage.
 */
export function hashPassword(password) {
  const salt = crypto.randomBytes(16);
  return scramSHA256(password, salt);
}

/**
 * SCRAMServer — server-side SCRAM-SHA-256 authentication.
 */
export class SCRAMServer {
  constructor(userLookup) {
    this.userLookup = userLookup; // (username) => { storedKey, serverKey, salt, iterations }
    this._sessions = new Map();
  }

  /**
   * Process client-first message.
   * Returns server-first message.
   */
  handleClientFirst(sessionId, clientFirstMessage) {
    // Parse: n,,n=username,r=client-nonce
    const parts = clientFirstMessage.split(',');
    const gs2Header = parts.slice(0, 2).join(',') + ',';
    const clientFirstBare = parts.slice(2).join(',');
    
    let username = null;
    let clientNonce = null;
    
    for (const part of parts.slice(2)) {
      if (part.startsWith('n=')) username = part.substring(2);
      if (part.startsWith('r=')) clientNonce = part.substring(2);
    }

    if (!username || !clientNonce) {
      throw new Error('Invalid client-first message');
    }

    const user = this.userLookup(username);
    if (!user) {
      throw new Error(`User '${username}' not found`);
    }

    const serverNonce = clientNonce + crypto.randomBytes(18).toString('base64');
    const salt = user.salt;
    const iterations = user.iterations;

    const serverFirstMessage = `r=${serverNonce},s=${salt},i=${iterations}`;

    this._sessions.set(sessionId, {
      username,
      clientNonce,
      serverNonce,
      salt,
      iterations,
      storedKey: user.storedKey,
      serverKey: user.serverKey,
      gs2Header,
      clientFirstBare,
      serverFirstMessage,
    });

    return serverFirstMessage;
  }

  /**
   * Process client-final message.
   * Returns server-final message if auth succeeds.
   */
  handleClientFinal(sessionId, clientFinalMessage) {
    const session = this._sessions.get(sessionId);
    if (!session) throw new Error('No active SCRAM session');

    // Parse: c=base64(gs2header),r=combined-nonce,p=client-proof
    const parts = clientFinalMessage.split(',');
    let channelBinding = null;
    let nonce = null;
    let clientProof = null;

    for (const part of parts) {
      if (part.startsWith('c=')) channelBinding = part.substring(2);
      if (part.startsWith('r=')) nonce = part.substring(2);
      if (part.startsWith('p=')) clientProof = part.substring(2);
    }

    // Verify nonce
    if (nonce !== session.serverNonce) {
      this._sessions.delete(sessionId);
      throw new Error('SCRAM authentication failed: nonce mismatch');
    }

    // Compute auth message
    const clientFinalWithoutProof = parts.filter(p => !p.startsWith('p=')).join(',');
    const authMessage = `${session.clientFirstBare},${session.serverFirstMessage},${clientFinalWithoutProof}`;

    // Verify client proof
    const storedKey = Buffer.from(session.storedKey, 'base64');
    const clientSignature = crypto.createHmac('sha256', storedKey).update(authMessage).digest();
    const clientProofBuf = Buffer.from(clientProof, 'base64');
    
    // Recover clientKey = clientProof XOR clientSignature
    const recoveredClientKey = Buffer.alloc(32);
    for (let i = 0; i < 32; i++) {
      recoveredClientKey[i] = clientProofBuf[i] ^ clientSignature[i];
    }

    // Verify: H(recoveredClientKey) should equal storedKey
    const recoveredStoredKey = crypto.createHash('sha256').update(recoveredClientKey).digest();
    if (!recoveredStoredKey.equals(storedKey)) {
      this._sessions.delete(sessionId);
      throw new Error('SCRAM authentication failed: invalid proof');
    }

    // Compute server signature
    const serverKey = Buffer.from(session.serverKey, 'base64');
    const serverSignature = crypto.createHmac('sha256', serverKey).update(authMessage).digest();

    this._sessions.delete(sessionId);
    return {
      serverFinalMessage: `v=${serverSignature.toString('base64')}`,
      username: session.username,
      authenticated: true,
    };
  }
}

/**
 * SCRAMClient — client-side SCRAM-SHA-256 authentication.
 */
export class SCRAMClient {
  constructor(username, password) {
    this.username = username;
    this.password = password;
    this._nonce = crypto.randomBytes(18).toString('base64');
    this._state = {};
  }

  /**
   * Generate client-first message.
   */
  clientFirst() {
    const bare = `n=${this.username},r=${this._nonce}`;
    this._state.clientFirstBare = bare;
    return `n,,${bare}`;
  }

  /**
   * Process server-first message and generate client-final.
   */
  clientFinal(serverFirstMessage) {
    this._state.serverFirstMessage = serverFirstMessage;
    
    // Parse server-first: r=combined-nonce,s=salt,i=iterations
    const parts = serverFirstMessage.split(',');
    let combinedNonce = null;
    let salt = null;
    let iterations = null;

    for (const part of parts) {
      if (part.startsWith('r=')) combinedNonce = part.substring(2);
      if (part.startsWith('s=')) salt = part.substring(2);
      if (part.startsWith('i=')) iterations = parseInt(part.substring(2));
    }

    // Verify nonce prefix
    if (!combinedNonce.startsWith(this._nonce)) {
      throw new Error('SCRAM: invalid server nonce');
    }

    // Compute salted password
    const saltBuf = Buffer.from(salt, 'base64');
    const saltedPassword = crypto.pbkdf2Sync(this.password, saltBuf, iterations, 32, 'sha256');
    const clientKey = crypto.createHmac('sha256', saltedPassword).update('Client Key').digest();
    const storedKey = crypto.createHash('sha256').update(clientKey).digest();

    const channelBinding = Buffer.from('n,,').toString('base64');
    const clientFinalWithoutProof = `c=${channelBinding},r=${combinedNonce}`;
    const authMessage = `${this._state.clientFirstBare},${serverFirstMessage},${clientFinalWithoutProof}`;

    const clientSignature = crypto.createHmac('sha256', storedKey).update(authMessage).digest();
    
    // clientProof = clientKey XOR clientSignature
    const clientProof = Buffer.alloc(32);
    for (let i = 0; i < 32; i++) {
      clientProof[i] = clientKey[i] ^ clientSignature[i];
    }

    // Store server key for verifying server
    this._state.serverKey = crypto.createHmac('sha256', saltedPassword).update('Server Key').digest();
    this._state.authMessage = authMessage;

    return `${clientFinalWithoutProof},p=${clientProof.toString('base64')}`;
  }

  /**
   * Verify server-final message.
   */
  verifyServer(serverFinalMessage) {
    const match = serverFinalMessage.match(/v=(.+)/);
    if (!match) return false;

    const serverSignature = crypto.createHmac('sha256', this._state.serverKey)
      .update(this._state.authMessage).digest();
    
    return serverSignature.toString('base64') === match[1];
  }
}
