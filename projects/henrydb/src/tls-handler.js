// tls-handler.js — TLS/SSL support for HenryDB connections
// Handles SSL negotiation, certificate validation, and connection upgrade.

import tls from 'node:tls';
import crypto from 'node:crypto';

/**
 * TLSConfig — TLS configuration for the server.
 */
export class TLSConfig {
  constructor(options = {}) {
    this.mode = options.mode || 'prefer'; // disable, allow, prefer, require, verify-ca, verify-full
    this.cert = options.cert || null;
    this.key = options.key || null;
    this.ca = options.ca || null;
    this.ciphers = options.ciphers || null;
    this.minVersion = options.minVersion || 'TLSv1.2';
  }

  /**
   * Create TLS options for tls.createSecureContext.
   */
  getSecureContextOptions() {
    const opts = {};
    if (this.cert) opts.cert = this.cert;
    if (this.key) opts.key = this.key;
    if (this.ca) opts.ca = this.ca;
    if (this.ciphers) opts.ciphers = this.ciphers;
    opts.minVersion = this.minVersion;
    return opts;
  }

  /**
   * Check if SSL is required for a connection.
   */
  requiresSSL() {
    return ['require', 'verify-ca', 'verify-full'].includes(this.mode);
  }

  /**
   * Check if SSL should be offered.
   */
  offersSSL() {
    return this.mode !== 'disable';
  }
}

/**
 * Generate a self-signed certificate for testing/development.
 */
export function generateSelfSignedCert(options = {}) {
  const { privateKey, publicKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
  });

  // Create a simple self-signed cert (simplified — real certs need ASN.1 encoding)
  const cn = options.cn || 'localhost';
  const days = options.days || 365;

  return {
    key: privateKey,
    cert: publicKey, // In real use, this would be a proper X.509 cert
    cn,
    validDays: days,
    fingerprint: crypto.createHash('sha256').update(publicKey).digest('hex'),
    createdAt: new Date().toISOString(),
    expiresAt: new Date(Date.now() + days * 86400000).toISOString(),
  };
}

/**
 * SSLNegotiator — handles PostgreSQL SSL negotiation protocol.
 */
export class SSLNegotiator {
  constructor(tlsConfig) {
    this.config = tlsConfig;
    this._stats = {
      sslRequested: 0,
      sslAccepted: 0,
      sslRejected: 0,
      sslUpgraded: 0,
    };
  }

  /**
   * Handle an SSL request from a client.
   * PostgreSQL protocol: client sends 8-byte SSL request (length=8, code=80877103).
   * Server responds with 'S' (will upgrade) or 'N' (won't upgrade).
   */
  handleSSLRequest(socket) {
    this._stats.sslRequested++;

    if (!this.config.offersSSL() || !this.config.cert) {
      this._stats.sslRejected++;
      return { accept: false, response: Buffer.from('N') };
    }

    this._stats.sslAccepted++;
    return {
      accept: true,
      response: Buffer.from('S'),
      upgrade: () => this._upgradeToTLS(socket),
    };
  }

  /**
   * Check if a raw buffer is an SSL request.
   */
  isSSLRequest(data) {
    if (data.length < 8) return false;
    const length = data.readInt32BE(0);
    const code = data.readInt32BE(4);
    return length === 8 && code === 80877103;
  }

  /**
   * Upgrade a plain socket to TLS.
   */
  _upgradeToTLS(socket) {
    return new Promise((resolve, reject) => {
      try {
        const secureContext = tls.createSecureContext(this.config.getSecureContextOptions());
        const tlsSocket = new tls.TLSSocket(socket, {
          secureContext,
          isServer: true,
        });

        tlsSocket.on('secure', () => {
          this._stats.sslUpgraded++;
          resolve({
            socket: tlsSocket,
            protocol: tlsSocket.getProtocol(),
            cipher: tlsSocket.getCipher(),
            authorized: tlsSocket.authorized,
          });
        });

        tlsSocket.on('error', reject);
      } catch (err) {
        reject(err);
      }
    });
  }

  /**
   * Validate a client certificate (for verify-ca/verify-full modes).
   */
  validateClientCert(tlsSocket, expectedHostname = null) {
    if (this.config.mode === 'verify-full' && expectedHostname) {
      const cert = tlsSocket.getPeerCertificate();
      if (!cert || !cert.subject) {
        return { valid: false, reason: 'No client certificate provided' };
      }
      // Check CN or SAN
      const cn = cert.subject.CN;
      if (cn !== expectedHostname) {
        return { valid: false, reason: `Certificate CN '${cn}' does not match '${expectedHostname}'` };
      }
    }

    if (this.config.mode === 'verify-ca') {
      if (!tlsSocket.authorized) {
        return { valid: false, reason: 'Client certificate not authorized by CA' };
      }
    }

    return { valid: true };
  }

  getStats() {
    return { ...this._stats };
  }
}
