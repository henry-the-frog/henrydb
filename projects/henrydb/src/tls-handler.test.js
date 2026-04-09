// tls-handler.test.js
import { describe, test } from 'node:test';
import assert from 'node:assert/strict';
import { TLSConfig, SSLNegotiator, generateSelfSignedCert } from './tls-handler.js';

describe('TLSConfig', () => {
  test('default mode is prefer', () => {
    const cfg = new TLSConfig();
    assert.equal(cfg.mode, 'prefer');
    assert.ok(cfg.offersSSL());
    assert.ok(!cfg.requiresSSL());
  });

  test('disable mode', () => {
    const cfg = new TLSConfig({ mode: 'disable' });
    assert.ok(!cfg.offersSSL());
    assert.ok(!cfg.requiresSSL());
  });

  test('require mode', () => {
    const cfg = new TLSConfig({ mode: 'require' });
    assert.ok(cfg.offersSSL());
    assert.ok(cfg.requiresSSL());
  });

  test('verify-full mode', () => {
    const cfg = new TLSConfig({ mode: 'verify-full' });
    assert.ok(cfg.requiresSSL());
  });

  test('secure context options', () => {
    const cfg = new TLSConfig({
      cert: 'cert-data',
      key: 'key-data',
      ca: 'ca-data',
      minVersion: 'TLSv1.3',
    });
    const opts = cfg.getSecureContextOptions();
    assert.equal(opts.cert, 'cert-data');
    assert.equal(opts.key, 'key-data');
    assert.equal(opts.minVersion, 'TLSv1.3');
  });
});

describe('SSLNegotiator', () => {
  test('rejects SSL when disabled', () => {
    const neg = new SSLNegotiator(new TLSConfig({ mode: 'disable' }));
    const result = neg.handleSSLRequest(null);
    assert.ok(!result.accept);
    assert.deepEqual(result.response, Buffer.from('N'));
  });

  test('rejects SSL when no cert', () => {
    const neg = new SSLNegotiator(new TLSConfig({ mode: 'prefer' }));
    const result = neg.handleSSLRequest(null);
    assert.ok(!result.accept);
  });

  test('accepts SSL when cert available', () => {
    const cert = generateSelfSignedCert();
    const neg = new SSLNegotiator(new TLSConfig({ mode: 'prefer', cert: cert.cert, key: cert.key }));
    const result = neg.handleSSLRequest(null);
    assert.ok(result.accept);
    assert.deepEqual(result.response, Buffer.from('S'));
  });

  test('isSSLRequest detects SSL request packet', () => {
    const neg = new SSLNegotiator(new TLSConfig());
    const buf = Buffer.alloc(8);
    buf.writeInt32BE(8, 0);
    buf.writeInt32BE(80877103, 4);
    assert.ok(neg.isSSLRequest(buf));
  });

  test('isSSLRequest rejects non-SSL', () => {
    const neg = new SSLNegotiator(new TLSConfig());
    const buf = Buffer.alloc(8);
    buf.writeInt32BE(8, 0);
    buf.writeInt32BE(196608, 4); // v3.0 startup
    assert.ok(!neg.isSSLRequest(buf));
  });

  test('stats tracking', () => {
    const neg = new SSLNegotiator(new TLSConfig({ mode: 'disable' }));
    neg.handleSSLRequest(null);
    neg.handleSSLRequest(null);
    
    const stats = neg.getStats();
    assert.equal(stats.sslRequested, 2);
    assert.equal(stats.sslRejected, 2);
  });
});

describe('generateSelfSignedCert', () => {
  test('generates key pair', () => {
    const cert = generateSelfSignedCert();
    assert.ok(cert.key);
    assert.ok(cert.cert);
    assert.ok(cert.key.includes('PRIVATE KEY'));
  });

  test('custom CN', () => {
    const cert = generateSelfSignedCert({ cn: 'mydb.example.com' });
    assert.equal(cert.cn, 'mydb.example.com');
  });

  test('fingerprint is hex string', () => {
    const cert = generateSelfSignedCert();
    assert.ok(/^[a-f0-9]+$/.test(cert.fingerprint));
  });

  test('expiration dates', () => {
    const cert = generateSelfSignedCert({ days: 30 });
    assert.ok(cert.createdAt);
    assert.ok(cert.expiresAt);
    assert.ok(new Date(cert.expiresAt) > new Date(cert.createdAt));
  });
});
