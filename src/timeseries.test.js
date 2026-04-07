// timeseries.test.js
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { TimeSeriesStore } from './timeseries.js';

describe('TimeSeriesStore', () => {
  it('write and query by time range', () => {
    const ts = new TimeSeriesStore();
    ts.write('cpu', 1000, 0.5);
    ts.write('cpu', 2000, 0.7);
    ts.write('cpu', 3000, 0.3);
    
    const result = ts.query('cpu', 1000, 2000);
    assert.equal(result.length, 2);
  });

  it('tag-based filtering', () => {
    const ts = new TimeSeriesStore();
    ts.write('temp', 1000, 20, { room: 'bedroom' });
    ts.write('temp', 1000, 22, { room: 'kitchen' });
    ts.write('temp', 2000, 21, { room: 'bedroom' });
    
    const result = ts.query('temp', 0, 3000, { room: 'bedroom' });
    assert.equal(result.length, 2);
  });

  it('downsample with avg', () => {
    const ts = new TimeSeriesStore();
    for (let i = 0; i < 100; i++) {
      ts.write('metric', i * 100, i); // 0, 1, 2, ..., 99
    }
    
    // 10 buckets of 1000ms each
    const result = ts.downsample('metric', 0, 9999, 1000, 'avg');
    assert.equal(result.length, 10);
    assert.ok(result[0].count === 10);
  });

  it('downsample with sum', () => {
    const ts = new TimeSeriesStore();
    ts.write('sales', 1000, 100);
    ts.write('sales', 1500, 200);
    ts.write('sales', 2000, 150);
    ts.write('sales', 2500, 250);
    
    const result = ts.downsample('sales', 1000, 3000, 1000, 'sum');
    assert.equal(result[0].value, 300); // 100 + 200
    assert.equal(result[1].value, 400); // 150 + 250
  });

  it('latest returns most recent point', () => {
    const ts = new TimeSeriesStore();
    ts.write('cpu', 1000, 0.5);
    ts.write('cpu', 2000, 0.8);
    
    const latest = ts.latest('cpu');
    assert.equal(latest.value, 0.8);
    assert.equal(latest.timestamp, 2000);
  });

  it('metrics lists all series', () => {
    const ts = new TimeSeriesStore();
    ts.write('cpu', 1000, 0.5);
    ts.write('memory', 1000, 0.7);
    ts.write('disk', 1000, 0.3);
    
    const metrics = ts.metrics();
    assert.equal(metrics.length, 3);
  });

  it('monitoring dashboard use case', () => {
    const ts = new TimeSeriesStore();
    
    // Simulate 1 minute of data at 1-second intervals
    for (let i = 0; i < 60; i++) {
      ts.write('request_latency', i * 1000, 10 + Math.random() * 40, { endpoint: '/api' });
      ts.write('request_count', i * 1000, Math.floor(Math.random() * 100), { endpoint: '/api' });
    }
    
    // Downsample to 10-second buckets
    const latency = ts.downsample('request_latency', 0, 60000, 10000, 'avg');
    assert.equal(latency.length, 6);
    
    // Max latency per 10 seconds
    const maxLatency = ts.downsample('request_latency', 0, 60000, 10000, 'max');
    assert.ok(maxLatency[0].value > latency[0].value); // Max should be >= avg
  });
});
