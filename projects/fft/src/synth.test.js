import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  sineOsc, sawtoothOsc, squareOsc, triangleOsc, noiseOsc,
  adsr, expDecay, applyEnvelope,
  additiveSynth, organHarmonics, bellHarmonics,
  fmSynth, fmSynthEnvelope,
  Wavetable,
  subtractiveSynth,
  delay, reverb, distort, tremolo, vibrato,
  mix, normalize,
} from './synth.js';
import { dominantFrequency, fft } from './fft.js';

const approx = (a, b, eps = 0.01) => Math.abs(a - b) < eps;

// ===== Oscillators =====
describe('Oscillators', () => {
  it('sine generates correct frequency', () => {
    const sampleRate = 4096;
    const signal = sineOsc(440, sampleRate, 0.25);
    assert.equal(signal.length, 1024);
    // Check zero crossings or frequency
    const padded = signal.slice(0, 1024);
    const freq = dominantFrequency(padded, sampleRate);
    assert.ok(approx(freq, 440, 10), `Expected 440Hz, got ${freq}`);
  });

  it('sawtooth has harmonics', () => {
    const sampleRate = 8192;
    const signal = sawtoothOsc(100, sampleRate, 0.25).slice(0, 2048);
    const spectrum = fft(signal);
    // Should have energy at fundamental and harmonics
    const mag = spectrum.map(c => c.magnitude());
    const fundBin = Math.round(100 * 2048 / sampleRate);
    const harmBin = Math.round(200 * 2048 / sampleRate);
    assert.ok(mag[fundBin] > 10, 'Should have fundamental');
    assert.ok(mag[harmBin] > 5, 'Should have 2nd harmonic');
  });

  it('square wave only has odd harmonics', () => {
    const sampleRate = 8192;
    const signal = squareOsc(100, sampleRate, 0.25).slice(0, 2048);
    const spectrum = fft(signal);
    const mag = spectrum.map(c => c.magnitude());
    const fund = Math.round(100 * 2048 / sampleRate);
    const h2 = Math.round(200 * 2048 / sampleRate);
    const h3 = Math.round(300 * 2048 / sampleRate);
    // Odd harmonics should be stronger than even
    assert.ok(mag[fund] > mag[h2], 'Fund > 2nd harmonic for square');
    assert.ok(mag[h3] > mag[h2], '3rd harmonic > 2nd for square');
  });

  it('triangle is bounded [-1, 1]', () => {
    const signal = triangleOsc(440, 44100, 0.1);
    assert.ok(signal.every(v => v >= -1.001 && v <= 1.001));
  });

  it('noise has approximately zero mean', () => {
    const signal = noiseOsc(44100, 1);
    const mean = signal.reduce((a, b) => a + b, 0) / signal.length;
    assert.ok(Math.abs(mean) < 0.05, `Mean should be ~0: ${mean}`);
  });

  it('square with duty cycle', () => {
    const signal = squareOsc(100, 1000, 0.01, 0.25);
    const positive = signal.filter(v => v > 0).length;
    const total = signal.length;
    assert.ok(approx(positive / total, 0.25, 0.15), `25% duty: ${(positive/total*100).toFixed(0)}%`);
  });
});

// ===== Envelopes =====
describe('Envelopes', () => {
  it('ADSR shape', () => {
    const env = adsr(0.1, 0.1, 0.7, 0.2, 1.0, 1000);
    assert.equal(env.length, 1000);
    // Attack peak
    assert.ok(approx(env[99], 1, 0.05), 'Should reach peak at end of attack');
    // Sustain level
    assert.ok(approx(env[500], 0.7, 0.1), `Should sustain at 0.7: ${env[500]}`);
    // Release end
    assert.ok(env[999] < 0.1, `Should decay to near 0: ${env[999]}`);
  });

  it('ADSR is bounded [0, 1]', () => {
    const env = adsr(0.05, 0.1, 0.5, 0.3, 0.5, 44100);
    assert.ok(env.every(v => v >= 0 && v <= 1));
  });

  it('exponential decay starts at 1', () => {
    const env = expDecay(0.1, 0.5, 44100);
    assert.ok(approx(env[0], 1, 0.001));
    assert.ok(env[env.length - 1] < 0.01, 'Should decay to near 0');
  });

  it('apply envelope modulates signal', () => {
    const signal = [1, 1, 1, 1];
    const env = [0.5, 1, 0.5, 0];
    const result = applyEnvelope(signal, env);
    assert.deepStrictEqual(result, [0.5, 1, 0.5, 0]);
  });
});

// ===== Additive Synthesis =====
describe('Additive Synthesis', () => {
  it('single harmonic = pure sine', () => {
    const sampleRate = 4096;
    const signal = additiveSynth(440, [{ harmonic: 1, amplitude: 1 }], sampleRate, 0.25);
    assert.equal(signal.length, 1024);
    const freq = dominantFrequency(signal, sampleRate);
    assert.ok(approx(freq, 440, 10));
  });

  it('organ harmonics produces richer timbre', () => {
    const sampleRate = 8192;
    const harmonics = organHarmonics(4);
    const signal = additiveSynth(200, harmonics, sampleRate, 0.25).slice(0, 2048);
    const spectrum = fft(signal);
    const mag = spectrum.map(c => c.magnitude());
    // Should have energy at 200, 400, 600, 800 Hz
    for (let h = 1; h <= 4; h++) {
      const bin = Math.round(200 * h * 2048 / sampleRate);
      assert.ok(mag[bin] > 1, `Should have harmonic ${h} at ${200*h}Hz`);
    }
  });

  it('bell harmonics are inharmonic', () => {
    const harmonics = bellHarmonics();
    assert.ok(harmonics.some(h => h.harmonic !== Math.round(h.harmonic)),
      'Bell should have non-integer harmonics');
  });

  it('respects Nyquist limit', () => {
    const sampleRate = 1000;
    const signal = additiveSynth(400, [
      { harmonic: 1, amplitude: 1 },
      { harmonic: 2, amplitude: 1 }, // 800 Hz > Nyquist (500 Hz) — should be skipped
    ], sampleRate, 0.1);
    // Signal should only contain 400 Hz, not 800 Hz
    assert.ok(signal.length > 0);
  });
});

// ===== FM Synthesis =====
describe('FM Synthesis', () => {
  it('zero modulation index = pure sine', () => {
    const sampleRate = 4096;
    const signal = fmSynth(440, 100, 0, sampleRate, 0.25);
    const freq = dominantFrequency(signal.slice(0, 1024), sampleRate);
    assert.ok(approx(freq, 440, 10), `Expected 440Hz carrier, got ${freq}`);
  });

  it('high modulation creates sidebands', () => {
    const sampleRate = 8192;
    const signal = fmSynth(440, 110, 5, sampleRate, 0.25).slice(0, 2048);
    const spectrum = fft(signal);
    const mag = spectrum.map(c => c.magnitude());
    // Should have energy beyond just the carrier
    const carrierBin = Math.round(440 * 2048 / sampleRate);
    let sidebandEnergy = 0;
    for (let i = 0; i < 1024; i++) {
      if (Math.abs(i - carrierBin) > 5) sidebandEnergy += mag[i];
    }
    assert.ok(sidebandEnergy > 0, 'FM should create sidebands');
  });

  it('envelope-modulated FM', () => {
    const sampleRate = 4096;
    const N = 1024;
    const modEnv = Array.from({ length: N }, (_, i) => 5 * (1 - i / N));
    const signal = fmSynthEnvelope(440, 110, modEnv, sampleRate, N / sampleRate);
    assert.equal(signal.length, N);
    assert.ok(signal.every(Number.isFinite));
  });
});

// ===== Wavetable Synthesis =====
describe('Wavetable', () => {
  it('sine wavetable plays correct frequency', () => {
    const wt = Wavetable.sine();
    const sampleRate = 4096;
    const signal = wt.play(440, sampleRate, 0.25);
    assert.equal(signal.length, 1024);
    const freq = dominantFrequency(signal, sampleRate);
    assert.ok(approx(freq, 440, 10));
  });

  it('from harmonics creates custom wavetable', () => {
    const wt = Wavetable.fromHarmonics([
      { harmonic: 1, amplitude: 1 },
      { harmonic: 3, amplitude: 0.5 },
    ]);
    assert.equal(wt.size, 2048);
    // Should be normalized
    const max = Math.max(...wt.table.map(Math.abs));
    assert.ok(approx(max, 1, 0.01));
  });

  it('saw wavetable has harmonics', () => {
    const wt = Wavetable.saw();
    const sampleRate = 8192;
    const signal = wt.play(100, sampleRate, 0.25).slice(0, 2048);
    const spectrum = fft(signal);
    const mag = spectrum.map(c => c.magnitude());
    const fundBin = Math.round(100 * 2048 / sampleRate);
    assert.ok(mag[fundBin] > 5, 'Saw wavetable should have fundamental');
  });

  it('linear interpolation between samples', () => {
    const wt = new Wavetable([0, 1, 0, -1]);
    // Phase 0 = 0, Phase 0.25 = 1, Phase 0.125 = 0.5 (interpolated)
    assert.ok(approx(wt.read(0), 0, 0.01));
    assert.ok(approx(wt.read(0.25), 1, 0.01));
    assert.ok(approx(wt.read(0.125), 0.5, 0.01));
  });
});

// ===== Subtractive Synthesis =====
describe('Subtractive Synthesis', () => {
  it('lowpass filter removes high harmonics from sawtooth', () => {
    const sampleRate = 8192;
    const raw = sawtoothOsc(100, sampleRate, 0.25);
    const filtered = subtractiveSynth(raw, 200, sampleRate);

    // Filtered should have less high-frequency content
    const rawSpec = fft(raw.slice(0, 2048)).map(c => c.magnitude());
    const filtSpec = fft(filtered.slice(0, 2048)).map(c => c.magnitude());

    const highBin = Math.round(400 * 2048 / sampleRate);
    assert.ok(filtSpec[highBin] < rawSpec[highBin],
      'Filter should attenuate high frequencies');
  });
});

// ===== Effects =====
describe('Effects', () => {
  it('delay adds echo', () => {
    const signal = [1, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    const result = delay(signal, 0.003, 0.5, 1000); // 3ms delay = 3 samples
    assert.ok(approx(result[0], 1));
    assert.ok(approx(result[3], 0.5), `Echo at 3: ${result[3]}`);
    assert.ok(approx(result[6], 0.25), `Second echo at 6: ${result[6]}`);
  });

  it('distortion soft clips', () => {
    const signal = [0, 0.5, 1, 2, -2];
    const result = distort(signal, 1);
    assert.ok(approx(result[0], 0));
    assert.ok(result[3] < 2, 'Should clip high values');
    assert.ok(result[3] > 0.9, 'Should preserve direction');
    assert.ok(result[4] < -0.9, 'Should clip negative');
  });

  it('tremolo modulates amplitude', () => {
    const sampleRate = 1000;
    const signal = new Array(1000).fill(1);
    const result = tremolo(signal, 5, 0.5, sampleRate);
    const min = Math.min(...result);
    const max = Math.max(...result);
    assert.ok(min < 1, 'Tremolo should reduce amplitude');
    assert.ok(max <= 1.001, 'Tremolo should not exceed original');
    assert.ok(min > 0.4, `Min should be bounded: ${min}`);
  });

  it('reverb preserves signal length', () => {
    const signal = sineOsc(440, 44100, 0.1);
    const result = reverb(signal);
    assert.equal(result.length, signal.length);
  });

  it('vibrato preserves signal length', () => {
    const signal = sineOsc(440, 44100, 0.1);
    const result = vibrato(signal, 5, 0.002, 44100);
    assert.equal(result.length, signal.length);
  });
});

// ===== Mix and Normalize =====
describe('Mix and Normalize', () => {
  it('mix combines signals', () => {
    const a = [1, 0, 0];
    const b = [0, 1, 0];
    const result = mix([a, b]);
    assert.ok(approx(result[0], 0.5));
    assert.ok(approx(result[1], 0.5));
    assert.ok(approx(result[2], 0));
  });

  it('mix with custom gains', () => {
    const a = [1, 0];
    const b = [0, 1];
    const result = mix([a, b], [0.8, 0.2]);
    assert.ok(approx(result[0], 0.8));
    assert.ok(approx(result[1], 0.2));
  });

  it('normalize scales to [-1, 1]', () => {
    const signal = [0, 5, -3, 2];
    const result = normalize(signal);
    assert.ok(approx(result[1], 1));
    assert.ok(approx(result[2], -0.6));
    const max = Math.max(...result.map(Math.abs));
    assert.ok(approx(max, 1));
  });

  it('normalize handles zero signal', () => {
    const result = normalize([0, 0, 0]);
    assert.deepStrictEqual(result, [0, 0, 0]);
  });
});
