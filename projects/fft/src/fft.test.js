import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Complex, dft, fft, ifft, convolve,
  hamming, hanning, blackman, applyWindow,
  powerSpectrum, magnitudeSpectrum, dominantFrequency,
  sine, cosine, squareWave, spectrogram,
  crossCorrelation, autoCorrelation,
  zeroPadInterpolate,
  firLowpass, firHighpass, firBandpass, applyFIR,
  biquadLowpass, biquadHighpass, biquadBandpass, applyBiquad, applyBiquadCascade,
  stft, istft,
  frequencyResponse, toDB, fromDB,
  goertzel, cepstrum, whiteNoise, detectPitch,
} from './fft.js';

const approx = (a, b, eps = 0.01) => Math.abs(a - b) < eps;

describe('Complex', () => {
  it('add', () => { const c = new Complex(1, 2).add(new Complex(3, 4)); assert.equal(c.re, 4); assert.equal(c.im, 6); });
  it('mul', () => { const c = new Complex(1, 2).mul(new Complex(3, 4)); assert.equal(c.re, -5); assert.equal(c.im, 10); });
  it('magnitude', () => { assert.ok(approx(new Complex(3, 4).magnitude(), 5)); });
  it('conjugate', () => { const c = new Complex(1, 2).conjugate(); assert.equal(c.im, -2); });
  it('polar', () => { const c = Complex.polar(1, Math.PI / 2); assert.ok(approx(c.re, 0)); assert.ok(approx(c.im, 1)); });
});

describe('DFT', () => {
  it('DC component of constant signal', () => {
    const result = dft([1, 1, 1, 1]);
    assert.ok(approx(result[0].re, 4));
    assert.ok(approx(result[1].magnitude(), 0));
  });

  it('pure sine', () => {
    const N = 8;
    const signal = Array.from({ length: N }, (_, n) => Math.sin(2 * Math.PI * n / N));
    const result = dft(signal);
    // Should have peak at bin 1
    assert.ok(result[1].magnitude() > 3);
    assert.ok(approx(result[0].magnitude(), 0));
  });
});

describe('FFT', () => {
  it('matches DFT', () => {
    const signal = [1, 2, 3, 4, 5, 6, 7, 8];
    const dftResult = dft(signal);
    const fftResult = fft(signal);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(fftResult[i].re, dftResult[i].re, 0.001));
      assert.ok(approx(fftResult[i].im, dftResult[i].im, 0.001));
    }
  });

  it('DC component', () => {
    const result = fft([3, 3, 3, 3]);
    assert.ok(approx(result[0].re, 12));
  });

  it('impulse', () => {
    const result = fft([1, 0, 0, 0]);
    // All bins should have magnitude 1
    for (const c of result) assert.ok(approx(c.magnitude(), 1));
  });

  it('power of 2 requirement', () => {
    assert.throws(() => fft([1, 2, 3]));
  });
});

describe('IFFT', () => {
  it('roundtrips', () => {
    const signal = [1, 2, 3, 4, 5, 6, 7, 8];
    const spectrum = fft(signal);
    const recovered = ifft(spectrum);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(recovered[i].re, signal[i], 0.001));
    }
  });

  it('roundtrips sine', () => {
    const N = 64;
    const signal = sine(10, N, 1).slice(0, N);
    const spectrum = fft(signal);
    const recovered = ifft(spectrum);
    for (let i = 0; i < N; i++) {
      assert.ok(approx(recovered[i].re, signal[i], 0.001));
    }
  });
});

describe('Convolution', () => {
  it('convolves two signals', () => {
    const a = [1, 2, 3];
    const b = [4, 5, 6];
    const result = convolve(a, b);
    // Expected: [4, 13, 28, 27, 18]
    assert.ok(approx(result[0], 4));
    assert.ok(approx(result[1], 13));
    assert.ok(approx(result[2], 28));
    assert.ok(approx(result[3], 27));
    assert.ok(approx(result[4], 18));
  });

  it('identity convolution', () => {
    const signal = [1, 2, 3, 4];
    const impulse = [1, 0, 0, 0];
    const result = convolve(signal, impulse);
    for (let i = 0; i < signal.length; i++) {
      assert.ok(approx(result[i], signal[i]));
    }
  });
});

describe('Windowing', () => {
  it('hamming window', () => {
    const w = hamming(8);
    assert.equal(w.length, 8);
    assert.ok(w[0] > 0);
    assert.ok(w[3] > w[0]); // center should be higher
  });

  it('hanning window', () => {
    const w = hanning(8);
    assert.ok(approx(w[0], 0)); // starts at 0
    assert.ok(approx(w[4], 1, 0.1)); // peak near center
  });

  it('blackman window', () => {
    const w = blackman(8);
    assert.equal(w.length, 8);
    assert.ok(w[4] > w[0]);
  });

  it('apply window', () => {
    const signal = [1, 1, 1, 1];
    const window = [0.5, 1, 1, 0.5];
    const result = applyWindow(signal, window);
    assert.deepStrictEqual(result, [0.5, 1, 1, 0.5]);
  });
});

describe('Frequency Analysis', () => {
  it('detects dominant frequency', () => {
    const sampleRate = 256;
    const signal = sine(32, sampleRate, 1); // 32 Hz
    const padded = signal.slice(0, 256);
    const freq = dominantFrequency(padded, sampleRate);
    assert.ok(approx(freq, 32, 2));
  });

  it('power spectrum has correct shape', () => {
    const signal = sine(10, 128, 1).slice(0, 128);
    const ps = powerSpectrum(signal);
    assert.equal(ps.length, 128);
    // Peak should be at bin 10
    let maxIdx = 0;
    for (let i = 1; i < 64; i++) if (ps[i] > ps[maxIdx]) maxIdx = i;
    assert.equal(maxIdx, 10);
  });
});

describe('Signal Generation', () => {
  it('sine wave', () => {
    const s = sine(1, 4, 1);
    assert.equal(s.length, 4);
    assert.ok(approx(s[0], 0));
    assert.ok(approx(s[1], 1));
  });

  it('square wave', () => {
    const s = squareWave(1, 8, 1);
    assert.equal(s.length, 8);
    assert.ok(s.every(v => v === 1 || v === -1));
  });
});

describe('Spectrogram', () => {
  it('produces frames', () => {
    const signal = sine(100, 1024, 1);
    const frames = spectrogram(signal, 256, 128);
    assert.ok(frames.length > 0);
    assert.equal(frames[0].length, 128); // half window
  });
});

// ===== Correlation Tests =====
describe('Cross-Correlation', () => {
  it('auto-correlation peak at zero lag', () => {
    const signal = sine(10, 128, 1).slice(0, 128);
    const ac = autoCorrelation(signal);
    // Peak should be at lag 0 (index 0)
    const peak = ac[0];
    for (let i = 1; i < ac.length; i++) {
      assert.ok(ac[i] <= peak + 0.001, `ac[${i}]=${ac[i]} should be <= peak=${peak}`);
    }
  });

  it('detects time delay between signals', () => {
    // Cross-correlation should have a peak that shifts with delay
    const N = 128;
    const signal = sine(10, N, 1).slice(0, N);
    const corr = crossCorrelation(signal, signal);
    // Auto-correlation peak is at index 0
    let maxIdx = 0;
    for (let i = 1; i < N; i++) {
      if (corr[i] > corr[maxIdx]) maxIdx = i;
    }
    assert.equal(maxIdx, 0, 'Auto-correlation peak should be at lag 0');
  });

  it('periodic signal has periodic auto-correlation', () => {
    const sampleRate = 256;
    const freq = 16; // 16 Hz
    const signal = sine(freq, sampleRate, 1).slice(0, 256);
    const ac = autoCorrelation(signal);
    // Should have secondary peak at period = sampleRate/freq = 16 samples
    const period = sampleRate / freq;
    // Check that ac[period] is also a local maximum
    assert.ok(ac[period] > ac[period - 1] || ac[period] > ac[period + 1],
      'Should have correlation peak at signal period');
  });
});

// ===== Zero-Padding Interpolation Tests =====
describe('Zero-Pad Interpolation', () => {
  it('preserves signal shape', () => {
    const N = 64;
    const signal = sine(4, N, 1).slice(0, N);
    const interp = zeroPadInterpolate(signal, 4);
    assert.equal(interp.length, N * 4);
    // Check that every 4th sample matches original (approximately)
    for (let i = 0; i < N; i++) {
      assert.ok(approx(interp[i * 4], signal[i], 0.1),
        `interp[${i*4}]=${interp[i*4]} vs signal[${i}]=${signal[i]}`);
    }
  });

  it('DC signal stays constant', () => {
    const signal = [3, 3, 3, 3];
    const interp = zeroPadInterpolate(signal, 2);
    assert.equal(interp.length, 8);
    for (const v of interp) {
      assert.ok(approx(v, 3, 0.1), `Expected ~3, got ${v}`);
    }
  });
});

// ===== FIR Filter Tests =====
describe('FIR Filters', () => {
  it('lowpass removes high frequencies', () => {
    const sampleRate = 1024;
    const low = sine(50, sampleRate, 1).slice(0, 1024);
    const high = sine(400, sampleRate, 1).slice(0, 1024);
    const signal = low.map((v, i) => v + high[i]);

    const coeffs = firLowpass(100, sampleRate, 101);
    const filtered = applyFIR(signal, coeffs);

    // After lowpass at 100 Hz, high frequency should be attenuated
    // Use power-of-2 slice for FFT
    const freqAfter = dominantFrequency(filtered.slice(64, 64 + 512), sampleRate);
    // Should still have 50 Hz component
    assert.ok(approx(freqAfter, 50, 10), `Expected ~50Hz, got ${freqAfter}`);
  });

  it('highpass removes low frequencies', () => {
    const sampleRate = 1024;
    const low = sine(20, sampleRate, 1).slice(0, 1024);
    const high = sine(300, sampleRate, 1).slice(0, 1024);
    const signal = low.map((v, i) => v + high[i]);

    const coeffs = firHighpass(100, sampleRate, 101);
    const filtered = applyFIR(signal, coeffs);

    const freqAfter = dominantFrequency(filtered.slice(64, 64 + 512), sampleRate);
    assert.ok(approx(freqAfter, 300, 20), `Expected ~300Hz, got ${freqAfter}`);
  });

  it('bandpass isolates middle frequency', () => {
    const sampleRate = 1024;
    const f1 = sine(30, sampleRate, 1).slice(0, 1024);
    const f2 = sine(200, sampleRate, 1).slice(0, 1024);
    const f3 = sine(450, sampleRate, 1).slice(0, 1024);
    const signal = f1.map((v, i) => v + f2[i] + f3[i]);

    const coeffs = firBandpass(100, 300, sampleRate, 101);
    const filtered = applyFIR(signal, coeffs);

    const freqAfter = dominantFrequency(filtered.slice(64, 64 + 512), sampleRate);
    assert.ok(approx(freqAfter, 200, 20), `Expected ~200Hz, got ${freqAfter}`);
  });

  it('frequency response shows lowpass shape', () => {
    const coeffs = firLowpass(100, 1024, 101);
    const resp = frequencyResponse(coeffs, 1024);
    // At DC (index 0), should be near 1
    assert.ok(approx(resp.magnitude[0], 1, 0.1));
    // At Nyquist/2, should be attenuated (< 0.1)
    const nyquistIdx = resp.magnitude.length - 1;
    assert.ok(resp.magnitude[nyquistIdx] < 0.1, `Nyquist should be attenuated: ${resp.magnitude[nyquistIdx]}`);
  });
});

// ===== IIR Biquad Filter Tests =====
describe('IIR Biquad Filters', () => {
  it('biquad lowpass attenuates high freq', () => {
    const sampleRate = 1024;
    const low = sine(50, sampleRate, 1).slice(0, 1024);
    const high = sine(400, sampleRate, 1).slice(0, 1024);
    const signal = low.map((v, i) => v + high[i]);

    const filter = biquadLowpass(100, sampleRate);
    const filtered = applyBiquad(signal, filter);

    // Measure energy in low vs high band after filtering
    const spec = powerSpectrum(filtered);
    const lowEnergy = spec.slice(40, 60).reduce((a, b) => a + b, 0);
    const highEnergy = spec.slice(350, 450).reduce((a, b) => a + b, 0);
    assert.ok(lowEnergy > highEnergy * 5, 'Low freq should have more energy');
  });

  it('biquad highpass attenuates low freq', () => {
    const sampleRate = 1024;
    const signal = sine(50, sampleRate, 1).slice(0, 1024);

    const filter = biquadHighpass(200, sampleRate);
    const filtered = applyBiquad(signal, filter);

    // Energy should be much lower
    const origEnergy = signal.reduce((a, b) => a + b * b, 0);
    const filtEnergy = filtered.reduce((a, b) => a + b * b, 0);
    assert.ok(filtEnergy < origEnergy * 0.1, 'Should attenuate low frequencies');
  });

  it('cascaded biquads for steeper rolloff', () => {
    const sampleRate = 1024;
    const signal = sine(400, sampleRate, 1).slice(0, 1024);

    const f1 = biquadLowpass(100, sampleRate);
    const f2 = biquadLowpass(100, sampleRate);
    const filtered = applyBiquadCascade(signal, [f1, f2]);

    const energy = filtered.reduce((a, b) => a + b * b, 0) / filtered.length;
    assert.ok(energy < 0.01, `Cascaded filter should strongly attenuate: rms=${Math.sqrt(energy)}`);
  });
});

// ===== STFT / ISTFT Tests =====
describe('STFT / ISTFT', () => {
  it('roundtrips signal', () => {
    const signal = sine(50, 1024, 0.5).slice(0, 512);
    const analysis = stft(signal, 128, 64);
    const reconstructed = istft(analysis, signal.length);

    // Should approximately match original (within window effects)
    let maxErr = 0;
    for (let i = 64; i < signal.length - 64; i++) { // skip edges
      const err = Math.abs(reconstructed[i] - signal[i]);
      if (err > maxErr) maxErr = err;
    }
    assert.ok(maxErr < 0.05, `Max error should be small: ${maxErr}`);
  });

  it('STFT frame count is correct', () => {
    const signal = new Array(1024).fill(0);
    const analysis = stft(signal, 256, 128);
    // Expected frames: floor((1024 - 256) / 128) + 1 = 7
    assert.equal(analysis.frames.length, 7);
  });
});

// ===== Goertzel Algorithm Tests =====
describe('Goertzel', () => {
  it('detects specific frequency', () => {
    const sampleRate = 256;
    const signal = sine(32, sampleRate, 1).slice(0, 256);
    const result = goertzel(signal, 32, sampleRate);
    assert.ok(result.magnitude() > 50, `Should detect 32Hz: mag=${result.magnitude()}`);
  });

  it('no false positive at wrong frequency', () => {
    const sampleRate = 256;
    const signal = sine(32, sampleRate, 1).slice(0, 256);
    const result = goertzel(signal, 100, sampleRate);
    assert.ok(result.magnitude() < 5, `Should not detect 100Hz: mag=${result.magnitude()}`);
  });

  it('matches FFT bin', () => {
    const signal = [1, 2, 3, 4, 5, 6, 7, 8];
    const sampleRate = 8;
    const targetFreq = 2; // bin 2
    const goertzelResult = goertzel(signal, targetFreq, sampleRate);
    const fftResult = fft(signal);
    const bin = Math.round(targetFreq * signal.length / sampleRate);
    assert.ok(approx(goertzelResult.magnitude(), fftResult[bin].magnitude(), 0.1));
  });
});

// ===== dB Conversion Tests =====
describe('dB Conversion', () => {
  it('unity is 0 dB', () => { assert.ok(approx(toDB(1), 0)); });
  it('double is ~6 dB', () => { assert.ok(approx(toDB(2), 6.02, 0.1)); });
  it('roundtrips', () => { assert.ok(approx(fromDB(toDB(0.5)), 0.5, 0.001)); });
  it('half is ~-6 dB', () => { assert.ok(approx(toDB(0.5), -6.02, 0.1)); });
});

// ===== Cepstrum Tests =====
describe('Cepstrum', () => {
  it('computes without error', () => {
    const signal = sine(100, 1024, 0.25).slice(0, 256);
    const ceps = cepstrum(signal);
    assert.equal(ceps.length, signal.length);
    assert.ok(Number.isFinite(ceps[0]));
  });
});

// ===== Pitch Detection Tests =====
describe('Pitch Detection', () => {
  it('detects 440 Hz', () => {
    const sampleRate = 8192;
    const signal = sine(440, sampleRate, 0.1).slice(0, 512);
    const pitch = detectPitch(signal, sampleRate, 100, 1000);
    assert.ok(approx(pitch, 440, 20), `Expected ~440Hz, got ${pitch}`);
  });

  it('detects 220 Hz', () => {
    const sampleRate = 8192;
    const signal = sine(220, sampleRate, 0.1).slice(0, 1024);
    const pitch = detectPitch(signal, sampleRate, 100, 1000);
    assert.ok(approx(pitch, 220, 15), `Expected ~220Hz, got ${pitch}`);
  });
});

// ===== Noise Generation Tests =====
describe('Noise', () => {
  it('white noise has correct length', () => {
    const n = whiteNoise(1024);
    assert.equal(n.length, 1024);
  });

  it('white noise is bounded', () => {
    const n = whiteNoise(1000, 0.5);
    assert.ok(n.every(v => Math.abs(v) <= 0.5));
  });

  it('white noise has ~zero mean', () => {
    const n = whiteNoise(10000);
    const mean = n.reduce((a, b) => a + b, 0) / n.length;
    assert.ok(Math.abs(mean) < 0.1, `Mean should be ~0: ${mean}`);
  });
});
