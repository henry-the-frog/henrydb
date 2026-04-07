import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Complex, dft, fft, ifft, convolve,
  hamming, hanning, blackman, applyWindow,
  powerSpectrum, magnitudeSpectrum, dominantFrequency,
  sine, cosine, squareWave, spectrogram,
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
