import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  Complex, dft, fft, ifft, convolve,
  powerSpectrum, magnitudeSpectrum,
  sine, whiteNoise,
  firLowpass, firHighpass, firBandpass, applyFIR,
  biquadLowpass, applyBiquad,
  frequencyResponse, toDB,
  crossCorrelation, autoCorrelation,
  stft, istft,
  zeroPadInterpolate,
  goertzel,
  hamming, hanning, blackman,
} from './fft.js';

const approx = (a, b, eps = 0.01) => Math.abs(a - b) < eps;

// ===== Parseval's Theorem =====
// Sum of |x[n]|² = (1/N) * Sum of |X[k]|²
describe('Parseval\'s Theorem', () => {
  it('holds for random signals', () => {
    for (let seed = 0; seed < 20; seed++) {
      const N = 64;
      const signal = Array.from({ length: N }, () => Math.random() * 2 - 1);
      const spectrum = fft(signal);

      const timeEnergy = signal.reduce((s, x) => s + x * x, 0);
      const freqEnergy = spectrum.reduce((s, c) => s + c.re * c.re + c.im * c.im, 0) / N;

      assert.ok(approx(timeEnergy, freqEnergy, 0.01),
        `Parseval failed (seed ${seed}): time=${timeEnergy.toFixed(4)} freq=${freqEnergy.toFixed(4)}`);
    }
  });

  it('holds for sine waves', () => {
    const N = 256;
    for (const freq of [4, 16, 32, 64]) {
      const signal = sine(freq, N, 1).slice(0, N);
      const spectrum = fft(signal);

      const timeEnergy = signal.reduce((s, x) => s + x * x, 0);
      const freqEnergy = spectrum.reduce((s, c) => s + c.re * c.re + c.im * c.im, 0) / N;

      assert.ok(approx(timeEnergy, freqEnergy, 0.1),
        `Parseval failed for ${freq}Hz: time=${timeEnergy.toFixed(4)} freq=${freqEnergy.toFixed(4)}`);
    }
  });
});

// ===== Convolution Theorem =====
// conv(a,b) = IFFT(FFT(a) * FFT(b))
describe('Convolution Theorem', () => {
  it('FFT convolution matches direct convolution', () => {
    for (let seed = 0; seed < 10; seed++) {
      const a = Array.from({ length: 16 }, () => Math.random() * 2 - 1);
      const b = Array.from({ length: 8 }, () => Math.random() * 2 - 1);

      // Direct convolution
      const direct = new Array(a.length + b.length - 1).fill(0);
      for (let i = 0; i < a.length; i++) {
        for (let j = 0; j < b.length; j++) {
          direct[i + j] += a[i] * b[j];
        }
      }

      // FFT convolution
      const fftConv = convolve(a, b);

      for (let i = 0; i < direct.length; i++) {
        assert.ok(approx(direct[i], fftConv[i], 0.001),
          `Mismatch at ${i}: direct=${direct[i].toFixed(4)} fft=${fftConv[i].toFixed(4)}`);
      }
    }
  });
});

// ===== FFT/IFFT Roundtrip Stress =====
describe('FFT/IFFT Roundtrip', () => {
  it('roundtrips 50 random signals', () => {
    for (let seed = 0; seed < 50; seed++) {
      const N = 128;
      const signal = Array.from({ length: N }, () => Math.random() * 10 - 5);
      const recovered = ifft(fft(signal));
      for (let i = 0; i < N; i++) {
        assert.ok(approx(recovered[i].re, signal[i], 0.001),
          `Roundtrip failed at ${i} (seed ${seed}): ${recovered[i].re.toFixed(4)} vs ${signal[i].toFixed(4)}`);
        assert.ok(Math.abs(recovered[i].im) < 0.001,
          `Imaginary part should be ~0 (seed ${seed}): ${recovered[i].im}`);
      }
    }
  });

  it('roundtrips complex signals', () => {
    const N = 32;
    const signal = Array.from({ length: N }, () => new Complex(Math.random(), Math.random()));
    const recovered = ifft(fft(signal));
    for (let i = 0; i < N; i++) {
      assert.ok(approx(recovered[i].re, signal[i].re, 0.001));
      assert.ok(approx(recovered[i].im, signal[i].im, 0.001));
    }
  });

  it('FFT matches DFT for various sizes', () => {
    for (const N of [2, 4, 8, 16, 32, 64]) {
      const signal = Array.from({ length: N }, () => Math.random() * 2 - 1);
      const dftResult = dft(signal);
      const fftResult = fft(signal);
      for (let i = 0; i < N; i++) {
        assert.ok(approx(fftResult[i].re, dftResult[i].re, 0.001),
          `Size ${N}, bin ${i}: FFT re=${fftResult[i].re.toFixed(4)} DFT re=${dftResult[i].re.toFixed(4)}`);
        assert.ok(approx(fftResult[i].im, dftResult[i].im, 0.001));
      }
    }
  });
});

// ===== FFT Properties =====
describe('FFT Properties', () => {
  it('linearity: FFT(a*x + b*y) = a*FFT(x) + b*FFT(y)', () => {
    const N = 64;
    const x = Array.from({ length: N }, () => Math.random());
    const y = Array.from({ length: N }, () => Math.random());
    const a = 2.5, b = -1.3;

    const combined = x.map((v, i) => a * v + b * y[i]);
    const fftCombined = fft(combined);
    const fftX = fft(x);
    const fftY = fft(y);

    for (let k = 0; k < N; k++) {
      const expected = fftX[k].scale(a).add(fftY[k].scale(b));
      assert.ok(approx(fftCombined[k].re, expected.re, 0.01));
      assert.ok(approx(fftCombined[k].im, expected.im, 0.01));
    }
  });

  it('symmetry: real signal has conjugate symmetric spectrum', () => {
    const N = 64;
    const signal = Array.from({ length: N }, () => Math.random());
    const spectrum = fft(signal);
    for (let k = 1; k < N / 2; k++) {
      assert.ok(approx(spectrum[k].re, spectrum[N - k].re, 0.001),
        `Real symmetry at ${k}`);
      assert.ok(approx(spectrum[k].im, -spectrum[N - k].im, 0.001),
        `Imaginary antisymmetry at ${k}`);
    }
  });

  it('DC bin equals sum of signal', () => {
    const signal = [3, -1, 2, 4, 0, -2, 1, 5];
    const spectrum = fft(signal);
    const sum = signal.reduce((a, b) => a + b, 0);
    assert.ok(approx(spectrum[0].re, sum, 0.001));
    assert.ok(approx(spectrum[0].im, 0, 0.001));
  });

  it('impulse has flat spectrum', () => {
    for (const N of [8, 16, 32, 64]) {
      const impulse = new Array(N).fill(0);
      impulse[0] = 1;
      const spectrum = fft(impulse);
      for (const c of spectrum) {
        assert.ok(approx(c.magnitude(), 1, 0.001));
      }
    }
  });
});

// ===== Filter Verification =====
describe('Filter Frequency Response Verification', () => {
  it('FIR lowpass has -3dB at cutoff', () => {
    const sampleRate = 1024;
    const cutoff = 100;
    const coeffs = firLowpass(cutoff, sampleRate, 201);
    const resp = frequencyResponse(coeffs, 2048);

    // Find bin closest to cutoff
    const binPerHz = 2048 / sampleRate;
    const cutoffBin = Math.round(cutoff * binPerHz);

    const dcGain = resp.magnitude[0];
    const cutoffGain = resp.magnitude[cutoffBin];
    const cutoffDb = toDB(cutoffGain / dcGain);

    // At cutoff, gain should be around -3 to -6 dB (windowed sinc)
    assert.ok(cutoffDb < 0 && cutoffDb > -10,
      `Cutoff gain should be -3 to -6 dB, got ${cutoffDb.toFixed(1)} dB`);
  });

  it('FIR highpass rejects DC', () => {
    const coeffs = firHighpass(100, 1024, 101);
    const resp = frequencyResponse(coeffs, 1024);
    assert.ok(resp.magnitude[0] < 0.01, `DC should be rejected: ${resp.magnitude[0]}`);
  });

  it('bandpass has passband between cutoffs', () => {
    const sampleRate = 1024;
    const coeffs = firBandpass(100, 300, sampleRate, 201);
    const resp = frequencyResponse(coeffs, 2048);
    const binPerHz = 2048 / sampleRate;

    // Check passband center (~200 Hz)
    const passBin = Math.round(200 * binPerHz);
    // Check stopband (50 Hz and 450 Hz)
    const stopLowBin = Math.round(50 * binPerHz);
    const stopHighBin = Math.round(450 * binPerHz);

    assert.ok(resp.magnitude[passBin] > resp.magnitude[stopLowBin] * 3,
      'Passband should be stronger than low stopband');
    assert.ok(resp.magnitude[passBin] > resp.magnitude[stopHighBin] * 3,
      'Passband should be stronger than high stopband');
  });
});

// ===== STFT/ISTFT Stress =====
describe('STFT/ISTFT Stress', () => {
  it('roundtrips multiple signal types', () => {
    const signals = [
      sine(50, 1024, 0.5).slice(0, 512),
      Array.from({ length: 512 }, () => Math.random() * 2 - 1),
      sine(100, 1024, 0.5).slice(0, 512).map((v, i) => v + 0.5 * Math.sin(2 * Math.PI * 200 * i / 1024)),
    ];

    for (const signal of signals) {
      const analysis = stft(signal, 128, 32); // 75% overlap
      const reconstructed = istft(analysis, signal.length);

      // Check interior samples (edges have windowing effects)
      let maxErr = 0;
      for (let i = 128; i < signal.length - 128; i++) {
        const err = Math.abs(reconstructed[i] - signal[i]);
        if (err > maxErr) maxErr = err;
      }
      assert.ok(maxErr < 0.05, `STFT roundtrip error too large: ${maxErr.toFixed(4)}`);
    }
  });
});

// ===== Goertzel vs FFT Cross-Check =====
describe('Goertzel vs FFT', () => {
  it('matches for 20 random signals at random frequencies', () => {
    for (let seed = 0; seed < 20; seed++) {
      const N = 64;
      const sampleRate = N;
      const signal = Array.from({ length: N }, () => Math.random() * 2 - 1);
      const targetBin = Math.floor(Math.random() * (N / 2 - 1)) + 1;
      const targetFreq = targetBin * sampleRate / N;

      const goertzelMag = goertzel(signal, targetFreq, sampleRate).magnitude();
      const fftMag = fft(signal)[targetBin].magnitude();

      assert.ok(approx(goertzelMag, fftMag, 0.1),
        `Goertzel/FFT mismatch at bin ${targetBin}: ${goertzelMag.toFixed(2)} vs ${fftMag.toFixed(2)}`);
    }
  });
});

// ===== Windowing Properties =====
describe('Window Properties', () => {
  it('all windows are symmetric', () => {
    for (const winFn of [hamming, hanning, blackman]) {
      const N = 64;
      const w = winFn(N);
      for (let i = 0; i < N / 2; i++) {
        assert.ok(approx(w[i], w[N - 1 - i], 0.001),
          `Window not symmetric at ${i}: ${w[i]} vs ${w[N-1-i]}`);
      }
    }
  });

  it('all windows peak at center', () => {
    for (const winFn of [hamming, hanning, blackman]) {
      const N = 65;
      const w = winFn(N);
      const center = Math.floor(N / 2);
      assert.ok(approx(w[center], 1, 0.1), `Window center should be ~1: ${w[center]}`);
    }
  });

  it('Hanning window starts and ends at 0', () => {
    const w = hanning(32);
    assert.ok(approx(w[0], 0, 0.001));
    assert.ok(approx(w[31], 0, 0.001));
  });
});

// ===== Zero-Pad Interpolation Verification =====
describe('Zero-Pad Interpolation Stress', () => {
  it('preserves frequency content', () => {
    const N = 64;
    const freq = 4;
    const signal = sine(freq, N, 1).slice(0, N);
    const interp = zeroPadInterpolate(signal, 4);

    // The interpolated signal should have the same dominant frequency
    // but at 4x the sample rate
    const specOrig = magnitudeSpectrum(signal);
    const specInterp = magnitudeSpectrum(interp);

    // Original peak at bin 4, interpolated peak should be at bin 4 (same frequency, 4x spectrum length)
    let origPeak = 0, interpPeak = 0;
    for (let i = 1; i < N / 2; i++) if (specOrig[i] > specOrig[origPeak]) origPeak = i;
    for (let i = 1; i < interp.length / 2; i++) if (specInterp[i] > specInterp[interpPeak]) interpPeak = i;

    assert.equal(origPeak, freq);
    assert.equal(interpPeak, freq); // Same bin index = same frequency
  });
});

// ===== Complex Number Stress =====
describe('Complex Arithmetic Stress', () => {
  it('multiplication is associative', () => {
    for (let i = 0; i < 20; i++) {
      const a = new Complex(Math.random() * 10, Math.random() * 10);
      const b = new Complex(Math.random() * 10, Math.random() * 10);
      const c = new Complex(Math.random() * 10, Math.random() * 10);
      const ab_c = a.mul(b).mul(c);
      const a_bc = a.mul(b.mul(c));
      assert.ok(approx(ab_c.re, a_bc.re, 0.01));
      assert.ok(approx(ab_c.im, a_bc.im, 0.01));
    }
  });

  it('magnitude of product = product of magnitudes', () => {
    for (let i = 0; i < 20; i++) {
      const a = new Complex(Math.random() * 5, Math.random() * 5);
      const b = new Complex(Math.random() * 5, Math.random() * 5);
      assert.ok(approx(a.mul(b).magnitude(), a.magnitude() * b.magnitude(), 0.01));
    }
  });
});
