import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  noteFrequency, frequencyToNote, detectNotes, detectChord,
  generateChord, asciiSpectrogram,
  dtmfGenerate, dtmfDetect,
} from './audio-analyzer.js';
import { sine } from './fft.js';

const approx = (a, b, eps = 0.01) => Math.abs(a - b) < eps;

describe('Note Frequencies', () => {
  it('A4 = 440 Hz', () => {
    assert.ok(approx(noteFrequency('A', 4), 440, 0.1));
  });

  it('C4 ≈ 261.63 Hz', () => {
    assert.ok(approx(noteFrequency('C', 4), 261.63, 0.5));
  });

  it('A5 = 880 Hz (octave above A4)', () => {
    assert.ok(approx(noteFrequency('A', 5), 880, 0.1));
  });

  it('A3 = 220 Hz (octave below A4)', () => {
    assert.ok(approx(noteFrequency('A', 3), 220, 0.1));
  });

  it('E4 ≈ 329.63 Hz', () => {
    assert.ok(approx(noteFrequency('E', 4), 329.63, 0.5));
  });
});

describe('Frequency to Note', () => {
  it('440 Hz → A4', () => {
    const n = frequencyToNote(440);
    assert.equal(n.note, 'A');
    assert.equal(n.octave, 4);
    assert.ok(Math.abs(n.cents) < 5);
  });

  it('261.63 Hz → C4', () => {
    const n = frequencyToNote(261.63);
    assert.equal(n.note, 'C');
    assert.equal(n.octave, 4);
  });

  it('880 Hz → A5', () => {
    const n = frequencyToNote(880);
    assert.equal(n.note, 'A');
    assert.equal(n.octave, 5);
  });

  it('slightly sharp note has positive cents', () => {
    const n = frequencyToNote(445);
    assert.equal(n.note, 'A');
    assert.ok(n.cents > 0);
  });

  it('slightly flat note has negative cents', () => {
    const n = frequencyToNote(435);
    assert.equal(n.note, 'A');
    assert.ok(n.cents < 0);
  });
});

describe('Note Detection', () => {
  it('detects single 440 Hz tone', () => {
    const sampleRate = 4096;
    const signal = sine(440, sampleRate, 0.25).slice(0, 1024);
    const notes = detectNotes(signal, sampleRate, -20);
    assert.ok(notes.length >= 1, 'Should detect at least one note');
    const a4 = notes.find(n => n.note === 'A' && n.octave === 4);
    assert.ok(a4, `Should detect A4, found: ${notes.map(n => n.note + n.octave).join(', ')}`);
  });

  it('detects two simultaneous tones', () => {
    const sampleRate = 8192;
    const N = 2048;
    const f1 = noteFrequency('A', 4); // 440
    const f2 = noteFrequency('E', 5); // 659.26
    const signal = Array.from({ length: N }, (_, i) =>
      Math.sin(2 * Math.PI * f1 * i / sampleRate) +
      Math.sin(2 * Math.PI * f2 * i / sampleRate)
    );
    const notes = detectNotes(signal, sampleRate, -20);
    const noteNames = notes.map(n => n.note);
    assert.ok(noteNames.includes('A'), `Should detect A: ${noteNames}`);
    assert.ok(noteNames.includes('E'), `Should detect E: ${noteNames}`);
  });
});

describe('Chord Detection', () => {
  it('detects C major chord', () => {
    const sampleRate = 8192;
    const chord = generateChord([
      { note: 'C', octave: 4 },
      { note: 'E', octave: 4 },
      { note: 'G', octave: 4 },
    ], sampleRate, 0.25);
    const signal = chord.slice(0, 2048);
    const result = detectChord(signal, sampleRate, -20);
    assert.ok(result.notes.length >= 3, `Should detect 3+ notes: ${result.notes.length}`);
    if (result.chord) {
      assert.ok(result.chord.startsWith('C'), `Expected C chord: ${result.chord}`);
    }
  });

  it('detects A minor chord', () => {
    const sampleRate = 8192;
    const chord = generateChord([
      { note: 'A', octave: 3 },
      { note: 'C', octave: 4 },
      { note: 'E', octave: 4 },
    ], sampleRate, 0.25);
    const signal = chord.slice(0, 2048);
    const result = detectChord(signal, sampleRate, -20);
    assert.ok(result.notes.length >= 3);
  });
});

describe('Generate Chord', () => {
  it('generates correct length', () => {
    const signal = generateChord([440, 550, 660], 44100, 0.5);
    assert.equal(signal.length, 22050);
  });

  it('signal is normalized to [-1, 1]', () => {
    const signal = generateChord([440, 550, 660], 44100, 0.1);
    const max = Math.max(...signal.map(Math.abs));
    assert.ok(max <= 1.001);
    assert.ok(max > 0.9);
  });

  it('accepts note objects', () => {
    const signal = generateChord([
      { note: 'C', octave: 4 },
      { note: 'E', octave: 4 },
    ], 44100, 0.1);
    assert.ok(signal.length > 0);
  });
});

describe('ASCII Spectrogram', () => {
  it('renders without error', () => {
    const sampleRate = 4096;
    const signal = sine(440, sampleRate, 0.5).slice(0, 2048);
    const result = asciiSpectrogram(signal, sampleRate, {
      windowSize: 256, hopSize: 128, width: 40, height: 12,
    });
    assert.ok(typeof result === 'string');
    assert.ok(result.length > 0);
    assert.ok(result.includes('Hz'));
  });

  it('shows frequency content', () => {
    const sampleRate = 4096;
    const signal = sine(500, sampleRate, 0.5).slice(0, 2048);
    const result = asciiSpectrogram(signal, sampleRate, {
      windowSize: 256, hopSize: 128, maxFreq: 1000, width: 30, height: 10,
    });
    // Should have some non-space characters in the row near 500 Hz
    assert.ok(result.includes('█') || result.includes('▓') || result.includes('▒') || result.includes('░'),
      'Should show activity at signal frequency');
  });
});

describe('DTMF', () => {
  it('generates valid signal', () => {
    const signal = dtmfGenerate('5', 8000, 0.1);
    assert.ok(signal.length > 0);
    assert.equal(signal.length, 800);
  });

  it('detects digit 5', () => {
    const sampleRate = 8000;
    const signal = dtmfGenerate('5', sampleRate, 0.1);
    const detected = dtmfDetect(signal, sampleRate);
    assert.equal(detected, '5');
  });

  it('detects all digits', () => {
    const sampleRate = 8000;
    const keys = '0123456789*#ABCD';
    for (const key of keys) {
      const signal = dtmfGenerate(key, sampleRate, 0.1);
      const detected = dtmfDetect(signal, sampleRate);
      assert.equal(detected, key, `Failed to detect DTMF key '${key}', got '${detected}'`);
    }
  });

  it('detects in noisy signal', () => {
    const sampleRate = 8000;
    const signal = dtmfGenerate('7', sampleRate, 0.1);
    // Add some noise
    const noisy = signal.map(v => v + (Math.random() - 0.5) * 0.3);
    const detected = dtmfDetect(noisy, sampleRate);
    assert.equal(detected, '7');
  });

  it('roundtrips phone number', () => {
    const sampleRate = 8000;
    const number = '5551234';
    const detected = [];
    for (const digit of number) {
      const signal = dtmfGenerate(digit, sampleRate, 0.05);
      detected.push(dtmfDetect(signal, sampleRate));
    }
    assert.equal(detected.join(''), number);
  });
});
