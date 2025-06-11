'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';
import { BIOS_ENDPOINT } from './tutils';



/**
 * @group it
 */

describe('Get simple signals', () => {

  beforeEach(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'admin@jsSimpleTest',
      password: 'admin',
    });
    return null;
  });

  afterAll(async () => {
    await bios.signOut();
  });

  it('From ingest user', async () => {
    await bios.signOut();
    await bios.signIn({
      email: 'ingest@jsSimpleTest',
      password: 'ingest',
    });
    const signals = await bios.getSignals();
    expect(signals.length).toBeGreaterThanOrEqual(5);
    signals.forEach((signal) => {
      expect(signal.version).toBeUndefined();
    });
  });

  it('From extract user', async () => {
    await bios.signOut();
    await bios.signIn({
      email: 'extract@jsSimpleTest',
      password: 'extract',
    });
    const signals = await bios.getSignals();
    expect(signals.length).toBeGreaterThanOrEqual(5);
    signals.forEach((signal) => {
      expect(signal.version).toBeUndefined();
    });
  });

  it('No options', async () => {
    const signals = await bios.getSignals();
    expect(signals.length).toBeGreaterThanOrEqual(5);
    signals.forEach((signal) => {
      expect(signal.version).toBeUndefined();
      expect(signal.signalName.charAt(0)).not.toBe('_');
    });
  });

  it('Option detail=true', async () => {
    const signals = await bios.getSignals({ detail: true });
    expect(signals.length).toBeGreaterThanOrEqual(5);
    signals.forEach((signal) => {
      expect(signal.version).toBeDefined();
      expect(signal.version > 0).toBe(true);
      expect(signal.attributes[0].inferredTags).toBeUndefined();
    });
  });

  it('Option detail=false', async () => {
    const signals = await bios.getSignals({ detail: false });
    signals.forEach((signal) => {
      expect(signal.version).toBeUndefined();
    });
  });

  it('Option includeInternal=true', async () => {
    const signals = await bios.getSignals({ includeInternal: true });
    const usage = signals.find((signal) => signal.signalName === '_usage');
    expect(usage).toBeDefined();
  });

  it('Option includeInternal=false', async () => {
    const signals = await bios.getSignals({ includeInternal: false });
    const usage = signals.find((signal) => signal.signalName === '_usage');
    expect(usage).toBeUndefined();
  });

  it('Option inferredTags=true', async () => {
    // Ensure to have all signals get populated with inferred tags
    await new Promise(resolve => setTimeout(resolve, 20000));
    const signals = await bios.getSignals({ detail: true, inferredTags: true });
    expect(signals.length).toBeGreaterThanOrEqual(5);
    signals.forEach((signal) => {
      expect(signal.version).toBeDefined();
      expect(signal.version > 0).toBe(true);
      expect(signal.attributes[0].inferredTags).toBeDefined();
    });
  });

  it('Specify a signal', async () => {
    const signals = await bios.getSignals({
      signals: ['minimum']
    });
    // console.debug('signals:', signals);
    expect(signals.length).toBe(1);
    expect(signals[0].signalName).toBe('minimum');
    expect(signals[0].version).toBeUndefined();
  });

  it('The getSignal method', async () => {
    const signal = await bios.getSignal('minimum');
    // console.debug('signals:', signals);
    expect(signal).toBeDefined();
    expect(signal.signalName).toBe('minimum');
    expect(signal.attributes[0].inferredTags).toBeUndefined();
  });

  it('The getSignal method with inferredTags option', async () => {
    const signal = await bios.getSignal('minimum', { inferredTags: true });
    // console.debug('signals:', signals);
    expect(signal).toBeDefined();
    expect(signal.signalName).toBe('minimum');
    expect(signal.attributes[0].inferredTags).toBeDefined();
  });

  it('Specify two signals', async () => {
    const signals = await bios.getSignals({
      signals: ['_usage', 'minimum']
    });
    // console.debug('signals:', JSON.stringify(signals, null, 2));
    expect(signals.length).toBe(2);
    const minimum = signals.find((signal) => signal.signalName === 'minimum');
    expect(minimum).toBeDefined();
    const systemOp = signals.find((signal) => signal.signalName === '_usage');
    expect(systemOp).toBeDefined();
  });

  // expected failure BB-1071
  xit('Specify a non-existing signal', async () => {
    try {
      await bios.getSignals({
        signals: ['minimum', 'nosuch']
      });
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_SIGNAL.errorCode);
    }
  });
});
