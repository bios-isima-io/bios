'use strict';

import bios from 'bios-sdk';
import { BIOS_ENDPOINT } from './tutils';



/**
 * @group it
 */

describe('Get tenant', () => {

  beforeAll(async () => {
    await bios.global.login({
      endpoint: BIOS_ENDPOINT,
      email: 'admin@jsSimpleTest',
      password: 'admin',
    });
    return null;
  });

  afterAll(async () => {
    await bios.logout();
  });

  it('No options', async () => {
    const tenant = await bios.getTenant();
    expect(tenant).toBeDefined();
    expect(tenant.tenantName).toBe('jsSimpleTest');
    expect(tenant.version).toBeGreaterThan(0);
    expect(tenant.signals.length).toBeGreaterThanOrEqual(6);
    expect(tenant.contexts.length).toBeGreaterThanOrEqual(3);
    expect(tenant.status).toBe('Active');
    const minimum = tenant.signals.find((signal) => signal.signalName === 'minimum');
    const simpleRollup = tenant.signals.find((signal) => signal.signalName === 'simpleRollup');
    const rollupTwoValues = tenant.signals.find((signal) => signal.signalName === 'rollupTwoValues');
    const usage =
      tenant.signals.find((signal) => signal.signalName === '_usage');
    const contextLeast = tenant.contexts.find((context) => context.contextName === 'contextLeast');
    const contextAllTypes =
      tenant.contexts.find((context) => context.contextName === 'contextAllTypes');
    expect(minimum).toBeDefined();
    expect(simpleRollup).toBeDefined();
    expect(rollupTwoValues).toBeDefined();
    expect(usage).toBeDefined();
    expect(contextLeast).toBeDefined();
    expect(contextAllTypes).toBeDefined();
    expect(Object.keys(minimum).length).toBe(1);
    expect(Object.keys(contextLeast).length).toBe(1);
  });

  it('Detail option', async () => {
    const tenant = await bios.getTenant({ detail: true });
    expect(tenant).toBeDefined();
    expect(tenant.tenantName).toBe('jsSimpleTest');
    expect(tenant.version).toBeGreaterThan(0);
    expect(tenant.signals.length).toBeGreaterThanOrEqual(6);
    expect(tenant.contexts.length).toBeGreaterThanOrEqual(3);
    expect(tenant.status).toBe('Active');
    const minimum = tenant.signals.find((signal) => signal.signalName === 'minimum');
    const simpleRollup = tenant.signals.find((signal) => signal.signalName === 'simpleRollup');
    const rollupTwoValues = tenant.signals.find((signal) => signal.signalName === 'rollupTwoValues');
    const usage =
      tenant.signals.find((signal) => signal.signalName === '_usage');
    const contextLeast = tenant.contexts.find((context) => context.contextName === 'contextLeast');
    const contextAllTypes =
      tenant.contexts.find((context) => context.contextName === 'contextAllTypes');
    const auditContextLeast = tenant.signals.find((signal) => signal.signalName === 'auditContextLeast');
    expect(minimum).toBeDefined();
    expect(minimum.attributes).toBeDefined();
    expect(minimum.attributes[0].inferredTags).toBeUndefined();
    expect(minimum.isInternal).toBe(false);
    expect(simpleRollup).toBeDefined();
    expect(simpleRollup.isInternal).toBe(false);
    expect(rollupTwoValues).toBeDefined();
    expect(rollupTwoValues.isInternal).toBe(false);
    expect(usage).toBeDefined();
    expect(usage.isInternal).toBe(true);
    expect(contextLeast).toBeDefined();
    expect(contextLeast.primaryKey).toBeDefined();
    expect(contextLeast.attributes[0].inferredTags).toBeUndefined();
    expect(contextLeast.isInternal).toBe(false);
    expect(auditContextLeast).toBeDefined();
    expect(auditContextLeast.isInternal).toBe(true);
    expect(contextAllTypes).toBeDefined();
    expect(contextAllTypes.isInternal).toBe(false);
  });

  it('InferredTags option', async () => {
    // Ensure to have all signals get populated with inferred tags
    await new Promise(resolve => setTimeout(resolve, 20000));

    const tenant = await bios.getTenant({ detail: true, inferredTags: true });
    expect(tenant).toBeDefined();
    expect(tenant.tenantName).toBe('jsSimpleTest');
    expect(tenant.version).toBeGreaterThan(0);
    expect(tenant.signals.length).toBeGreaterThanOrEqual(6);
    expect(tenant.contexts.length).toBeGreaterThanOrEqual(3);
    expect(tenant.status).toBe('Active');
    const minimum = tenant.signals.find((signal) => signal.signalName === 'minimum');
    const simpleRollup = tenant.signals.find((signal) => signal.signalName === 'simpleRollup');
    const rollupTwoValues = tenant.signals.find((signal) => signal.signalName === 'rollupTwoValues');
    const met =
      tenant.signals.find((signal) => signal.signalName === '_usage');
    const contextLeast = tenant.contexts.find((context) => context.contextName === 'contextLeast');
    const contextAllTypes =
      tenant.contexts.find((context) => context.contextName === 'contextAllTypes');
    expect(minimum).toBeDefined();
    expect(minimum.attributes[0].inferredTags).toBeDefined();
    expect(simpleRollup).toBeDefined();
    expect(rollupTwoValues).toBeDefined();
    expect(met).toBeDefined();
    expect(contextLeast).toBeDefined();
    expect(contextAllTypes).toBeDefined();
    expect(minimum.attributes).toBeDefined();
    expect(contextLeast.primaryKey).toBeDefined();
  });
});
