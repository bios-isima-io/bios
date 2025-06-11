/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';

import {
  BIOS_ENDPOINT,
  clearTenant,
  sleep,
} from './tutils';

describe('Feature status and refresh test', () => {

  const tenantName = 'jsFeatureTest';

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, false);
    await bios.createTenant({ tenantName });
    await bios.signOut();
  });

  afterAll(async () => {
    await bios.signIn({
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, true);
    await bios.signOut();
  });

  beforeEach(async () => {
    await bios.signIn({
      email: `admin@${tenantName}`,
      password: 'admin',
    });
  });

  afterEach(async () => {
    await bios.signOut();
  });

  it('featureStatusRefresh', async () => {
    const contextConfig = {
      contextName: "contextWithFeature",
      missingAttributePolicy: "Reject",
      attributes: [
        {attributeName: "storeId", type: "Integer"},
        {attributeName: "zipCode", type: "Integer"},
        {attributeName: "address", type: "String"},
        {attributeName: "numProducts", type: "Integer"},
      ],
      primaryKey: ["storeId"],
      auditEnabled: true,
      features: [
        {
          featureName: "byZipCode",
          dimensions: ["zipCode"],
          attributes: ["numProducts"],
          featureInterval: 15000,
          aggregated: true
        }
      ]
    }

    const response = await bios.createContext(contextConfig);
    expect(response.contextName).toBe(contextConfig.contextName);
    const streamName = contextConfig.contextName;

    // Check status of feature before and after requesting a refresh.
    let status = await bios.featureStatus(streamName, "byZipCode");
    // console.log(status);
    expect(status.refreshRequested).toBe(false);

    await bios.featureRefresh(streamName, "byZipCode");

    status = await bios.featureStatus(streamName, "byZipCode");
    // console.log(status);
    expect(status.refreshRequested).toBe(true);

    // wait 20s to get the rollup done
    await sleep(20000);

    let status1 = await bios.featureStatus(streamName, "byZipCode");
    // console.log(status1);
    expect(status1.refreshRequested).toBe(false);
    expect(status1.doneUntil).toBeGreaterThan(status.doneUntil);

    // Specify the entire features, indexes, and sketches
    status = await bios.featureStatus(streamName);
    // console.log(status);
    expect(status.refreshRequested).toBe(false);

    await bios.featureRefresh(streamName);

    status = await bios.featureStatus(streamName);
    // console.log(status);
    expect(status.refreshRequested).toBe(true);

    // wait 20s to get the rollup done
    await sleep(20000);

    status1 = await bios.featureStatus(streamName);
    // console.log(status1);
    expect(status1.refreshRequested).toBe(false);
    expect(status1.doneUntil).toBeGreaterThan(status.doneUntil);

    // invalid context name
    try {
      await bios.featureStatus('noSuchStream');
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBeTrue();
      expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_STREAM.errorCode);
    }

    // invalid feature
    try {
      await bios.featureStatus(streamName, 'noSuchFeature');
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBeTrue();
      expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_STREAM.errorCode);
    }
  });
});
