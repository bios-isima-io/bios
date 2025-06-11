/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios from 'bios-sdk';

import {
  BIOS_ENDPOINT,
  clearTenant,
} from './tutils';

describe('Signal Config operations test', () => {

  const tenantName = 'jsSignalConfigTest';

  beforeAll(async () => {
    await bios.global.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, false);
    await bios.createTenant({ tenantName });
    await bios.logout();
  });

  afterAll(async () => {
    await bios.global.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, true);
    await bios.logout();
  });

  beforeEach(async () => {
    await bios.global.login({
      endpoint: BIOS_ENDPOINT,
      email: `admin@${tenantName}`,
      password: 'admin',
    });
  });

  afterEach(async () => {
    await bios.logout();
  });

  it('createUpdateSignalConfig', async () => {
    const signalConfig = {
      signalName: 'signalTestSignalTest',
      description: 'A test description with special numbers and symbols 0123456789 !@#$%^&*() `~-_=+[{]}\|;:,<.>/?',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'transaction_id', type: 'Integer' },
        {
          attributeName: 'product_name', type: 'String',
          tags: {
            category: 'Dimension'
          },
          missingAttributePolicy: 'StoreDefaultValue',
          default: 'missing_product_name',
        },
        {
          attributeName: 'manufacturer_name', type: 'String',
          tags: {
            category: 'Dimension'
          },
          missingAttributePolicy: 'StoreDefaultValue',
          default: 'missing_manufacturer_name',
        },
        { attributeName: 'price', type: 'Decimal' },
        { attributeName: 'units_sold', type: 'Integer' },
        {
          attributeName: 'overhead', type: 'Decimal',
          tags: {
            category: 'Quantity',
            kind: 'Dimensionless',
            unit: 'Ratio',
            positiveIndicator: 'Low',
          },
          missingAttributePolicy: 'StoreDefaultValue',
          default: 0,
        }
      ],
      postStorageStage: {
        features: [
          {
            featureName: 'byProductName',
            dimensions: ['product_name'],
            attributes: ['price', 'units_sold'],
            featureInterval: 300000,
            alerts: [
              {
                alertName: 'priceOutOfRange',
                condition: '((min(price) < 100) OR (max(price) > 1000))',
                webhookUrl: 'https://localhost:8443'
              },
              {
                alertName: 'countExceedsThreshold',
                condition: '(count() > 10000)',
                webhookUrl: 'https://localhost:8443'
              },
              {
                alertName: 'countEqualsThreshold',
                condition: '(count() == 10000)',
                webhookUrl: 'https://localhost:8443'
              },
              {
                alertName: 'sumOfPriceExceedsThreshold',
                condition: '(sum(price) > 10000)',
                webhookUrl: 'https://localhost:8443'
              },
              {
                alertName: 'abc123',
                condition: '((sum(price) / sum(units_sold)) > 10000)',
                webhookUrl: 'https://localhost:8443'
              },
              {
                alertName: 'threeConditions',
                condition: '(((min(price) <= 100) OR (max(price) >= 1000)) AND (count() > 50))',
                webhookUrl: 'https://localhost:8443'
              },
            ]
          }
        ]
      }
    };
    const response = await bios.createSignal(signalConfig);
    expect(response).toBeDefined();
    expect(response.signalName).toBe(signalConfig.signalName);
    expect(response.version).toBeGreaterThan(0);

    const retrieved = await bios.getSignals({
      signals: ['signalTestSignalTest'],
      detail: true,
    });
    expect(retrieved.length).toBe(1);
    expect(retrieved[0].signalName).toBe(response.signalName);
    expect(retrieved[0].version).toBe(response.version);
    delete retrieved[0].version;
    delete retrieved[0].biosVersion;
    expect(retrieved[0]).toEqual({
      ...signalConfig,
      isInternal: false,
    });

    signalConfig.description = 'Updated description.';
    const response2 = await bios.updateSignal('signalTestSignalTest', signalConfig);
    expect(response2.signalName).toBe(signalConfig.signalName);
    expect(response2.version).toBeGreaterThan(0);
    expect(response2.description).toBe(signalConfig.description);

    const retrieved2 = await bios.getSignals({
      signals: ['signalTestSignalTest'],
      detail: true,
    });
    expect(retrieved2.length).toBe(1);
    expect(retrieved2[0].signalName).toBe(response2.signalName);
    expect(retrieved2[0].version).toBe(response2.version);
    expect(retrieved2[0].description).toBe(response2.description);

    // Create a new signal without a description, then update it to containa description.
    delete signalConfig.description;
    signalConfig.signalName = 'signalWithoutDescriptionAtFirst';
    const response3 = await bios.createSignal(signalConfig);
    expect(response3.signalName).toBe(signalConfig.signalName);
    expect(response3.version).toBeGreaterThan(0);
    expect(response3.description).toBe(undefined);

    const retrieved3 = await bios.getSignals({
      signals: ['signalWithoutDescriptionAtFirst'],
      detail: true,
    });
    expect(retrieved3.length).toBe(1);
    expect(retrieved3[0].signalName).toBe(response3.signalName);
    expect(retrieved3[0].version).toBe(response3.version);
    expect(retrieved3[0].description).toBe(undefined);

    signalConfig.description = 'Added description.';
    const response4 = await bios.updateSignal('signalWithoutDescriptionAtFirst', signalConfig);
    expect(response4.signalName).toBe(signalConfig.signalName);
    expect(response4.version).toBeGreaterThan(0);
    expect(response4.description).toBe(signalConfig.description);

    const retrieved4 = await bios.getSignals({
      signals: ['signalWithoutDescriptionAtFirst'],
      detail: true,
    });
    expect(retrieved4.length).toBe(1);
    expect(retrieved4[0].signalName).toBe(response4.signalName);
    expect(retrieved4[0].version).toBe(response4.version);
    expect(retrieved4[0].description).toBe(response4.description);
  });
});
