/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';

import {
  BIOS_ENDPOINT,
  clearTenant,
} from './tutils';

describe('Context Config operations test', () => {

  const tenantName = 'jsContextConfigTest';

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
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({

      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, true);
    await bios.signOut();
  });

  beforeEach(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: `admin@${tenantName}`,
      password: 'admin',
    });
  });

  afterEach(async () => {
    await bios.signOut();
  });

  it('createUpdateContextConfig', async () => {
    const contextConfig = {
      contextName: 'Ctx0609211924ravid1test',
      description: '_ a test description with special numbers and symbols 0123456789 !@#$%^&*() `~-_=+[{]}\|;:,<.>/?',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'transaction_id', 'type': 'Integer' },
        {
          attributeName: 'product_name', type: 'String',
          missingAttributePolicy: 'StoreDefaultValue', default: 'missing_product_name'
        },
        {
          attributeName: 'manufacturer_name', type: 'String',
          missingAttributePolicy: 'StoreDefaultValue', default: 'missing_manufacturer_name'
        },
        { attributeName: 'price', type: 'Decimal' },
        {
          attributeName: 'asfdasdf', type: 'Decimal',
          missingAttributePolicy: 'StoreDefaultValue', default: 0
        }
      ],
      primaryKey: ['transaction_id'],
    };
    const response = await bios.createContext(contextConfig);
    expect(response).toBeDefined();
    expect(response.contextName).toBe(contextConfig.contextName);
    expect(response.version).toBeGreaterThan(0);

    const retrieved = await bios.getContexts({
      contexts: ['Ctx0609211924ravid1test'],
      detail: true,
    });
    expect(retrieved.length).toBe(1);
    expect(retrieved[0].version).toBe(response.version);
    delete retrieved[0].version;
    delete retrieved[0].biosVersion;
    delete retrieved[0].attributes[0].category;
    delete retrieved[0].attributes[1].category;
    delete retrieved[0].attributes[2].category;
    delete retrieved[0].attributes[3].category;
    delete retrieved[0].attributes[4].category;
    delete retrieved[0].attributes[3].positiveIndicator;
    delete retrieved[0].attributes[4].positiveIndicator;
    expect(retrieved[0]).toEqual({
      ...contextConfig,
      auditEnabled: true,
      isInternal: false,
    });

    delete contextConfig.description;
    const response2 = await bios.updateContext('Ctx0609211924ravid1test', contextConfig);
    expect(response2.contextName).toBe(contextConfig.contextName);
    expect(response2.version).toBeGreaterThan(0);
    expect(response2.description).toBe(undefined);

    const retrieved2 = await bios.getContexts({
      contexts: ['Ctx0609211924ravid1test'],
      detail: true,
    });
    expect(retrieved2.length).toBe(1);
    expect(retrieved2[0].contextName).toBe(response2.contextName);
    expect(retrieved2[0].version).toBe(response2.version);
    expect(retrieved2[0].description).toBe(undefined);
  });

  it('crudContextConfigWithCompositeKey', async () => {
    const contextName =  'contextWithCompositeKey';
    const contextConfig = {
      contextName,
      description: 'Composite key',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'userId', type: 'Integer' },
        { attributeName: 'transactionId', type: 'Integer' },
        {
          attributeName: 'productName', type: 'String',
          missingAttributePolicy: 'StoreDefaultValue', default: 'missing_product_name'
        },
        {
          attributeName: 'manufacturerName', type: 'String',
          missingAttributePolicy: 'StoreDefaultValue', default: 'missing_manufacturer_name'
        },
        { attributeName: 'price', type: 'Decimal' },
        {
          attributeName: 'asfdasdf', type: 'Decimal',
          missingAttributePolicy: 'StoreDefaultValue', default: 0
        }
      ],
      primaryKey: ['userId', 'transactionId'],
    };
    const response = await bios.createContext(contextConfig);
    expect(response).toBeDefined();
    expect(response.contextName).toBe(contextConfig.contextName);
    expect(response.version).toBeGreaterThan(0);

    const retrieved = await bios.getContexts({
      contexts: [contextName],
      detail: true,
    });
    expect(retrieved.length).toBe(1);
    expect(retrieved[0].version).toBe(response.version);
    delete retrieved[0].version;
    delete retrieved[0].biosVersion;
    delete retrieved[0].attributes[0].category;
    delete retrieved[0].attributes[1].category;
    delete retrieved[0].attributes[2].category;
    delete retrieved[0].attributes[3].category;
    delete retrieved[0].attributes[4].category;
    delete retrieved[0].attributes[3].positiveIndicator;
    delete retrieved[0].attributes[4].positiveIndicator;
    expect(retrieved[0]).toEqual({
      ...contextConfig,
      auditEnabled: true,
      isInternal: false,
    });

    delete contextConfig.description;
    const response2 = await bios.updateContext(contextName, contextConfig);
    expect(response2.contextName).toBe(contextConfig.contextName);
    expect(response2.version).toBeGreaterThan(0);
    expect(response2.description).toBe(undefined);

    const retrieved2 = await bios.getContexts({
      contexts: [contextName],
      detail: true,
    });
    expect(retrieved2.length).toBe(1);
    expect(retrieved2[0].contextName).toBe(response2.contextName);
    expect(retrieved2[0].version).toBe(response2.version);
    expect(retrieved2[0].description).toBe(undefined);

    await bios.deleteContext(contextName);

    try {
      const retrieved2 = await bios.getContexts({
        contexts: [contextName],
        detail: true,
      });
        fail('exception must be thrown');
    } catch (e) {
      // ok
    }
  });
});
