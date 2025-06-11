'use strict';

import axios from 'axios';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';

import { BIOS_ENDPOINT } from './tutils';

/**
 * @group it
 */

describe('Get contexts', () => {

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

  afterEach(async () => {
    await bios.signOut();
  });

  it('No options', async () => {
    const contexts = await bios.getContexts();
    expect(contexts.length).toBeGreaterThanOrEqual(3);
    const least = contexts.find((context) => context.contextName === 'contextLeast');
    const allTypes = contexts.find((context) => context.contextName === 'contextAllTypes');
    expect(least).toBeDefined();
    expect(Object.keys(least).length).toBe(1);
    expect(allTypes).toBeDefined();
    expect(Object.keys(allTypes).length).toBe(1);
  });

  it('Test includeInternal option', async () => {
    // try without the option first
    {
      const contexts = await bios.getContexts();
      const ip2geo = contexts.find((context) => context.contextName === '_ip2geo');
      expect(ip2geo).toBeUndefined();
    }
    // then try the option
    {
      const contexts = await bios.getContexts({
        includeInternal: true,
      });
      const ip2geo = contexts.find((context) => context.contextName === '_ip2geo');
      expect(ip2geo).toBeDefined();
    }
    // also test setting includeInternal: false
    {
      const contexts = await bios.getContexts({
        includeInternal: false,
      });
      const ip2geo = contexts.find((context) => context.contextName === '_ip2geo');
      expect(ip2geo).toBeUndefined();
    }
  });

  it('From ingest user', async () => {
    await bios.signOut();
    await bios.signIn({
      email: 'ingest@jsSimpleTest',
      password: 'ingest',
    });
    const contexts = await bios.getContexts();
    expect(contexts.length).toBeGreaterThanOrEqual(3);
  });

  it('From extract user', async () => {
    await bios.signOut();
    await bios.signIn({
      email: 'extract@jsSimpleTest',
      password: 'extract',
    });
    const contexts = await bios.getContexts();
    expect(contexts.length).toBeGreaterThanOrEqual(3);
  });

  it('Specify one context', async () => {
    const contexts = await bios.getContexts({ contexts: ['contextLeast'] });
    expect(contexts.length).toBe(1);
    expect(contexts[0].contextName).toBe('contextLeast');
  });

  it('The getContext method', async () => {
    const context = await bios.getContext('contextLeast');
    // console.debug('signals:', signals);
    expect(context).toBeDefined();
    expect(context.contextName).toBe('contextLeast');
  });

  it('Specify two contexts', async () => {
    const contexts = await bios.getContexts({ contexts: ['contextAllTypes', 'contextLeast'] });
    expect(contexts.length).toBe(2);
    const least = contexts.find((context) => context.contextName === 'contextLeast');
    const allTypes = contexts.find((context) => context.contextName === 'contextAllTypes');
    expect(least).toBeDefined();
    expect(Object.keys(least).length).toBe(1);
    expect(allTypes).toBeDefined();
    expect(Object.keys(allTypes).length).toBe(1);
  });

  it('Add detail option', async () => {
    const contexts = await bios.getContexts({
      contexts: ['contextLeast'],
      detail: true,
    });
    expect(contexts.length).toBe(1);
    const clone = { ...contexts[0] };
    expect(clone.version).toBeGreaterThan(0);
    expect(clone.biosVersion).toBeGreaterThan(0);
    delete clone.version;
    delete clone.biosVersion;
    expect(clone).toEqual({
      contextName: 'contextLeast',
      missingAttributePolicy: 'Reject',
      attributes: [
        {
          attributeName: 'ip',
          type: 'String',
          tags: {
            category: 'Dimension',
          },
        },
        {
          attributeName: 'site',
          type: 'String',
          tags: {
            category: 'Description',
          },
        }
      ],
      primaryKey: ['ip'],
      auditEnabled: true,
      isInternal: false,
    });
  });

  it('Try fetching non-existing context', async () => {
    try {
      await bios.getContexts({
        contexts: ['noSuchContext'],
        detail: true,
      });
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_STREAM.errorCode);
    }
  });
});
