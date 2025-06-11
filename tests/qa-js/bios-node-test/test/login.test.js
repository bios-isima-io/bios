'use strict';

import { BiosClient } from 'bios-sdk';

import { BIOS_ENDPOINT } from './tutils';

describe('Login', () => {

  const sessionConfig = {
    endpoint: BIOS_ENDPOINT,
    email: 'superadmin',
    password: 'superadmin',
  };

  describe('Use starter', () => {

    test('Simple login', async () => {
      const client = BiosClient.start(sessionConfig);
      const session = await client.getSession();
      const tenants = await session.listTenantNames();
      expect(tenants.length).toBeGreaterThan(0);
      const signals = await client.getSession()
        .then((session) => session.getSignals({ signals: ['_operations'] }))
      expect(signals.length).toBeGreaterThan(0);
    });

    test('Break auth token', async () => {
      const session = await BiosClient.start(sessionConfig).getSession();

      session.httpClient.defaults.headers['authorization'] = 'broken';

      const tenants = await session.listTenantNames();
      expect(tenants.length).toBeGreaterThan(0);
    });

    test('Wrong password', async () => {
      await expect(BiosClient.start({
        ...sessionConfig,
        password: 'wrong',
      }).getSession()).rejects.toThrow('Login failed');
    });

  });

  describe('Use initialize method', () => {
    let client = null;

    beforeAll(() => {
      client = new BiosClient(sessionConfig);
      client.initialize();
    });

    test('Simple login', async () => {
      const session = await client.getSession();
      const tenants = await session.listTenantNames();
      expect(tenants.length).toBeGreaterThan(0);
      const signals = await client.getSession()
        .then((session) => session.getSignals({ signals: ['_operations'] }))
      expect(signals.length).toBeGreaterThan(0);
    });

    test('Break auth token', async () => {
      const session = await client.getSession();

      session.httpClient.defaults.headers['authorization'] = 'broken';

      const tenants = await session.listTenantNames();
      expect(tenants.length).toBeGreaterThan(0);
    });
  });

  describe('Initialization Failure', () => {
    let client = null;

    beforeAll(() => {
      client = new BiosClient({ ...sessionConfig, password: 'superadminx' });
      client.initialize();
    });

    test('Wrong password', async () => {
      await expect(client.getSession()).rejects.toThrow('Login failed');
    });
  });
});
