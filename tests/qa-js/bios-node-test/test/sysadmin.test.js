import { BiosClient } from 'bios-sdk';

import { BIOS_ENDPOINT, createRandomString } from './tutils';

describe('Registration', () => {

  const sessionConfig = {
    endpoint: BIOS_ENDPOINT,
    email: 'superadmin',
    password: 'superadmin',
  };

  let client = null;

  beforeAll(async () => {
    client = new BiosClient(sessionConfig);
    client.initialize();
  });

  test('Shared properties', async () => {
    const session = await client.getSession();

    const nodes = await session.getProperty('nodes');
    expect(nodes).toMatch(/https/i);

    await session.setProperty('abcde', '12345');
    const value = await session.getProperty('abcde');
    expect(value).toBe('12345');
  });
});
