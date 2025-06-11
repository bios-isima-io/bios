/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';

import { BIOS_ENDPOINT } from './tutils';

describe('Miscellaneous test', () => {
  let session;
  beforeAll(async () => {
    session = await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
  });

  afterAll(async () => {
    await session.logout();
  });

  it('Server JSON reader check', async () => {
    const httpClient = session._httpClient;
    const response = await httpClient.post('/bios/v1/test/jsontype', [
      'hello',
      1123456,
      20.43,
      true,
    ]);
    const types = response.data;
    expect(types).toEqual([
      'java.lang.String',
      'java.lang.Long',
      'java.lang.Double',
      'java.lang.Boolean',
    ]);
  });
});
