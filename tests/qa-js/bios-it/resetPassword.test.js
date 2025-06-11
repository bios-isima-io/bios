'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';

import {
  BIOS_ENDPOINT,
  clearTenant,
} from './tutils';

describe('Reset password tests', () => {
  const tenantName = 'jschangePasswordTest';

  const ingestUser = `ingest@${tenantName}`;
  const ingestPass = 'ingest';
  const adminUser = `admin@${tenantName}`;
  const adminPass = 'admin';

  const verifyUnauthorized = async (name, func) => {
    try {
      await func();
      fail(`exception is expected: ${name}`);
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.INVALID_PASSWORD.errorCode);
    }
  };

  const verifyForbidden = async (name, func) => {
    try {
      await func();
      fail(`exception is expected: ${name}`);
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.FORBIDDEN.errorCode);
    }
  };

  beforeAll(async () => {
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, false);
    await bios.createTenant({
      tenantName,
    });
    await bios.logout();
  });

  afterAll(async () => {
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, true);
    await bios.logout();
  });

  afterEach(async () => {
    await bios.logout();
  });

  it('Self password reset', async () => {
    const newPassword = 'al09knenva;lkj2@^*0-1k';
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: ingestUser,
      password: ingestPass,
    });
    await verifyForbidden('Reset with wrong current password', () => bios.changePassword({
      currentPassword: ingestPass + 'wrong',
      newPassword: newPassword,
    }));
    await bios.changePassword({
      currentPassword: ingestPass,
      newPassword: newPassword,
    });
    await bios.logout();
    await verifyUnauthorized('Login with the old password', () => bios.login({
      endpoint: BIOS_ENDPOINT,
      email: ingestUser,
      password: ingestPass,
    }));
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: ingestUser,
      password: newPassword,
    });
    await bios.changePassword({
      currentPassword: newPassword,
      newPassword: ingestPass,
    });
  });

  it('Cross-user password reset', async () => {
    const newPassword = '798\+EN=MZ<r[x?N7;';
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: adminUser,
      password: adminPass,
    });
    await bios.changePassword({
      email: ingestUser,
      newPassword: newPassword,
    });
    await bios.logout();
    await verifyUnauthorized('Login with the old password', () => bios.login({
      endpoint: BIOS_ENDPOINT,
      email: ingestUser,
      password: ingestPass,
    }));
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: ingestUser,
      password: newPassword,
    });
    await bios.logout();
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: adminUser,
      password: adminPass,
    });
    await bios.changePassword({
      email: ingestUser,
      newPassword: ingestPass,
    });
  });

  // TODO(Naoki): Figure out a way to generate the token dynamically.
  // This test cannot be run for a long term since the token expires.
  xit('Try resetting password of a non-existing user', async () => {
    const token = 'ZXlKaGJHY2lPaUpJVXpVeE1pSjkuZXlKemRXSWlPaUlrTW1Fa01URWtNVE5rZDNOWVluWjNXVFYwVW5'
      + 'sQlNqaG1hSEJOWlZsT2JXRnNUblZHZEhCTGRVUjZibE5DZGk0dkxtMU5ibEpDUWk1S1IxTWlMQ0pwWVhRaU9qRTNN'
      + 'REU1TURRM05qZ3NJbTVpWmlJNk1UY3dNVGt3TkRjMk9Dd2laWGh3SWpveE56QXlNRGMzTlRZNExDSjFjMlZ5SWpve'
      + 'E5qRTBOemszTWpZNE56WXdMQ0p2Y21jaU9qRTJNVEExTmpnMU56a3dNamg5LnFCYndSenZCMjhRUE9ZdGxwQkhHYW1'
      + 'YcF9uWmVFdFNWOW1ybG5FejBLcTUzZ3NJWWdoemtPRU8zMFFNRWF2X3FmQ29Cck1rX29QTkV2YWx4d2dVbVpn';
    try {
      await bios.completeResetPassword(token, 'newPassword');
      fail('exception is expected');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.UNAUTHORIZED.errorCode);
    }
  });
});
