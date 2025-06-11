/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, {
  BiosClientError,
  BiosErrorType
} from 'bios-sdk';
import {
  BIOS_ENDPOINT, sleep
} from './tutils';

describe('Session expiration test', () => {

  const SESSION_EXPIRY = "session_expiry";
  const FIFTEEN_SECONDS = '15000';

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'superadmin',
      password: 'superadmin',
    });
    // Set session expiration time to 15 seconds
    await bios.setProperty(SESSION_EXPIRY, FIFTEEN_SECONDS);
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
    // Reset the session expiration time
    await bios.setProperty(SESSION_EXPIRY, '');
    await bios.signOut();
  });

  var flag = false;
  const unauthorizedErrorInterceptor = (error) => {
    flag = true;
  };

  beforeEach(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
      unauthorizedErrorInterceptor,
    });
    await bios.signIn({
      email: 'admin@jsSimpleTest',
      password: 'admin',
    });
    flag = false;
  });

  afterEach(async () => {
    bios.signOut();
  });

  it('Session timeout', async () => {
    // sleep for 18 seconds
    await sleep(18000);

    // Session should be expired at this point of time
    try {
      await bios.getSignals();
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.SESSION_EXPIRED.errorCode);
    }

    // Verify that the interceptor has been called
    expect(flag).toBe(true);

    // Try again and make sure it does not cause any crash
    try {
      await bios.getSignals();
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.SESSION_EXPIRED.errorCode);
    }
  });

  it('Session renewal', async () => {
    // keep calling methods for 20 seconds
    for (var i = 0; i < 7; ++i) {
      await sleep(3000);
      await bios.getSignals();
    }

    // Session has been alive, the interceptor has not been called
    expect(flag).toBe(false);
  });
});
