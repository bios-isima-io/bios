/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';
import { BIOS_ENDPOINT, sleep } from './tutils';


describe('Login', () => {
  describe('Sign in', () => {
    afterEach(async () => {
      await bios.signOut();
    });

    it('Valid admin sign in', async () => {
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
      });
      const session = await bios.signIn({
        email: 'admin@jsSimpleTest',
        password: 'admin',
      });
      expect(session.tenantName).toBe('jsSimpleTest');

      const userInfo = await bios.getUserInfo();
      expect(userInfo.email).toBe('admin@jssimpletest');
      expect(userInfo.roles).toEqual(['TenantAdmin']);

      // Get users in the tenant
      const users = await bios.getUsers();
      const expectedUsers = [{
        "email": "mlengineer+report@jssimpletest",
        "fullName": "Test Machine Learning Engineer + Report",
        "roles": [
          "SchemaExtractIngest",
          "Report",
        ],
        "status": "Active",
      },
      {
        "email": "extract+report@jssimpletest",
        "fullName": "Test Extract + Report User",
        "roles": [
          "Extract",
          "Report",
        ],
        "status": "Active",
      },
      {
        "email": "admin+report@jssimpletest",
        "fullName": "Tenant jsSimpleTest Administrator + Report",
        "roles": [
          "TenantAdmin",
          "Report",
        ],
        "status": "Active",
      },
      {
        "email": "report@jssimpletest",
        "fullName": "Report Only User",
        "roles": [
          "Report",
        ],
        "status": "Active",
      },
      {
        "email": "mlengineer@jssimpletest",
        "fullName": "Test Machine Learning Engineer",
        "roles": [
          "SchemaExtractIngest",
        ],
        "status": "Active",
      },
      {
        "email": "extract@jssimpletest",
        "fullName": "Test Extract User",
        "roles": [
          "Extract",
        ],
        "status": "Active",
      },
      {
        "email": "ingest@jssimpletest",
        "fullName": "Test Ingest User",
        "roles": [
          "Ingest",
        ],
        "status": "Active",
      },
      {
        "email": "ingest+report@jssimpletest",
        "fullName": "Test Ingest + Report User",
        "roles": [
          "Ingest",
          "Report",
        ],
        "status": "Active",
      },
      {
        "email": "admin@jssimpletest",
        "fullName": "Tenant jsSimpleTest Administrator",
        "roles": [
          "TenantAdmin",
        ],
        "status": "Active",
      },
      {
        "email": "support+jssimpletest@isima.io",
        "fullName": "Support user",
        "roles": [
          "TenantAdmin",
          "Internal",
        ],
        "status": "Active",
      }].map((entry) => JSON.stringify(entry));

      users.forEach((user) => {
        const toTest = JSON.stringify((({ email, fullName, roles, status }) => ({ email, fullName, roles, status }))(user));
        var found = false;
        for (var i = 0; i < expectedUsers.length; ++i) {
          if (toTest === expectedUsers[i]) {
            found = true;
            break;
          }
        }
        expect(found).withContext(toTest).toBe(true);
      });
    });

    it('Unauthorized response interceptor', async () => {
      var flag = false;
      const unauthorizedErrorInterceptor = (_) => {
        flag = true;
      };
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
        unauthorizedErrorInterceptor,
      });

      expect(flag).toBe(false);

      flag = false;
      const session = await bios.signIn({
        email: 'admin@jsSimpleTest',
        password: 'admin'
      });
      expect(session.tenantName).toBe('jsSimpleTest');
      // The flag should be unchanged
      expect(flag).toBe(false);

      await bios.logout();

      expect(flag).toBe(false);

      try {
        await bios.getUserInfo();
        fail('exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.UNAUTHORIZED.errorCode);
        expect(e.message).toBe(BiosErrorType.UNAUTHORIZED.message);
        expect(flag).toBe(true);
      }

      flag = false;

      try {
        await bios.signIn({
          email: 'admin@jsSimpleTest',
          password: 'wrongPass'
        });
        fail('exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.INVALID_PASSWORD.errorCode);
        expect(e.message).toBe(BiosErrorType.INVALID_PASSWORD.message);
        // verify that the interceptor was called
        expect(flag).toBe(true);
      }
    });

    it('Reloading session', async () => {
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
      });
      const session = await bios.signIn({
        email: 'admin@jsSimpleTest',
        password: 'admin'
      });
      expect(session.tenantName).toBe('jsSimpleTest');

      const userInfo = await bios.getUserInfo();
      expect(userInfo.email).toBe('admin@jssimpletest');
      expect(userInfo.roles).toEqual(['TenantAdmin']);

      // simulate browser reloading
      bios.__clear();
      try {
        await bios.getUserInfo();
        fail('exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NOT_LOGGED_IN.errorCode);
      }

      // Initialize the session again. The session token is still alive,
      // so the session would continue without signing in
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
      });
      const userInfo2 = await bios.getUserInfo();
      expect(bios.tenantName).toBe("jsSimpleTest");
      expect(userInfo2.email).toBe('admin@jssimpletest');
      expect(userInfo2.roles).toEqual(['TenantAdmin']);
      const signals = await bios.getSignals();
      expect(signals.length).toBeGreaterThan(0);
    });
  });

  describe('System admin login', () => {
    beforeAll(() => {
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
      });
    });

    afterEach(async () => {
      await bios.signOut();
    });

    it('run successful login', async () => {
      const session = await bios.signIn({
        email: 'superadmin',
        password: 'superadmin',
        appName: 'bios-qa-test-sadmin',
        appType: 'Adhoc',
      });
      expect(session.tenantName).toBe('_system');

      const userInfo = await bios.getUserInfo();
      expect(userInfo.roles).toEqual(['SystemAdmin']);
      expect(userInfo.roles.map(bios.getRoleName)).toEqual(['System Administrator']);
      expect(userInfo.appName).toBe('bios-qa-test-sadmin');
      expect(userInfo.appType).toBe('Adhoc');
    });

    it('Login failure', async () => {
      try {
        await bios.signIn({
          endpoint: BIOS_ENDPOINT,
          email: 'superadmin',
          password: 'superadminx',
        });
        fail('exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.INVALID_PASSWORD.errorCode);
        expect(e.message).toBe(BiosErrorType.INVALID_PASSWORD.message);
      }
    });
  });

  describe('Admin login', () => {
    beforeAll(() => {
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
      });
    });

    afterEach(async () => {
      await bios.signOut();
    });

    it('Valid admin login', async () => {
      const session = await bios.signIn({
        email: 'admin@jsSimpleTest',
        password: 'admin',
        appName: 'bios-qa-test-admin',
        appType: 'Realtime',
      });
      expect(session.tenantName).toBe('jsSimpleTest');

      const userInfo = await bios.getUserInfo();
      expect(userInfo.email).toBe('admin@jssimpletest');
      expect(userInfo.roles).toEqual(['TenantAdmin']);
      expect(userInfo.appName).toBe('bios-qa-test-admin');
      expect(userInfo.appType).toBe('Realtime');
    });
  });

  describe('Global login', () => {
    afterEach(async () => {
      await bios.logout();
    });

    it('Before login', async () => {
      try {
        await bios.getSignals();
        fail('exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NOT_LOGGED_IN.errorCode);
      }
    });

    it('Run global login', async () => {
      await bios.global.login({
        endpoint: BIOS_ENDPOINT,
        email: 'admin@jsSimpleTest',
        password: 'admin'
      });

      const signals = await bios.getSignals();
      expect(signals.length > 0).toBe(true);

      await bios.logout();
      try {
        await bios.getSignals();
        fail('exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NOT_LOGGED_IN.errorCode);
      }
    });

    it('Run new style of global login', async () => {
      await bios.login({
        endpoint: BIOS_ENDPOINT,
        email: 'admin@jsSimpleTest',
        password: 'admin'
      });

      const signals = await bios.getSignals();
      expect(signals.length > 0).toBe(true);

      await bios.logout();
      try {
        await bios.getSignals();
        fail('exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NOT_LOGGED_IN.errorCode);
      }
    });
  });
});
