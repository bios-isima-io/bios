/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';
import { BIOS_ENDPOINT, sleep } from './tutils';

xdescribe('Signup', () => {

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
  });

  const createRandomString = (length) => {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let result = "";
    for (let i = 0; i < length; i++) {
      result += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return result;
  }

  xdescribe('Service registration', () => {
    it('Successful registration', async () => {
      const primaryEmail = `user-${createRandomString(5)}@isima.io`;
      const additionalEmails = [`user-${createRandomString(5)}@isima.io`];
      const service = 'Analytics';
      const source = 'Wordpress';
      const subdomain = createRandomString(5);
      const domain = `${subdomain}.example.com`;
      const tenantName = `${subdomain}_example_com`;

      // Initiate app registration
      var initiateResponse = await bios.initiateServiceRegistration(primaryEmail, service, source,
        domain, additionalEmails);
      expect(initiateResponse).toBeDefined();
      expect(initiateResponse.tenantName).toEqual(tenantName);

      // Approve registration
      const data = new FormData();
      data.append('token', initiateResponse.token);
      data.append('action', 'Approve');

      const reply = await bios._getGlobalSession().httpClient.post(
        '/bios/v1/signup/approve', data, {
        headers: {
          'content-type': 'multipart/form-data',
        }
      });
      const token = reply.data;
      expect(token).toBeDefined();

      // Verify the primary user
      const verifyResponse = await bios.verifySignupToken(token);
      const verifyToken = verifyResponse.token;
      expect(verifyToken).toBeDefined();

      // Complete registration
      const result = await bios.completeSignup(verifyToken, 'strongPassword', 'Test User');

      // Check whether the tenant and user has been created
      let count = 0;
      let tenant = null;
      while (true) {
        try {
          ++count;
          await bios.signIn({
            email: primaryEmail,
            password: 'strongPassword',
          });
          tenant = await bios.getTenant();
          break
        } catch (e) {
          if (count > 60) {
            throw e;
          }
        }
        await sleep(1000);
      }
      expect(tenant.tenantName).toEqual(tenantName);
    });
  });
});
