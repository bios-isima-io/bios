/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, {
  BiosClientError,
  BiosErrorType
} from 'bios-sdk';
import { BIOS_ENDPOINT } from './tutils';


describe('Users', () => {
  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
  });

  describe('Create, modify, and delete', () => {

    afterEach(async () => {
      await bios.signOut();
    });

    it('normal case', async () => {
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
      });
      await bios.signIn({
        email: 'superadmin',
        password: 'superadmin'
      });

      try {
        const user = await bios.createUser({
          email: 'js+test@isima.io',
          password: 'secret',
          tenantName: 'jsSimpleTest',
          fullName: 'Test User',
          roles: ['Ingest'],
        });

        expect(user.email).toBe('js+test@isima.io');
        expect(user.fullName).toBe('Test User');
        expect(user.roles).toEqual(['Ingest']);
        expect(user.status).toBe('Active');
        expect(user.userId).toBeDefined();

        await bios.signOut();
        await bios.signIn({
          email: 'admin@jsSimpleTest',
          password: 'admin',
        });

        const users = await bios.getUsers();
        const emails = users.map((user) => user.email);
        expect(emails.includes('js+test@isima.io')).toBe(true);

        const modifiedUser1 = await bios.modifyUser({
          ...user,
          fullName: 'Modified User',
          roles: ['Extract', 'Report'],
        });

        expect(modifiedUser1.email).toBe('js+test@isima.io');
        expect(modifiedUser1.fullName).toBe('Modified User');
        expect(modifiedUser1.roles).toEqual(['Extract', 'Report']);
        expect(modifiedUser1.status).toBe('Active');
        expect(modifiedUser1.userId).toBe(user.userId);

        const modifiedUser2 = await bios.modifyUser({
          ...modifiedUser1,
          status: 'Suspended',
        });

        expect(modifiedUser2.email).toBe('js+test@isima.io');
        expect(modifiedUser2.fullName).toBe('Modified User');
        expect(modifiedUser2.roles).toEqual(['Extract', 'Report']);
        expect(modifiedUser2.status).toBe('Suspended');
        expect(modifiedUser2.userId).toBe(user.userId);

        await bios.signOut();
        await bios.signIn({
          email: 'superadmin',
          password: 'superadmin'
        });
        await bios.deleteUser({ email: 'js+test@isima.io' });

        await bios.signOut();
        await bios.signIn({
          email: 'admin@jsSimpleTest',
          password: 'admin',
        });
        const usersAfterDeletion = await bios.getUsers();
        const emailsAfterDeletion = usersAfterDeletion.map((user) => user.email);
        expect(emailsAfterDeletion.includes('js+test@isima.io')).toBe(false);

      } finally {
        try {
          await bios.signOut();
          await bios.signIn({
            email: 'superadmin',
            password: 'superadmin'
          });
          await bios.deleteUser({ email: 'js+test@isima.io' });
        } catch (e) {
          // it's ok
        }
      }
    });

    it('delete user by ID', async () => {
      await bios.signOut();
      await bios.signIn({
        email: 'admin@jsSimpleTest',
        password: 'admin',
      });

      try {
        try {
          const user = await bios.createUser({
            email: 'js+test2@isima.io',
            password: 'secret',
            tenantName: 'noSuchTenant',
            fullName: 'Test User',
            roles: ['Extract'],
          });
          fail('exception is expected');
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.FORBIDDEN.errorCode);
        }

        const user = await bios.createUser({
          email: 'js+test2@isima.io',
          password: 'secret',
          tenantName: 'jsSimpleTest',
          fullName: 'Test User',
          roles: ['Extract'],
        });

        expect(user.email).toBe('js+test2@isima.io');
        expect(user.fullName).toBe('Test User');
        expect(user.roles).toEqual(['Extract']);
        expect(user.status).toBe('Active');
        expect(user.userId).toBeDefined();

        const userId = user.userId;

        const modifiedUser = await bios.modifyUser({
          ...user,
          status: 'Suspended',
        });

        expect(modifiedUser.email).toBe('js+test2@isima.io');
        expect(modifiedUser.fullName).toBe('Test User');
        expect(modifiedUser.roles).toEqual(['Extract']);
        expect(modifiedUser.status).toBe('Suspended');
        expect(modifiedUser.userId).toBe(user.userId);

        await bios.signOut();
        await bios.signIn({
          email: 'admin@jsSimpleTest2',
          password: 'admin',
        });

        try {
          const modifiedUser = await bios.modifyUser({
            ...user,
            status: 'Active',
          });
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
        }

        try {
          await bios.deleteUser({ userId });
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
        }

        await bios.signOut();
        await bios.signIn({
          email: 'admin@jsSimpleTest',
          password: 'admin',
        });

        await bios.deleteUser({ userId });

        await bios.signOut();
        await bios.signIn({
          email: 'admin@jsSimpleTest',
          password: 'admin',
        });
        const usersAfterDeletion = await bios.getUsers();
        const emailsAfterDeletion = usersAfterDeletion.map((user) => user.email);
        expect(emailsAfterDeletion.includes('js+test2@isima.io')).toBe(false);

      } finally {
        try {
          await bios.signOut();
          await bios.signIn({
            email: 'superadmin',
            password: 'superadmin'
          });
          await bios.deleteUser({ email: 'js+test2@isima.io' });
        } catch (e) {
          // it's ok
        }
      }
    });
  });
});
