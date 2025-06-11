/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, { BiosClientError, BiosErrorType, ResponseType, SelectISqlResponse } from 'bios-sdk';
import {
  BIOS_ENDPOINT,
  clearTenant,
  sleep,
} from './tutils';


describe('Sysadmin operations test', () => {
  beforeEach(async () => {
    await bios.global.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
  });

  let addedTenants = [];
  afterEach(async () => {
    for (let i = 0; i < addedTenants.length; ++i) {
      await clearTenant(addedTenants[i], false);
    }
    addedTenants = [];
    await bios.logout();
  });

  describe('Functionality tests', () => {
    describe('Tenant sysadmin', () => {
      it('Create tenant', async () => {
        const tenantName = 'jsCreateTenantTest';
        addedTenants.push(tenantName);
        const response = await bios.createTenant({
          tenantName,
        });
        expect(response).not.toBeDefined();

        const tenants = await bios.listTenantNames();
        const added = tenants.find((tenant) => tenant.tenantName === tenantName);
        expect(added).toBeDefined();

        // Repeated attempt to create the tenant should be rejected
        addedTenants.push(tenantName);
        await clearTenant(tenantName, false);
        await bios.createTenant({ tenantName });

        await expectAsync(bios.createTenant({ tenantName })).toBeRejectedWithError(BiosClientError);
      });

      it('List all tenants', async () => {
        const tenantName1 = 'jsCreateTenantTest1';
        const tenantName2 = 'jsCreateTenantTest2';
        addedTenants.push(tenantName1);
        addedTenants.push(tenantName2);
        await bios.createTenant({ tenantName: tenantName1 });
        await bios.createTenant({ tenantName: tenantName2 });

        const allTenants = await bios.listTenantNames();

        // three tenants should be there at least
        expect(allTenants.length).toBeGreaterThanOrEqual(3);
        const tenant1 = allTenants.find((tenant) => tenant.tenantName === tenantName1);
        const tenant2 = allTenants.find((tenant) => tenant.tenantName === tenantName2);
        const tenantSystem = allTenants.find((tenant) => tenant.tenantName === '_system');
        expect(tenant1).toBeDefined();
        expect(tenant2).toBeDefined();
        expect(tenantSystem).toBeDefined();
        expect(typeof (tenant1.version)).toBe('number');
        expect(typeof (tenant2.version)).toBe('number');
        expect(typeof (tenantSystem.version)).toBe('number');
        expect(Object.keys(tenant1).length).toBe(2);
      });

      it('List one of tenants', async () => {
        const tenantName1 = 'jsCreateTenantTest1';
        const tenantName2 = 'jsCreateTenantTest2';
        addedTenants.push(tenantName1);
        addedTenants.push(tenantName2);
        await bios.createTenant({ tenantName: tenantName1 });
        await bios.createTenant({ tenantName: tenantName2 });

        // try partial list
        const someTenants = await bios.listTenantNames({
          names: [tenantName2],
        });
        expect(someTenants.length).toBe(1);
        const tenant2 = someTenants.find((tenant) => tenant.tenantName === tenantName2);
        expect(tenant2).toBeDefined();
        expect(typeof (tenant2.version)).toBe('number');
      });

      it('List two of tenants', async () => {
        const tenantName1 = 'jsCreateTenantTest1';
        const tenantName2 = 'jsCreateTenantTest2';
        addedTenants.push(tenantName1);
        addedTenants.push(tenantName2);
        await bios.createTenant({ tenantName: tenantName1 });
        await bios.createTenant({ tenantName: tenantName2 });

        // try partial list
        const someTenants = await bios.listTenantNames({
          names: [tenantName1, '_system'],
        });
        expect(someTenants.length).toBe(2);
        const tenant21 = someTenants.find((tenant) => tenant.tenantName === tenantName1);
        const tenantSystem2 = someTenants.find((tenant) => tenant.tenantName === '_system');
        expect(tenant21).toBeDefined();
        expect(tenantSystem2).toBeDefined();
        expect(typeof (tenant21.version)).toBe('number');
        expect(typeof (tenantSystem2.version)).toBe('number');
      });

      it('Try listing a non-existing tenant', async () => {
        const tenantName1 = 'jsCreateTenantTest1';
        const tenantName2 = 'jsCreateTenantTest2';
        addedTenants.push(tenantName1);
        addedTenants.push(tenantName2);
        await bios.createTenant({ tenantName: tenantName1 });
        await bios.createTenant({ tenantName: tenantName2 });

        // try partial list
        try {
          await bios.listTenantNames({
            names: [tenantName1, '_system', 'nonExisting'],
          });
          fail('Exception must happen');
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_TENANT.errorCode);
        }
      });

      it('Try listing two non-existing tenants', async () => {
        const tenantName1 = 'jsCreateTenantTest1';
        const tenantName2 = 'jsCreateTenantTest2';
        addedTenants.push(tenantName1);
        addedTenants.push(tenantName2);
        await bios.createTenant({ tenantName: tenantName1 });
        await bios.createTenant({ tenantName: tenantName2 });

        // try partial list
        try {
          await bios.listTenantNames({
            names: ['hello', tenantName1, '_system', 'nonExisting'],
          });
          fail('Exception must happen');
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_TENANT.errorCode);
        }
      });
    });

    describe('Security tests', () => {
      let adminSession;
      beforeEach(async () => {
        adminSession = await bios.login({
          endpoint: BIOS_ENDPOINT,
          email: 'admin@jsSimpleTest',
          password: 'admin',
        });
      });

      afterEach(async () => {
        await adminSession.logout();
      });

      it('Try creating a tenant by Tenant Admin user', async () => {
        const tenantName = 'jsCreateTenantByTenantAdmin';
        try {
          await adminSession.createTenant({ tenantName });
          fail('Exception must happen');
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.FORBIDDEN.errorCode);
        }
      });

      it('Try listing a tenant by Tenant Admin user', async () => {
        try {
          await adminSession.listTenantNames();
          fail('Exception must happen');
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.FORBIDDEN.errorCode);
        }
      });

      it('Try deleting a tenant by Tenant Admin user', async () => {
        const tenantName = 'jsTryDeletingTenantByAdmin';
        try {
          await adminSession.deleteTenant(tenantName);
          fail('Exception must happen');
        } catch (e) {
          expect(e instanceof BiosClientError).toBe(true);
          expect(e.errorCode).toBe(BiosErrorType.FORBIDDEN.errorCode);
        }
      });
    });

    describe('Properties', () => {
      it('fundamental', async () => {
        await bios.setProperty('hello', 'world');
        const retrieved = await bios.getProperty('hello');
        expect(retrieved).toBe('world');
        await bios.setProperty('hello', '');
      });
    });

    describe('System signals', () => {
      it('System signal _query', async () => {
        let statement = bios.iSqlStatement()
          .select()
          .from('_query')
          .timeRange(bios.time.now(), -bios.time.days(1))
          .build();

        let result = await bios.execute(statement);

        if (result.definitions.length === 0) {
          // select may not have happened before, wait for 1 min and retry
          await sleep(60000);
          let statement = bios.iSqlStatement()
            .select()
            .from('_query')
            .timeRange(bios.time.now(), -bios.time.days(1))
            .build();
          result = await bios.execute(statement);
        }

        expect(result).toBeInstanceOf(SelectISqlResponse);
        expect(result.responseType).toBe(ResponseType.SELECT);
        expect(result.definitions.length).toBeGreaterThan(10);
        expect(result.dataWindows[0].records.length).toBeGreaterThanOrEqual(1);

        statement = bios.iSqlStatement()
          .select('synopsis(queryType)')
          .from('_query')
          .tumblingWindow(bios.time.hours(1))
          .snappedTimeRange(bios.time.now(), -bios.time.hours(24))
          .build();
        result = await bios.multiExecute(statement);
        expect(result[0]).toBeInstanceOf(SelectISqlResponse);
        expect(result[0].responseType).toBe(ResponseType.SELECT);
        expect(result[0].definitions.length).toBeGreaterThan(4);
        expect(result[0].dataWindows[0].records.length).toBeGreaterThanOrEqual(1);
      });
    });
  });
});
