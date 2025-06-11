'use strict';

import bios, {
  BiosClientError,
  BiosErrorType
} from 'bios-sdk';
import { BIOS_ENDPOINT } from './tutils';


describe('Apps registration', () => {

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'superadmin',
      password: 'superadmin',
    });
    await bios.setProperty('prop.apps.resourceReservationSeconds', '10');
    await bios.setProperty('prop.apps.hosts', 'deli-1, deli-2, deli-3, deli-4');
    return null;
  });

  afterAll(async () => {
    await bios.setProperty('prop.apps.resourceReservationSeconds', '0');
    try {
      await bios.deregisterAppsService('jsSimpleTest');
    } catch (e) {
      // fine to have exceptions
    }
    await bios.setProperty('prop.apps.resourceReservationSeconds', '');
    await bios.setProperty('prop.apps.hosts', '');
    await bios.signOut();
  });

  it('Fundamental', async () => {
    const appsInfo = {
      tenantName: 'jsSimpleTest',
    };
    const registered = await bios.registerAppsService(appsInfo);
    const expectedAppsInfo = {
      tenantName: 'jsSimpleTest',
      hosts: ['deli-1', 'deli-2', 'deli-3', 'deli-4'],
      controlPort: 9001,
      webhookPort: 8081,
    };
    expect(registered).toEqual(expectedAppsInfo);

    try {
      await bios.registerAppsService(appsInfo);
      fail('exception is expected');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.RESOURCE_ALREADY_EXISTS.errorCode);
    }

    const retrieved = await bios.getAppsInfo('jsSimpleTest');
    expect(retrieved).toEqual(expectedAppsInfo);

    await bios.deregisterAppsService('jsSimpleTest');
    try {
      await bios.deregisterAppsService('jsSimpleTest');
      fail('exception is expected');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
    }
    const appsInfo2 = {
      tenantName: 'jsSimpleTest',
      hosts: ['second', 'registration'],
      controlPort: 9010,
      webhookPort: 8090,
    };
    const registered2 = await bios.registerAppsService(appsInfo2);
    expect(registered2).toEqual(appsInfo2);

    await bios.setProperty('prop.apps.resourceReservationSeconds', '0');
    await bios.deregisterAppsService('jsSimpleTest');
  });
});
