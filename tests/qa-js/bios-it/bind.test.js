import axios from 'axios';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';

import { BIOS_ENDPOINT } from './tutils';

async function selfserveLogin(username, password) {
  const instance = axios.create({
    baseURL: BIOS_ENDPOINT,
    timeout: 120000,
    headers: {
      'Content-Type': 'application/json',
      xsrfToken: 'BIClient',
      // Accept: 'application/x-protobuf,application/json',
    },
    withCredentials: true
  });
  const resp = await instance.post('/selfserve/v1/auth/login', {
    username,
    password,
  });
  return instance;
}

// TODO(BIOS-5007): Enable or drop this case
xdescribe('Bind axios instances', () => {
  beforeEach(async () => {
    // console.debug('resp:', resp.data);
  });

  afterEach(async () => {
    await bios.logout();
  });

  it('Fundamental', async () => {
    const instance = await selfserveLogin('admin@jsSimpleTest', 'admin');
    const session = await bios.bindAxios(instance);
    expect(session.tenantName).toBe('jsSimpleTest');
    const signals = await bios.getSignals();
    expect(signals.length).toBeGreaterThanOrEqual(5);
  });

  it('SysAdmin', async () => {
    const instance = await selfserveLogin('superadmin', 'superadmin');
    const session = await bios.bindAxios(instance);
    expect(session.tenantName).toBe('_system');
    const signals = await bios.getSignals({ includeInternal: true });
    expect(signals.length).toBeGreaterThan(3);
  });

  it('Bind with "global" keyword (deprecated)', async () => {
    const instance = await selfserveLogin('admin@jsSimpleTest', 'admin');
    const session = await bios.global.bindAxios(instance);
    const signals = await bios.getSignals();
    expect(signals.length).toBeGreaterThanOrEqual(5);
  });
});
