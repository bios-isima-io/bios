import bios, { BiosClient, BiosClientError, BiosErrorType } from 'bios-sdk';

import { BIOS_ENDPOINT, createRandomString, SMTP_ENDPOINT } from './tutils';

xdescribe('Registration', () => {

  const sessionConfig = {
    endpoint: BIOS_ENDPOINT,
    email: 'superadmin',
    password: 'superadmin',
  };

  const appMasterConfig = {
    endpoint: BIOS_ENDPOINT,
    email: `app-master-${createRandomString(5)}@example.com`,
    password: 'app-master',
  };

  const masterTenantName = `appMaster${createRandomString(5)}`;
  let appMasterClient = null;

  beforeAll(async () => {
    const systemAdminClient = BiosClient.start(sessionConfig);
    const session = await systemAdminClient.getSession();
    await session.createTenant({ tenantName: masterTenantName });
    const { email, password } = appMasterConfig;
    await session.createUser({
      email,
      password,
      tenantName: masterTenantName,
      fullName: 'app master',
      roles: ['AppMaster', 'SchemaExtractIngest'],
    });
    appMasterClient = BiosClient.start(appMasterConfig);

    const masterSession = await BiosClient.start({
      endpoint: BIOS_ENDPOINT,
      email: `admin@${masterTenantName}`,
      password: 'admin',
    }).getSession();
    await masterSession.createContext({
      'contextName': 'tenants',
      'missingAttributePolicy': 'Reject',
      'attributes': [
        { 'attributeName': 'domain', 'type': 'String' },
        { 'attributeName': 'config', 'type': 'String' }
      ],
      'primaryKey': ['domain']
    });
    await masterSession.createContext({
      "contextName": "merchants",
      "missingAttributePolicy": "Reject",
      "attributes": [
        {
          "attributeName": "domain",
          "type": "String"
        },
        {
          "attributeName": "tenantName",
          "type": "String"
        },
        {
          "attributeName": "ownerEmail",
          "type": "String"
        },
        {
          "attributeName": "status",
          "type": "String"
        },
        {
          "attributeName": "createdAt",
          "type": "Integer"
        },
        {
          "attributeName": "updatedAt",
          "type": "Integer"
        }
      ],
      "primaryKey": [
        "domain"
      ]
    });
    await masterSession.createSignal({
      "signalName": "shopifyAppAudit",
      "missingAttributePolicy": "StoreDefaultValue",
      "attributes": [
        {
          "attributeName": "eventType",
          "type": "String",
          "missingAttributePolicy": "Reject"
        },
        {
          "attributeName": "domain",
          "type": "String",
          "default": "MISSING"
        },
        {
          "attributeName": "webhookId",
          "type": "String",
          "default": "MISSING"
        },
        {
          "attributeName": "eventTimestamp",
          "type": "Integer",
          "missingAttributePolicy": "Reject"
        },
        {
          "attributeName": "payload",
          "type": "String",
          "default": "MISSING"
        },
        {
          "attributeName": "slackNotification",
          "type": "String",
          "default": "MISSING"
        },
        {
          "attributeName": "status",
          "type": "String",
          "missingAttributePolicy": "Reject"
        },
        {
          "attributeName": "response",
          "type": "String",
          "missingAttributePolicy": "StoreDefaultValue",
          "default": "MISSING"
        }
      ]
    });
  });

  afterAll(async () => {
    await BiosClient.start(sessionConfig).getSession()
      .then((session) => session.deleteTenant(masterTenantName));
  });

  beforeEach(async () => {
    await fetch(`${SMTP_ENDPOINT}/api/v1/messages`, { method: 'DELETE' });
  });

  test('Fundamental', async () => {
    const domain = `${createRandomString(5)}.example.com`;
    const email = `owner@${domain}`;

    let startTime = bios.time.now();
    const response = await appMasterClient.getSession()
      .then((session) => session.registerForService({
        domain,
        email,
        serviceType: 'StoreEnhancement',
      }));

    // check audit and merchant context
    let now = bios.time.now();
    let auditStatement = bios.iSqlStatement()
      .select()
      .from('shopifyAppAudit')
      .timeRange(startTime, now - startTime)
      .build();
    let audit = await appMasterClient.getSession()
      .then((session) => session.execute(auditStatement));
    expect(audit.dataWindows.length).toBe(1);
    let dataWindow = audit.dataWindows[0];
    expect(dataWindow.records.length).toBe(1);
    expect(dataWindow.records[0][0]).toBe('registration');
    expect(dataWindow.records[0][1]).toBe(domain);
    expect(dataWindow.records[0][2]).toBe('MISSING');
    expect(dataWindow.records[0][3]).toBeLessThan(now);
    expect(dataWindow.records[0][3]).toBeGreaterThan(startTime);
    expect(dataWindow.records[0][5]).toBe('MISSING');
    expect(dataWindow.records[0][6]).toBe('OK');
    let payload = JSON.parse(dataWindow.records[0][4]);
    expect(payload.email).toBe(email);
    let responseObj = JSON.parse(dataWindow.records[0][7]);
    expect(responseObj.result).toBe('TenantCreated');

    const expectedTenantName = domain.replaceAll('.', '_');

    let merchantStatement = bios.iSqlStatement()
      .select()
      .fromContext('merchants')
      .where(bios.isql.key(domain))
      .build()
    let merchant = await appMasterClient.getSession()
      .then((session) => session.execute(merchantStatement));
    expect(merchant.entries.length).toBe(1);
    expect(merchant.entries[0].attributes[1]).toBe(expectedTenantName);
    expect(merchant.entries[0].attributes[2]).toBe(email);
    expect(merchant.entries[0].attributes[3]).toBe('Active');

    expect(response.result).toBe('TenantCreated');
    expect(response.tenantName).toBe(expectedTenantName);

    const appTenantSession = await BiosClient.start({
      endpoint: BIOS_ENDPOINT,
      email: `admin@${expectedTenantName}`,
      password: 'admin',
    }).getSession();

    const tenant = await appTenantSession.getTenant({ detail: true });

    expect(tenant.appMaster).toBe(masterTenantName);

    const contexts = tenant.contexts;
    expect(contexts).toBeDefined();
    expect(contexts.some((context) => context.contextName === 'sessions')).toBe(true);
    expect(contexts.some((context) => context.contextName === 'productViews_byClientId'))
      .toBe(true);
    expect(contexts.some((context) => context.contextName === 'productViews')).toBe(true);

    const signals = tenant.signals;
    expect(signals).toBeDefined();
    expect(signals.some((signal) => signal.signalName === 'auditProductViews')).toBe(true);
    expect(signals.some((signal) => signal.signalName === 'auditSessions')).toBe(true);
    expect(signals.some((signal) => signal.signalName === 'auditProductViews_byClientId'))
      .toBe(false);

    const users = await appTenantSession.getUsers();
    expect(users.some((user) => user.email === email)).toBe(true);

    const messages = await fetch(`${SMTP_ENDPOINT}/api/v1/messages`)
      .then((response) => response.json());
    expect(messages.length).toBe(1);
    expect(messages[0].To.length).toBe(1);
    expect(messages[0].To[0].Mailbox).toBe('owner');
    expect(messages[0].To[0].Domain).toBe(domain);

    // no data is accumulated yet. just check if delegating query succeeds
    await appMasterClient.getSession()
      .then((session) => session.forTenant(domain).execute(
        bios.iSqlStatement()
          .select("count()")
          .from("auditProductViews")
          .groupBy("widgetId")
          .tumblingWindow(bios.time.minutes(5))
          .snappedTimeRange(bios.time.now(), -bios.time.minutes(30))
          .build()
      ));

    // It is fine to register for the same service again
    startTime = bios.time.now();
    const secondResponse = await appMasterClient.getSession()
      .then((session) => session.registerForService({
        domain,
        email,
        serviceType: 'StoreEnhancement',
      }));
    expect(secondResponse.result).toBe('TenantAlreadyExists');
    expect(secondResponse.tenantName).toBe(expectedTenantName);

    const detailTenant = await appMasterClient.getSession()
      .then((session) => session.getTenant({
        tenantName: expectedTenantName,
        detail: true,
      }));
    expect(detailTenant.tenantName).toBe(expectedTenantName);
    expect(detailTenant.domain).toBe(domain);

    // check audit and merchant context
    now = bios.time.now();
    auditStatement = bios.iSqlStatement()
      .select()
      .from('shopifyAppAudit')
      .timeRange(startTime, now - startTime)
      .build();
    audit = await appMasterClient.getSession()
      .then((session) => session.execute(auditStatement));
    expect(audit.dataWindows.length).toBe(1);
    dataWindow = audit.dataWindows[0];
    expect(dataWindow.records.length).toBe(1);
    expect(dataWindow.records[0][0]).toBe('registration');
    expect(dataWindow.records[0][1]).toBe(domain);
    expect(dataWindow.records[0][2]).toBe('MISSING');
    expect(dataWindow.records[0][3]).toBeLessThan(now);
    expect(dataWindow.records[0][3]).toBeGreaterThan(startTime);
    expect(dataWindow.records[0][5]).toBe('MISSING');
    expect(dataWindow.records[0][6]).toBe('OK');
    payload = JSON.parse(dataWindow.records[0][4]);
    expect(payload.email).toBe(email);
    responseObj = JSON.parse(dataWindow.records[0][7]);
    expect(responseObj.result).toBe('TenantAlreadyExists');

    // merchant info should be unchanged
    merchantStatement = bios.iSqlStatement()
      .select()
      .fromContext('merchants')
      .where(bios.isql.key(domain))
      .build()
    merchant = await appMasterClient.getSession()
      .then((session) => session.execute(merchantStatement));
    expect(merchant.entries.length).toBe(1);
    expect(merchant.entries[0].attributes[1]).toBe(expectedTenantName);
    expect(merchant.entries[0].attributes[2]).toBe(email);
    expect(merchant.entries[0].attributes[3]).toBe('Active');

    try {
      await appMasterClient.getSession()
        .then((session) => session.storeEnhancementQuery({
          domain,
          sessionId: 'bogusSessionId',
          productId: 'bogusProductId',
          queries: [
            {
              queryType: 'RecentViews',
              maxItems: 3,
            },
            {
              queryType: 'NumViewers',
            },
          ]
        }));
      fail("exception is expected");
    } catch (e) {
      expect(e).toBeInstanceOf(BiosClientError);
      expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      expect(e.message).toMatch(/clientId must be set for query 'RecentViews';/);
    }

    const queryResponse = await appMasterClient.getSession()
      .then((session) => session.storeEnhancementQuery({
        domain,
        clientId: 'bogusSessionId',
        productId: 'bogusProductId',
        queries: [
          {
            queryType: 'RecentViews',
            maxItems: 3,
          },
          {
            queryType: 'NumViewers',
          },
        ]
      }));

    // but verification email should not be out
    const messages2 = await fetch(`${SMTP_ENDPOINT}/api/v1/messages`)
      .then((response) => response.json());
    expect(messages2.length).toBe(1);

    // should throw
    try {
      await BiosClient.start({
        endpoint: BIOS_ENDPOINT,
        email,
        password: ''
      }).getSession();
      fail('Exception is expected');
    } catch (e) {
      expect(e).toBeInstanceOf(BiosClientError);
      expect(e.errorCode).toBe(BiosErrorType.USER_ID_NOT_VERIFIED.errorCode);
      expect(e.message).toMatch(/User ID is not verified yet/);
    }
    const messages3 = await fetch(`${SMTP_ENDPOINT}/api/v1/messages`)
      .then((response) => response.json());
    expect(messages3.length).toBe(1);
  });

  it('User conflict', async () => {
    const domain = `${createRandomString(5)}.example.com`;
    const email = `owner@${domain}`;

    const response = await appMasterClient.getSession()
      .then((session) => session.registerForService({
        domain,
        email,
        serviceType: 'StoreEnhancement',
      }));
    const messages = await fetch(`${SMTP_ENDPOINT}/api/v1/messages`)
      .then((response) => response.json());
    expect(messages.length).toBe(1);

    await fetch(`${SMTP_ENDPOINT}/api/v1/messages`, { method: 'DELETE' });

    const secondDomain = `${createRandomString(5)}.example.com`;

    const startTime = bios.time.now();
    const response2 = await appMasterClient.getSession()
      .then((session) => session.registerForService({
        domain: secondDomain,
        email,
        serviceType: 'StoreEnhancement',
      }));
    const expectedEmail = `owner+shopify1@${domain}`;
    const expectedResponse = {
      result: 'TenantCreated',
      tenantName: secondDomain.replaceAll('.', '_'),
      initialUserEmail: expectedEmail,
    };
    expect(response2).toEqual(expectedResponse);

    const messages2 = await fetch(`${SMTP_ENDPOINT}/api/v1/messages`)
      .then((response) => response.json());
    expect(messages2.length).toBe(1);
    expect(messages2[0].To[0].Mailbox).toBe('owner+shopify1');
    expect(messages2[0].To[0].Domain).toBe(domain);
    expect(messages2[0].Content.Headers.To[0]).toBe(expectedEmail);

    let now = bios.time.now();
    let auditStatement = bios.iSqlStatement()
      .select()
      .from('shopifyAppAudit')
      .timeRange(startTime, now - startTime)
      .build();
    let audit = await appMasterClient.getSession()
      .then((session) => session.execute(auditStatement));
    expect(audit.dataWindows.length).toBe(1);
    let dataWindow = audit.dataWindows[0];
    // console.log(JSON.stringify(dataWindow, null, 2));
    expect(dataWindow.records.length).toBe(1);
    expect(dataWindow.records[0][1]).toBe(secondDomain);
    expect(dataWindow.records[0][6]).toBe('OK');
    const result = JSON.parse(dataWindow.records[0][7]);
    expect(result).toEqual(expectedResponse);

    let merchantStatement = bios.iSqlStatement()
      .select()
      .fromContext('merchants')
      .where(bios.isql.key(secondDomain))
      .build()
    let merchant = await appMasterClient.getSession()
      .then((session) => session.execute(merchantStatement));
    expect(merchant.entries.length).toBe(1);
    expect(merchant.entries[0].attributes[0]).toBe(secondDomain);
    expect(merchant.entries[0].attributes[1]).toBe(secondDomain.replaceAll('.', '_'));
    expect(merchant.entries[0].attributes[2]).toBe(expectedEmail);
    expect(merchant.entries[0].attributes[3]).toBe('Active');
  });

  test('Wrong service name', async () => {
    const domain = `${createRandomString(5)}.example.com`;
    const email = `owner@${domain}`;

    try {
      await appMasterClient.getSession()
        .then((session) => session.registerForService({
          domain,
          email,
          serviceType: 'NoSuchService',
        }));
      fail('exception is expected');
    } catch (e) {
      expect(e).toBeInstanceOf(BiosClientError);
      expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      expect(e.message).toMatch(/Unknown value: NoSuchService/);
    }
    const messages = await fetch(`${SMTP_ENDPOINT}/api/v1/messages`)
      .then((response) => response.json());
    expect(messages.length).toBe(0);
  });

  test('App config', async () => {
    const domain = `${createRandomString(5)}.example.com`;

    const config = {
      pixelId: createRandomString(10),
    };

    // Writing app config
    const writeStatement = bios.iSqlStatement()
      .upsert()
      .into('tenants')
      .csv(`${domain},${JSON.stringify(config)}`)
      .build();
    await appMasterClient.getSession()
      .then((session) => session.execute(writeStatement));

    // Read app config
    const readStatement = bios.iSqlStatement()
      .select()
      .fromContext('tenants')
      .where(`domain = '${domain}'`)
      .build();
    const result = await appMasterClient.getSession()
      .then((session) => session.execute(readStatement));
    expect(result.entries.length).toBe(1);
    const decoded = JSON.parse(result.entries[0].config);
    expect(decoded).toEqual(config);
  });

  test('Concurrent requests', async () => {
    const domain = `${createRandomString(5)}.example.com`;
    const email = `owner@${domain}`;
    const conf = {
      domain,
      email,
      serviceType: 'StoreEnhancement',
    };

    const promise1 = appMasterClient.getSession()
      .then((session) => session.registerForService(conf));

    const promise2 = appMasterClient.getSession()
      .then((session) => session.registerForService(conf));

    const promise3 = appMasterClient.getSession()
      .then((session) => session.registerForService(conf));

    const response1 = await promise1;
    const response2 = await promise2;
    const response3 = await promise3;

    const counters = {
      TenantCreated: 0,
      RequestConflicted: 0,
    };

    ++counters[response1.result];
    ++counters[response2.result];
    ++counters[response3.result];

    expect(counters.TenantCreated).toBe(1);
    expect(counters.RequestConflicted).toBe(2);

    const messages = await fetch(`${SMTP_ENDPOINT}/api/v1/messages`)
      .then((response) => response.json());
    expect(messages.length).toBe(1);
  });
});
