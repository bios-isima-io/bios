/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */
'use strict';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';
import {
  BIOS_ENDPOINT,
  clearTenant
} from './tutils';

describe('Permissions test', () => {

  const tenantName = 'jsPermissionsTest';
  const adminUser = `admin@${tenantName}`;
  const adminReportUser = `admin+report@${tenantName}`;
  const adminPass = 'admin';
  const mleUser = `mlengineer@${tenantName}`;
  const mleReportUser = `mlengineer+report@${tenantName}`;
  const mlePass = 'mlengineer';
  const ingestUser = `ingest@${tenantName}`;
  const ingestReportUser = `ingest+report@${tenantName}`;
  const ingestPass = 'ingest';
  const extractUser = `extract@${tenantName}`;
  const extractReportUser = `extract+report@${tenantName}`;
  const extractPass = 'extract';
  const reportUser = `report@${tenantName}`;
  const reportPass = 'report';

  /**
   *  Utility to verify permission denial.
   */
  const verifyForbidden = async (func, testName) => {
    try {
      await func();
      const hdr = testName ? `${testName}: ` : '';
      fail(`${hdr}exception is expected`);
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.FORBIDDEN.errorCode);
    }
  };

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, false);
    await bios.createTenant({ tenantName });
    await bios.signOut();
  });

  afterAll(async () => {
    await bios.signIn({
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, true);
    await bios.signOut();
  });

  afterEach(async () => {
    await bios.signOut();
  });

  describe('Signal schema', () => {

    const signalConfig = {
      signalName: 'verySimpleSignal',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'transaction_id', type: 'Integer' },
      ]
    };

    const modifiedSignal = { ...signalConfig };
    modifiedSignal.attributes = [
      ...signalConfig.attributes,
      {
        attributeName: 'customerName',
        type: 'String',
        default: 'n/a'
      },
    ];

    it('CRUD signal by admin', async () => {
      // Login as a tenant admin
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });

      // CR by admin
      await bios.createSignal(signalConfig);
      await bios.getSignal(signalConfig.signalName);

      // access by an ingest user
      await bios.signOut();
      await bios.signIn({
        email: ingestUser,
        password: ingestPass,
      });

      // R is allowed
      await bios.getSignal(signalConfig.signalName);
      // UD are denied
      await verifyForbidden(() => bios.updateSignal(signalConfig.signalName, modifiedSignal));
      await verifyForbidden(() => bios.deleteSignal(signalConfig.signalName));

      // access by a report only user
      await bios.signOut();
      await bios.signIn({
        email: reportUser,
        password: reportPass,
      });

      // R is allowed
      await bios.getSignal(signalConfig.signalName);
      // UD are denied
      await verifyForbidden(() => bios.updateSignal(signalConfig.signalName, modifiedSignal));
      await verifyForbidden(() => bios.deleteSignal(signalConfig.signalName));

      // Login as a tenant admin again, run UD
      await bios.signOut();
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });
      await bios.updateSignal(signalConfig.signalName, modifiedSignal);
      await bios.deleteSignal(signalConfig.signalName);
    });

    it('CRUD signal by ML engineer', async () => {
      // Login as a ML engineer
      await bios.signIn({
        email: mleUser,
        password: mlePass,
      });

      // CR by ML engineer
      await bios.createSignal(signalConfig);
      await bios.getSignal(signalConfig.signalName);

      // access by extract
      await bios.signOut();
      await bios.signIn({
        email: extractUser,
        password: extractPass,
      });
      // R is allowed
      await bios.getSignal(signalConfig.signalName);
      // UD are denied
      await verifyForbidden(() => bios.updateSignal(signalConfig.signalName, modifiedSignal));
      await verifyForbidden(() => bios.deleteSignal(signalConfig.signalName));

      // Login as a tenant admin again, run UD
      await bios.signOut();
      await bios.signIn({
        email: mleUser,
        password: mlePass,
      });
      await bios.updateSignal(signalConfig.signalName, modifiedSignal);
      await bios.deleteSignal(signalConfig.signalName);
    });

    it('CREATE signal by extract user', async () => {
      await bios.signIn({
        email: extractUser,
        password: extractPass,
      });
      await verifyForbidden(() => bios.createSignal(signalConfig));
    });

    it('CREATE signal by ingest user', async () => {
      await bios.signIn({
        email: ingestUser,
        password: ingestPass,
      });
      await verifyForbidden(() => bios.createSignal(signalConfig));
    });

    it('CREATE signal by report only user', async () => {
      await bios.signIn({
        email: reportUser,
        password: reportPass,
      });
      await verifyForbidden(() => bios.createSignal(signalConfig));
    });
  });

  describe('Context schema', () => {

    const contextConfig = {
      contextName: 'verySimpleContext',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'productId', type: 'Integer' },
        { attributeName: 'productName', type: 'String' },
      ],
      primaryKey: ['productId'],
    };

    const modifiedContext = { ...contextConfig };
    modifiedContext.attributes = [
      ...contextConfig.attributes,
      {
        attributeName: 'category',
        type: 'String',
        default: 'n/a'
      },
    ];

    it('CRUD context by admin', async () => {
      // Login as a tenant admin
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });

      // CR by admin
      await bios.createContext(contextConfig);
      await bios.getContext(contextConfig.contextName);

      // access by an ingest user
      await bios.signOut();
      await bios.signIn({
        email: ingestUser,
        password: ingestPass,
      });
      // R is allowed
      await bios.getContext(contextConfig.contextName);
      // UD are denied
      await verifyForbidden(() => bios.updateContext(contextConfig.contextName, modifiedContext));
      await verifyForbidden(() => bios.deleteContext(contextConfig.contextName));

      // access by a report only user
      await bios.signOut();
      await bios.signIn({
        email: reportUser,
        password: reportPass,
      });
      // R is allowed
      await bios.getContext(contextConfig.contextName);
      // UD are denied
      await verifyForbidden(() => bios.updateContext(contextConfig.contextName, modifiedContext));
      await verifyForbidden(() => bios.deleteContext(contextConfig.contextName));

      // Login as a tenant admin again, run UD
      await bios.signOut();
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });
      await bios.updateContext(contextConfig.contextName, modifiedContext);
      await bios.deleteContext(contextConfig.contextName);
    });

    it('CRUD context by ML engineer', async () => {
      // Login as a ML engineer
      await bios.signIn({
        email: mleUser,
        password: mlePass,
      });

      // CR by ML engineer
      await bios.createContext(contextConfig);
      await bios.getContext(contextConfig.contextName);

      // access by extract
      await bios.signOut();
      await bios.signIn({
        email: extractUser,
        password: extractPass,
      });
      // R is allowed
      await bios.getContext(contextConfig.contextName);
      // UD are denied
      await verifyForbidden(() => bios.updateContext(contextConfig.contextName, modifiedContext));
      await verifyForbidden(() => bios.deleteContext(contextConfig.contextName));

      // Login as a tenant admin again, run UD
      await bios.signOut();
      await bios.signIn({
        email: mleUser,
        password: mlePass,
      });
      await bios.updateContext(contextConfig.contextName, modifiedContext);
      await bios.deleteContext(contextConfig.contextName);
    });

    it('CREATE context by extract user', async () => {
      await bios.signIn({
        email: extractUser,
        password: extractPass,
      });
      await verifyForbidden(() => bios.createContext(contextConfig));
    });

    it('CREATE context by ingest user', async () => {
      await bios.signIn({
        email: ingestUser,
        password: ingestPass,
      });
      await verifyForbidden(() => bios.createContext(contextConfig));
    });

    it('CREATE context by report only user', async () => {
      await bios.signIn({
        email: reportUser,
        password: reportPass,
      });
      await verifyForbidden(() => bios.createContext(contextConfig));
    });
  });

  describe('Insight and report configs', () => {

    afterEach(async () => {
      await bios.signOut();
    });

    const testInsightOps = async (user, pass, reportUser, reportPass) => {
      const insightConfigs = {
        sections: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfigs: [
              {
                insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
            ],
          },
        ],
      };

      const reportConfig = {
        reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        reportName: 'Revenue in USD',
        metrics: [bios.metric('customers', 'max(stay_length)', 'Maximum Age')],
        dimensions: ['country', 'state'],
        defaultTimeRange: 604800000,
        defaultWindowLength: 43200000,
      };

      const insightName = "signal";

      // non-report user cannot CRUD an insight config
      if (!!user) {
        await bios.signIn({
          email: user,
          password: pass,
        });
        await verifyForbidden(() => bios.putInsightConfigs(insightName, insightConfigs), 'putInsightConfigs');
        await verifyForbidden(() => bios.getInsightConfigs(insightName), 'getInsightConfigs');
        await verifyForbidden(() => bios.deleteInsightConfigs(insightName), 'deleteInsightConfigs');
        await verifyForbidden(() => bios.putReportConfig(reportConfig), 'putReportConfig');
        await verifyForbidden(() => bios.getReportConfigs(), 'getReportConfigs');
        await verifyForbidden(() => bios.deleteReportConfig(reportConfig.reportId), 'deleteReportConfig');
      }

      await bios.signOut();
      await bios.signIn({
        email: reportUser,
        password: reportPass,
      });
      await bios.putInsightConfigs(insightName, insightConfigs);
      await bios.getInsightConfigs(insightName);
      await bios.deleteInsightConfigs(insightName);
      await bios.putReportConfig(reportConfig);
      await bios.getReportConfigs();
      await bios.deleteReportConfig(reportConfig.reportId);
    };

    it('Admin users', async () => {
      await testInsightOps(adminUser, adminPass, adminReportUser, adminPass);
    });

    it('ML engineers', async () => {
      await testInsightOps(mleUser, mlePass, mleReportUser, mlePass);
    });

    it('Extract users', async () => {
      await testInsightOps(extractUser, extractPass, extractReportUser, extractPass);
    });

    it('Ingest users', async () => {
      await testInsightOps(ingestUser, ingestPass, ingestReportUser, ingestPass);
    });

    it('Report only user', async () => {
      await testInsightOps(null, null, reportUser, reportPass);
    });
  });

  describe('Signal data', () => {
    const signalConfig = {
      signalName: 'signalDataTest',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'hello', type: 'String' },
      ]
    };

    beforeAll(async () => {
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });
      await bios.createSignal(signalConfig);
      await bios.signOut();
    });

    afterAll(async () => {
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });
      await bios.deleteSignal(signalConfig.signalName);
      await bios.signOut();
    });

    const runTest = async (user, pass, expectReadable, expectWritable) => {
      await bios.signIn({
        email: user,
        password: pass,
      });

      // Test signal write
      const insertStatement = bios.iSqlStatement().insert().into(signalConfig.signalName)
        .csv('hello').build();
      if (expectWritable) {
        await bios.execute(insertStatement);
      } else {
        await verifyForbidden(() => bios.execute(insertStatement));
      }

      // Test signal read
      const selectStatement = bios.iSqlStatement().select().from(signalConfig.signalName)
        .timeRange(bios.time.now(), -bios.time.minutes(10)).build();
      if (expectReadable) {
        await bios.execute(selectStatement);
      } else {
        await verifyForbidden(() => bios.execute(selectStatement));
      }
    };

    it('Admin user', async () => {
      await runTest(adminUser, adminPass, true, true);
    });

    it('Admin + report user', async () => {
      await runTest(adminReportUser, adminPass, true, true);
    });

    it('ML enginner', async () => {
      await runTest(mleUser, mlePass, true, true);
    });

    it('ML engineer + report user', async () => {
      await runTest(mleReportUser, mlePass, true, true);
    });

    it('Ingest user', async () => {
      await runTest(ingestUser, ingestPass, false, true);
    });

    it('Ingest + report user', async () => {
      await runTest(ingestReportUser, ingestPass, true, true);
    });

    it('Extract user', async () => {
      await runTest(extractUser, extractPass, true, false);
    });

    it('Extract + report user', async () => {
      await runTest(extractReportUser, extractPass, true, false);
    });

    it('Report only user', async () => {
      await runTest(reportUser, reportPass, true, false);
    });
  });

  describe('Context data', () => {
    const contextConfig = {
      contextName: 'contextDataTest',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'productId', type: 'Integer' },
        { attributeName: 'productName', type: 'String' },
      ],
      primaryKey: ['productId'],
    };

    beforeAll(async () => {
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });
      await bios.createContext(contextConfig);
      await bios.signOut();
    });

    afterAll(async () => {
      await bios.signIn({
        email: adminUser,
        password: adminPass,
      });
      await bios.deleteContext(contextConfig.contextName);
      await bios.signOut();
    });

    const runTest = async (user, pass, expectReadable, expectWritable, expectedRoleNames) => {
      await bios.signIn({
        email: user,
        password: pass,
      });

      const userInfo = await bios.getUserInfo();
      expect(userInfo.roles.map(bios.getRoleName)).toEqual(expectedRoleNames);

      // Test context write
      const upsertStatement = bios.iSqlStatement().upsert().into(contextConfig.contextName)
        .csv('1,hello').build();

      const updateStatement = bios.iSqlStatement().update(contextConfig.contextName)
        .set(bios.isql.attribute('productName', 'hi'))
        .where(bios.isql.key(1)).build();

      const deleteStatement = bios.iSqlStatement().delete()
        .fromContext(contextConfig.contextName)
        .where(bios.isql.key(1))
        .build();

      if (expectWritable) {
        await bios.execute(upsertStatement);
        await bios.execute(updateStatement);
        await bios.execute(deleteStatement);
      } else {
        await verifyForbidden(() => bios.execute(upsertStatement));
        await verifyForbidden(() => bios.execute(updateStatement));
        await verifyForbidden(() => bios.execute(deleteStatement));
      }

      // Test context read
      const selectStatement = bios.iSqlStatement().select().fromContext(contextConfig.contextName)
        .where(bios.isql.key(1)).build();
      if (expectReadable) {
        await bios.execute(selectStatement);
      } else {
        await verifyForbidden(() => bios.execute(selectStatement));
      }
    };

    it('Admin user', async () => {
      await runTest(adminUser, adminPass, true, true, ['Administrator']);
    });

    it('Admin + report user', async () => {
      await runTest(adminReportUser, adminPass, true, true, ['Administrator', 'Business User']);
    });

    it('ML enginner', async () => {
      await runTest(mleUser, mlePass, true, true, ['ML Engineer']);
    });

    it('ML engineer + report user', async () => {
      await runTest(mleReportUser, mlePass, true, true, ['ML Engineer', 'Business User']);
    });

    it('Ingest user', async () => {
      await runTest(ingestUser, ingestPass, false, true, ['Data Engineer']);
    });

    it('Ingest + report user', async () => {
      await runTest(ingestReportUser, ingestPass, true, true, ['Data Engineer', 'Business User']);
    });

    it('Extract user', async () => {
      await runTest(extractUser, extractPass, true, false, ['Data Scientist']);
    });

    it('Extract + report user', async () => {
      await runTest(
        extractReportUser, extractPass, true, false, ['Data Scientist', 'Business User']
      );
    });

    it('Report only user', async () => {
      await runTest(reportUser, reportPass, true, false, ['Business User']);
    });
  });

  describe('Get users', () => {

    const runTest = async (user, pass, expectDoable) => {
      bios.initialize({
        endpoint: BIOS_ENDPOINT,
      });
      await bios.signIn({
        email: user,
        password: pass,
      });

      if (expectDoable) {
        await bios.getUsers();
      } else {
        await verifyForbidden(() => bios.getUsers());
      }
    };

    it('SuperAdmin user', async () => {
      await runTest('superadmin', 'superadmin', true);
    });

    it('Admin user', async () => {
      await runTest(adminUser, adminPass, true);
    });

    it('Admin + report user', async () => {
      await runTest(adminReportUser, adminPass, true);
    });

    it('ML enginner', async () => {
      await runTest(mleUser, mlePass, false);
    });

    it('ML engineer + report user', async () => {
      await runTest(mleReportUser, mlePass, false);
    });

    it('Ingest user', async () => {
      await runTest(ingestUser, ingestPass, false);
    });

    it('Ingest + report user', async () => {
      await runTest(ingestReportUser, ingestPass, false);
    });

    it('Extract user', async () => {
      await runTest(extractUser, extractPass, false);
    });

    it('Extract + report user', async () => {
      await runTest(extractReportUser, extractPass, false);
    });

    it('Report only user', async () => {
      await runTest(reportUser, reportPass, false);
    });
  });
});
