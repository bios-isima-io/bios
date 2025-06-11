'use strict';

import bios, {
  BiosClientError,
  BiosErrorType,
  ImportDataMappingType,
  ImportDestinationType,
  ImportSourceType,
  IntegrationsAuthenticationType
} from 'bios-sdk';
import { BIOS_ENDPOINT } from './tutils';


describe('Integrations', () => {

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'admin@jsSimpleTest',
      password: 'admin',
    });
    return null;
  });

  afterAll(async () => await bios.signOut());

  afterEach(async () => {
    const tenantConfig = await bios.getTenant({ detail: true });
    for (let i = 0; i < tenantConfig.importFlowSpecs.length; ++i) {
      const entry = tenantConfig.importFlowSpecs[i];
      await bios.deleteImportFlowSpec(entry.importFlowId);
    }
    for (let i = 0; i < tenantConfig.importSources.length; ++i) {
      const entry = tenantConfig.importSources[i];
      await bios.deleteImportSource(entry.importSourceId);
    }
    for (let i = 0; i < tenantConfig.importDestinations.length; ++i) {
      const entry = tenantConfig.importDestinations[i];
      await bios.deleteImportDestination(entry.importDestinationId);
    }
    for (let i = 0; i < tenantConfig.importDataProcessors.length; ++i) {
      const entry = tenantConfig.importDataProcessors[i];
      await bios.deleteImportDataProcessor(entry.processorName);
    }
    for (let i = 0; i < tenantConfig.signals.length; ++i) {
      const entry = tenantConfig.signals[i];
      if (entry.signalName === 'impressionSignal') {
        await bios.deleteSignal(entry.signalName);
        break;
      }
    }
  });

  it('Import source', async () => {
    const importSourceConfig = {
      importSourceId: "205",
      importSourceName: "Webhook Covid Data Source",
      type: ImportSourceType.KAFKA,
      webhookPath: "/deli/pharmeasy/ct",
      authentication: {
        type: IntegrationsAuthenticationType.SASL_PLAINTEXT,
      }
    };

    // create
    const created = await bios.createImportSource(importSourceConfig);
    const importSourceId = created.importSourceId;
    expect(importSourceId).toBe(importSourceConfig.importSourceId);
    expect(created).toEqual(importSourceConfig);

    // adding with the same ID would cause conflict
    try {
      await bios.createImportSource(importSourceConfig);
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.RESOURCE_ALREADY_EXISTS.errorCode);
    }

    // adding without ID would create another entry
    const entryWithoutId = { ...importSourceConfig };
    delete entryWithoutId.importSourceId;
    const created2 = await bios.createImportSource(entryWithoutId);
    const importSourceId2 = created2.importSourceId;
    expect(importSourceId2).toBeInstanceOf(String);
    expect(importSourceId2).not.toBe(importSourceId);
    const reference1 = {
      ...entryWithoutId,
      importSourceId: importSourceId2,
    };
    expect(created2).toEqual(reference1);

    // get
    const retrieved = await bios.getImportSource(importSourceId);
    expect(retrieved).toEqual(importSourceConfig);
    const retrieved2 = await bios.getImportSource(importSourceId2);
    expect(retrieved2).toEqual(reference1)

    // update
    const reference2 = {
      ...importSourceConfig,
      type: ImportSourceType.WEBHOOK,
      webhookPath: '/modified/path',
      importSourceName: 'ウェブフック',
      authentication: {
        type: IntegrationsAuthenticationType.LOGIN,
        user: 'my_user',
        password: 'my_password',
      }
    }
    const modified = await bios.updateImportSource(importSourceId, reference2);
    expect(modified).toEqual(reference2);
    const retrieved3 = await bios.getImportSource(importSourceId);
    expect(retrieved3).toEqual(reference2);

    // get config via getTenant
    const tenantConfig = await bios.getTenant({ detail: true });
    expect(tenantConfig.importSources.length).toBe(2);
    expect(tenantConfig.importSources).toContain(reference2);

    // delete
    await bios.deleteImportSource(importSourceId);
    try {
      await bios.getImportSource(importSourceId);
      fail('Exception must happen')
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
    }
    const tenantConfig2 = await bios.getTenant({ detail: true });
    expect(tenantConfig2.importSources.length).toBe(1);
  });

  it('Import destination', async () => {
    const importDestinationConfig = {
      importDestinationId: "101",
      type: ImportDestinationType.BIOS,
      endpoint: "https://bios.isima.io",
      authentication: {
        type: "Login",
        user: "ravid@isima.io",
        password: "Test123!"
      }
    };

    // create
    const created = await bios.createImportDestination(importDestinationConfig);
    expect(created).toEqual(importDestinationConfig);
    const importDestinationId = created.importDestinationId;
    expect(importDestinationId).toBe(importDestinationConfig.importDestinationId);

    // adding with the same ID would cause conflict
    try {
      await bios.createImportDestination(importDestinationConfig);
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.RESOURCE_ALREADY_EXISTS.errorCode);
    }

    // adding without ID would create another entry
    const entryWithoutId = { ...importDestinationConfig };
    delete entryWithoutId.importDestinationId;
    const created2 = await bios.createImportDestination(entryWithoutId);
    const importDestinationId2 = created2.importDestinationId;
    expect(importDestinationId2).toBeInstanceOf(String);
    expect(importDestinationId2).not.toBe(importDestinationId);
    const reference1 = {
      ...entryWithoutId,
      importDestinationId: importDestinationId2,
    };
    expect(created2).toEqual(reference1);

    // get
    const retrieved = await bios.getImportDestination(importDestinationId);
    expect(retrieved).toEqual(importDestinationConfig);
    const retrieved2 = await bios.getImportDestination(importDestinationId2);
    expect(retrieved2).toEqual(reference1);


    // update
    const reference2 = {
      ...importDestinationConfig,
      endpoint: 'https://bios.isima.io:8443',
      importDestinationName: 'ウェブフック',
    }
    const modified = await bios.updateImportDestination(importDestinationId, reference2);
    expect(modified).toEqual(reference2);
    const retrieved3 = await bios.getImportDestination(importDestinationId);
    expect(retrieved3).toEqual(reference2);

    // get config via getTenant
    const tenantConfig = await bios.getTenant({ detail: true });
    expect(tenantConfig.importDestinations.length).toBe(2);
    expect(tenantConfig.importDestinations).toContain(reference2);

    // delete
    await bios.deleteImportDestination(importDestinationId);
    try {
      await bios.getImportDestination(importDestinationId);
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
    }
    const tenantConfig2 = await bios.getTenant({ detail: true });
    expect(tenantConfig2.importDestinations.length).toBe(1);
  });

  it('Import flow spec', async () => {
    const importSourceConfig = {
      importSourceId: "215",
      importSourceName: "Webhook Covid Data Source",
      type: ImportSourceType.WEBHOOK,
      webhookPath: "/ct",
      authentication: {
        type: IntegrationsAuthenticationType.IN_MESSAGE,
      }
    };

    const importDestinationConfig = {
      importDestinationId: "111",
      type: ImportDestinationType.BIOS,
      endpoint: "https://bios.isima.io",
      authentication: {
        type: "Login",
        user: "ravid@isima.io",
        password: "Test123!"
      }
    };

    const signalConfig = {
      signalName: "impressionSignal",
      missingAttributePolicy: "Reject",
      attributes: [
        {
          attributeName: "geoId",
          type: "String"
        },
        {
          attributeName: "cases",
          type: "Integer"
        },
        {
          attributeName: "deaths",
          type: "Integer"
        },
        {
          attributeName: "dataRep",
          type: "String"
        }
      ]
    };

    const importFlowSpec = {
      importFlowName: "Impressions",
      importFlowId: "305",
      sourceDataSpec: {
        importSourceId: "215",
        webhookSubPath: "impressions",
        payloadType: ImportDataMappingType.JSON,
      },
      destinationDataSpec: {
        importDestinationId: "111",
        type: "Signal",
        name: "impressionSignal",
      },
      dataPickupSpec: {
        attributeSearchPath: "abc/*/def",
        attributes: [
          { sourceAttributeName: "geoId" },
          { sourceAttributeName: "cases" },
          { sourceAttributeName: "deaths" },
          { sourceAttributeName: "dataRep" },
        ]
      },
      checkpointingEnabled: true,
      acknowledgementEnabled: true
    };

    // prep
    await bios.createSignal(signalConfig);
    await bios.createImportDestination(importDestinationConfig);
    await bios.createImportSource(importSourceConfig);

    // create
    const created = await bios.createImportFlowSpec(importFlowSpec);
    expect(created).toEqual(importFlowSpec);
    const importFlowId = created.importFlowId;

    // adding with the same ID would cause conflict
    try {
      await bios.createImportFlowSpec(importFlowSpec);
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.RESOURCE_ALREADY_EXISTS.errorCode);
    }

    // adding without ID would create another entry
    const entryWithoutId = { ...importFlowSpec };
    delete entryWithoutId.importFlowId;
    const created2 = await bios.createImportFlowSpec(entryWithoutId);
    const importFlowId2 = created2.importFlowId;
    expect(importFlowId2).toBeInstanceOf(String);
    expect(importFlowId2).not.toBe(importFlowId);
    const reference1 = {
      ...entryWithoutId,
      importFlowId: importFlowId2
    };
    expect(created2).toEqual(reference1);

    // get
    const retrieved = await bios.getImportFlowSpec(importFlowId);
    expect(retrieved).toEqual(importFlowSpec);
    const retrieved2 = await bios.getImportFlowSpec(importFlowId2);
    expect(retrieved2).toEqual(reference1);

    // update
    const reference2 = {
      ...reference1,
      importFlowName: 'Clicks',
    }
    const modified = await bios.updateImportFlowSpec(importFlowId, reference2);
    reference2.importFlowId = importFlowId;
    expect(modified).toEqual(reference2);
    const retrieved3 = await bios.getImportFlowSpec(importFlowId);
    expect(retrieved3).toEqual(reference2);

    // get config via getTenant
    const tenantConfig = await bios.getTenant({ detail: true });
    expect(tenantConfig.importFlowSpecs.length).toBe(2);
    expect(tenantConfig.importFlowSpecs).toContain(reference2);

    // delete
    await bios.deleteImportFlowSpec(importFlowId);
    try {
      await bios.getImportFlowSpec(importFlowId);
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
    }
    const tenantConfig2 = await bios.getTenant({ detail: true });
    expect(tenantConfig2.importFlowSpecs.length).toBe(1);
  });

  it('Import data processor', async () => {
    const processorCode = `
    def process(record):
        return record + 'しっぽ'
    `;

    // create
    const processorName = 'processCovidData';
    await bios.createImportDataProcessor(processorName, processorCode);

    // get
    const retrieved = await bios.getImportDataProcessor(processorName);
    expect(retrieved).toBe(processorCode);

    // update
    const processorCode2 = `
    def process(record):
        record['additionalAttribute'] = record.get('score') * 2
        return (record, 'あくまでもしっぽ')
    `
    await bios.updateImportDataProcessor(processorName, processorCode2);
    const retrieved2 = await bios.getImportDataProcessor(processorName);
    expect(retrieved2).toEqual(processorCode2);

    // get config via getTenant
    const tenantConfig = await bios.getTenant({ detail: true });
    expect(tenantConfig.importDataProcessors.length).toBe(1);
    const registeredCode2 = tenantConfig.importDataProcessors[0].code;
    expect(registeredCode2).toEqual(Buffer.from(processorCode2).toString('base64'));
    expect(Buffer.from(registeredCode2, 'base64').toString()).toEqual(processorCode2);

    // delete
    await bios.deleteImportDataProcessor(processorName);
    try {
      await bios.getImportDataProcessor(processorName);
      fail('Exception must happen');
    } catch (e) {
      expect(e instanceof BiosClientError).toBe(true);
      expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
    }
    const tenantConfig2 = await bios.getTenant({ detail: true });
    expect(tenantConfig2.importDataProcessors.length).toBe(0);
  });

  describe('Export destination', () => {

    beforeEach(async () => {
      const dependents = ['exporting', 'exporting2'];
      for (let signalName of dependents) {
        try {
          await bios.deleteSignal(signalName);
        } catch (e) {
          // ignore
        }
      }
      const tenantConfig = await bios.getTenant({ detail: true });
      for (let dest of tenantConfig.exportDestinations || []) {
        try {
          await bios.deleteExportDestination(dest.exportDestinationId);
        } catch (e) {
          // ignore
        }
      }
    });

    xit('Fundamental', async () => {
      const exportDestinationName = 'exportToS3'
      const exportDestinationId = 'destinationCreationTest';

      // You can't add a non-existing destination to a signal
      const signal1 = {
        signalName: 'exporting',
        missingAttributePolicy: 'Reject',
        attributes: [{
          attributeName: 'one',
          type: 'String'
        }],
        exportDestinationId,
      };
      try {
        await bios.createSignal(signal1);
        fail('Exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }

      const signal2 = {
        signalName: 'exporting2',
        missingAttributePolicy: 'Reject',
        attributes: [{
          attributeName: 'one',
          type: 'String'
        }],
      };
      await bios.createSignal(signal2);
      try {
        await bios.updateSignal('exporting2',
          { ...signal2, exportDestinationId });
        fail('Exception must happen')
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }

      // create an export destination
      const exportDestinationConfig = {
        exportDestinationId,
        exportDestinationName,
        storageType: 'S3',
        status: 'Enabled',
        storageConfig: {
          s3BucketName: 'export-s3-bios-test',
          s3AccessKeyId: '',
          s3SecretAccessKey: '',
          s3Region: 'ap-south-1',
        }
      };

      const created = await bios.createExportDestination(exportDestinationConfig);
      expect(created).toEqual(exportDestinationConfig);

      // adding with the same ID would cause conflict
      try {
        await bios.createExportDestination(exportDestinationConfig);
        fail('Exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.RESOURCE_ALREADY_EXISTS.errorCode);
      }

      // Creating a depending signal is possible now
      await bios.createSignal(signal1);

      await bios.updateSignal('exporting2',
        { ...signal2, exportDestinationId });

      // get
      const retrieved = await bios.getExportDestination(exportDestinationId);

      // update
      const reference2 = {
        ...exportDestinationConfig,
        status: 'Disabled',
      }
      const modified = await bios.updateExportDestination(exportDestinationId, reference2);
      expect(modified).toEqual(reference2);
      const retrieved3 = await bios.getExportDestination(exportDestinationId);
      expect(retrieved3).toEqual(reference2);

      // get config via getTenant
      const tenantConfig = await bios.getTenant({ detail: true });
      expect(tenantConfig.exportDestinations.length).toBe(1);
      expect(tenantConfig.exportDestinations).toContain(reference2);

      // delete would fail when a signal is using the destination
      try {
        await bios.deleteExportDestination(exportDestinationId);
        fail('Exception must happen')
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }
      await bios.deleteSignal('exporting');
      try {
        await bios.deleteExportDestination(exportDestinationId);
        fail('Exception must happen')
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }
      await bios.updateSignal(signal2.signalName, signal2);

      // delete
      await bios.deleteExportDestination(exportDestinationId);
      try {
        await bios.getExportDestination(exportDestinationId);
        fail('Exception must happen')
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NOT_FOUND.errorCode);
      }
      const tenantConfig2 = await bios.getTenant({ detail: true });
      expect(tenantConfig2.destinationTargets).toBeUndefined();
    });
  });
});
