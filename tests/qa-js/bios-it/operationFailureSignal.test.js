'use strict';

import bios from 'bios-sdk';
import { BIOS_ENDPOINT, sleep } from './tutils';

describe('_operationFailure signal test', () => {
  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'admin@jsSimpleTest',
      password: 'admin',
    });
    const signalConfig = {
      'signalName': 'testerrors4signal',
      'missingAttributePolicy': 'Reject',
      'attributes': [
        { 'attributeName': 'orderId', 'type': 'Integer' },
        { 'attributeName': 'customerId', 'type': 'Integer' },
        { 'attributeName': 'deliveryTime', 'type': 'Integer' },
        { 'attributeName': 'zipCode', 'type': 'String' }
      ],
      'postStorageStage': {
        'features': [
          {
            'featureName': 'byZipcode',
            'dimensions': ['zipCode'],
            'attributes': ['deliveryTime'],
            'featureInterval': 60000,
            'featureAsContext': { 'state': 'Enabled' }
          }]
      }
    }
    try {
      await bios.deleteSignal(signalConfig.signalName);
    } catch (e) {
    }
    await bios.createSignal(signalConfig);

    const contextConfig = {
      contextName: 'testerrors4Context',
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'product_id', 'type': 'Integer' },
        {
          attributeName: 'product_name', type: 'String',
          missingAttributePolicy: 'StoreDefaultValue', default: 'missing_product_name'
        },
        {
          attributeName: 'manufacturer_name', type: 'String',
          missingAttributePolicy: 'StoreDefaultValue', default: 'missing_manufacturer_name'
        },
        { attributeName: 'price', type: 'Decimal' }
      ],
      primaryKey: ['product_id'],
    };
    try {
      await bios.deleteContext(contextConfig.contextName);
    } catch (e) {
    }
    await bios.createContext(contextConfig);
  });

  afterAll(async () => {
    await bios.signOut();
  });

  it('Get _operationFailure stream', async () => {
    const tenant = await bios.getTenant();
    const signal =
      tenant.signals.find((signal) => signal.signalName === '_operationFailure');
    expect(signal).toBeDefined();
  });

  it('ingest signal error into _operationFailure signal', async () => {
    let clockMargin = bios.time.seconds(1);
    await sleep(clockMargin);

    const signalName = 'testerrors4signal';
    const uuid1 = "8f279406-de60-11eb-ba80-0242ac130004";
    const uuid2 = "8f279672-de60-11eb-ba80-0242ac130004";
    const start = bios.time.now();
    const ingestStatement = bios.iSqlStatement()
      .insert().into(signalName).values([1, uuid1, 1624526179, 'abc']).build();
    try {
      await bios.execute(ingestStatement);
    } catch (e) {
      // ignore error
    }

    await sleep(2000);

    let now = bios.time.now();
    const selectStatement = bios.iSqlStatement().select().from('_operationFailure')
      .timeRange(start - clockMargin, now - start + clockMargin).build();
    const response1 = await bios.execute(selectStatement);
    expect(response1.dataWindows[0].records.length).toBe(1);
    expect(response1.dataWindows[0].records[0][1]).toBe(signalName);
    expect(response1.dataWindows[0].records[0][3]).toBe('INVALID_VALUE_SYNTAX');

    const start2 = bios.time.now();
    const bulkIngestStatement = bios.iSqlStatement().insert().into(signalName)
      .valuesBulk([1, 1, 2, '560102'], [2, 1624526179, uuid2, '123'],
        [3, 1, 1624526179, 'xyz']).build();
    try {
      await bios.execute(bulkIngestStatement);
    } catch (e) {
      // ignore error
    }

    await sleep(2000);

    now = bios.time.now();
    const selectStatement2 = bios.iSqlStatement().select().from('_operationFailure')
      .timeRange(start2 - clockMargin, now - start2 + clockMargin).build();
    const response2 = await bios.execute(selectStatement2);
    expect(response2.dataWindows[0].records.length).toBe(1);
    expect(response2.dataWindows[0].records[0][1]).toBe(signalName);
    expect(response2.dataWindows[0].records[0][3]).toBe('INVALID_VALUE_SYNTAX');
  });

  it('ingest context error into _operationFailure signal', async () => {
    let clockMargin = bios.time.seconds(1);
    await sleep(clockMargin);

    const contextName = 'testerrors4Context';
    const uuid1 = "8f279762-de60-11eb-ba80-0242ac130004";
    const uuid2 = "8f27982a-de60-11eb-ba80-0242ac130004";
    const start1 = bios.time.now();
    const ingestStatement = bios.iSqlStatement()
      .upsert()
      .into(contextName)
      .csv(`1,tablet,apple,${uuid1}`)
      .build();
    try {
      await bios.execute(ingestStatement);
    } catch (e) {
      // ignore error
    }

    await sleep(2000);

    const selectStatement = bios.iSqlStatement().select().from('_operationFailure')
      .timeRange(start1 - clockMargin, bios.time.now() - start1 + clockMargin).build();
    const response1 = await bios.execute(selectStatement);
    expect(response1.dataWindows[0].records.length).toBe(1);
    expect(response1.dataWindows[0].records[0][1]).toBe(contextName);
    expect(response1.dataWindows[0].records[0][3]).toBe('INVALID_VALUE_SYNTAX');

    const start2 = bios.time.now();
    const bulkIngestStatement = bios.iSqlStatement()
      .upsert()
      .into(contextName)
      .csvBulk([
        `1,tablet,apple,${uuid2}`,
        '2,smartphone,samsung,1222.3',
        `3,tv,sony,${uuid2}`,
      ]).build();

    try {
      await bios.execute(bulkIngestStatement);
    } catch (e) {
      // ignore error
    }

    await sleep(2000);

    const selectStatement2 = bios.iSqlStatement().select().from('_operationFailure')
      .timeRange(start2 - clockMargin, bios.time.now() - start2 + clockMargin).build();
    const response2 = await bios.execute(selectStatement2);
    expect(response2.dataWindows[0].records.length).toBe(1);
    expect(response2.dataWindows[0].records[0][1]).toBe(contextName);
    expect(response2.dataWindows[0].records[0][3]).toBe('INVALID_VALUE_SYNTAX');
  });
});
