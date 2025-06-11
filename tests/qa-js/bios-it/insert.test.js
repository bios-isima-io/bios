/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

'use strict';

import bios from 'bios-sdk';

import {
  BIOS_ENDPOINT,
  clearTenant,
  canonicalizeUuid,
} from './tutils';
import {
  BiosClientError,
  BiosErrorType,
  InsertISqlResponse,
  ResponseType,
} from 'bios-sdk';

describe('Insert', () => {
  const tenantName = 'insertTest';
  const signalName = 'insertTestSignal1';
  const signalName2 = 'insertTestSignal2';

  beforeAll(async () => {
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, false);
    await bios.createTenant({
      tenantName,
    });
    await bios.logout();
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: `admin@${tenantName}`,
      password: 'admin',
    });
    await bios.createSignal({
      signalName,
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'stringAttr', type: 'String' },
        { attributeName: 'integerAttr', type: 'Integer' },
      ]
    });
    await bios.createSignal({
      signalName: signalName2,
      missingAttributePolicy: 'Reject',
      attributes: [
        { attributeName: 'state', type: 'String' },
        { attributeName: 'city', type: 'String' },
        { attributeName: 'value', type: 'Decimal' },
      ]
    });
  });

  afterAll(async () => {
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: 'superadmin',
      password: 'superadmin',
    });
    await clearTenant(tenantName, true);
    await bios.logout();
  });

  describe('Normal cases', () => {
    it('Fundamental CSV', async () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into(signalName)
        .csv('hello,1')
        .build();
      const response = await bios.execute(statement);

      expect(response).toBeInstanceOf(InsertISqlResponse);
      expect(response.responseType).toBe(ResponseType.INSERT);
      expect(response.records.length).toBe(1);
      expect(response.records[0].eventId).toEqual(canonicalizeUuid(statement.data.eventIds[0]));
      expect(response.records[0].timestamp).toBeGreaterThan(0);

      const query = bios.iSqlStatement()
        .select()
        .from(signalName)
        .timeRange(response.records[0].timestamp, 1)
        .build();

      const extracted = await bios.execute(query);
      expect(extracted.dataWindows.length).toBe(1);
      const dataWindow = extracted.dataWindows[0];
      expect(dataWindow.records.length).toBe(1);
      expect(dataWindow.records[0][0]).toBe('hello');
      expect(dataWindow.records[0][1]).toBe(1);
    });

    it('Fundamental Values', async () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into(signalName)
        .values(['"enclosed in double quotes"', 1])
        .build();
      const response = await bios.execute(statement);

      expect(response).toBeInstanceOf(InsertISqlResponse);
      expect(response.responseType).toBe(ResponseType.INSERT);
      expect(response.records.length).toBe(1);
      expect(response.records[0].eventId).toEqual(canonicalizeUuid(statement.data.eventIds[0]));
      expect(response.records[0].timestamp).toBeGreaterThan(0);

      const query = bios.iSqlStatement()
        .select()
        .from(signalName)
        .timeRange(response.records[0].timestamp, 1)
        .build();

      const extracted = await bios.execute(query);
      expect(extracted.dataWindows.length).toBe(1);
      const dataWindow = extracted.dataWindows[0];
      expect(dataWindow.records.length).toBe(1);
      expect(dataWindow.records[0][0]).toBe('"enclosed in double quotes"');
      expect(dataWindow.records[0][1]).toBe(1);
    });

    it('Bulk values', async () => {
      const numEvents = 3891;
      const events = new Array(numEvents);
      events[0] = ['With, a comma', 0];
      events[1] = ['With \n newline', 1];
      events[2] = ['"enclosed in double quotes"', 2];
      events[3] = ['a single "double quote', 3];
      for (let i = 4; i < numEvents; ++i) {
        let event = [i.toString(), i];
        events[i] = event;
      }

      const statement = bios.iSqlStatement()
        .insert()
        .into(signalName)
        .valuesBulk(...events)
        .build();
      const response = await bios.execute(statement);
      expect(response.records).toBeDefined();
      expect(response.records.length).toBe(numEvents);
      let startTime;
      let endTime;
      response.records.forEach((response, i) => {
        expect(response.eventId).toEqual(canonicalizeUuid(statement.data.eventIds[i]));
        if (!startTime) {
          startTime = response.timestamp;
        }
        endTime = response.timestamp;
      })

      const query = bios.iSqlStatement()
        .select()
        .from(signalName)
        .timeRange(startTime, endTime - startTime + 1)
        .build();

      const extracted = await bios.execute(query);
      expect(extracted.dataWindows[0].records.length).toBe(numEvents);
      extracted.dataWindows[0].records.forEach((event, i) => {
        expect(event).toEqual(events[i]);
      });

    });

    it('Bulk csv', async () => {
      const numEvents = 479;
      const events = new Array(numEvents);
      for (let i = 0; i < numEvents; ++i) {
        let event = i.toString() + ',' + i;
        events[i] = event;
      }

      const statement = bios.iSqlStatement()
        .insert()
        .into(signalName)
        .csvBulk(...events)
        .build();
      const response = await bios.execute(statement);
      expect(response.records).toBeDefined();
      expect(response.records.length).toBe(numEvents);
      let startTime;
      let endTime;
      response.records.forEach((response, i) => {
        expect(response.eventId).toEqual(canonicalizeUuid(statement.data.eventIds[i]));
        if (!startTime) {
          startTime = response.timestamp;
        }
        endTime = response.timestamp;
      })

      const query = bios.iSqlStatement()
        .select()
        .from(signalName)
        .timeRange(startTime, endTime - startTime + 1)
        .build();

      const extracted = await bios.execute(query);
      expect(extracted.dataWindows[0].records.length).toBe(numEvents);
      extracted.dataWindows[0].records.forEach((event, i) => {
        expect(event[0]).toEqual(i.toString());
        expect(event[1]).toEqual(i);
      });

    });
  });

  describe('Negative cases', () => {
    it('Wrong signal name', async () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into('noSuchSignal')
        .csv('hello,1')
        .build();
      try {
        await bios.execute(statement);
        fail('exception is expected');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_STREAM.errorCode);
      }
    });

    it('Partial failure: Invalid input', async () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into(signalName)
        .csv('hello,1')
        .csv('hello2,2')
        .csv('this,is,wrong')
        .csv('hello3,3')
        .build();
      try {
        await bios.execute(statement);
        fail('exception is expected');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }
    });
  });
})
