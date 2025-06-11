/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

'use strict';

import bios, {
  BiosClientError,
  BiosErrorType,
  ResponseType,
  SelectISqlResponse
} from 'bios-sdk';
import {
  BIOS_ENDPOINT,
  INGEST_TIMESTAMP_ON_THE_FLY_TEST,
} from './tutils';

describe('OnTheFly query test', () => {
  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    })
    await bios.signIn({
      email: 'admin@onTheFlySelectTest',
      password: 'admin',
    });
  });

  afterAll(async () => {
    await bios.signOut();
  });


  describe('Tumbling window', () => {

    it('default', async () => {
      if (!INGEST_TIMESTAMP_ON_THE_FLY_TEST) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ON_THE_FLY_TEST is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('count()')
        .from('onTheFlyTestSignal')
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ON_THE_FLY_TEST, bios.time.minutes(4), bios.time.minutes(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.dataWindows.length).toBe(1);
      const dataWindow0 = result.dataWindows[0];
      expect(dataWindow0.records[0][0]).toBe(19);
    });

    it('onTheFly', async () => {
      if (!INGEST_TIMESTAMP_ON_THE_FLY_TEST) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ON_THE_FLY_TEST is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('count()', 'avg(score)', 'distinctcount(city)')
        .from('onTheFlyTestSignal')
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ON_THE_FLY_TEST, bios.time.minutes(4) + 1, bios.time.minutes(1))
        .onTheFly()
        .build();

      const result = await bios.execute(statement);
      expect(result.dataWindows.length).toBe(3);
      expect(result.dataWindows[0].records[0][0]).toBe(19);
      expect(result.dataWindows[0].records[0][1]).toBeCloseTo(486 / 19, 5);
      expect(result.dataWindows[0].records[0][2]).toBe(9);
      expect(result.dataWindows[1].records[0][0]).toBe(8);
      expect(result.dataWindows[1].records[0][1]).toBeCloseTo(34.5, 5);
      expect(result.dataWindows[1].records[0][2]).toBe(7);
      expect(result.dataWindows[2].records.length).toBe(0);
    });

    it('group by country, onTheFly', async () => {
      if (!INGEST_TIMESTAMP_ON_THE_FLY_TEST) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ON_THE_FLY_TEST is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('count()', 'avg(score)', 'distinctcount(city)')
        .from('onTheFlyTestSignal')
        .groupBy('country')
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ON_THE_FLY_TEST, bios.time.minutes(4) + 1, bios.time.minutes(1))
        .onTheFly()
        .build();

      const result = await bios.execute(statement);
      expect(result.dataWindows.length).toBe(3);
      const window0 = result.dataWindows[0];
      expect(window0.records.length).toBe(4);
      expect(window0.records[0][0]).toBe('Australia');
      expect(window0.records[0][1]).toBe(5);
      expect(window0.records[0][2]).toBeCloseTo(103 / 5, 5);
      expect(window0.records[0][3]).toBe(3);
      expect(window0.records[1][0]).toBe('India');
      expect(window0.records[1][1]).toBe(1);
      expect(window0.records[1][2]).toBeCloseTo(17, 5);
      expect(window0.records[1][3]).toBe(1);
      expect(window0.records[2][0]).toBe('Japan');
      expect(window0.records[2][1]).toBe(4);
      expect(window0.records[2][2]).toBeCloseTo(24, 5);
      expect(window0.records[2][3]).toBe(2);
      expect(window0.records[3][0]).toBe('US');
      expect(window0.records[3][1]).toBe(9);
      expect(window0.records[3][2]).toBeCloseTo(270 / 9, 5);
      expect(window0.records[3][3]).toBe(5);

      const window1 = result.dataWindows[1];
      expect(window1.records.length).toBe(5);
      expect(window1.records[0][0]).toBe('Australia');
      expect(window1.records[0][1]).toBe(1);
      expect(window1.records[0][2]).toBeCloseTo(13, 5);
      expect(window1.records[0][3]).toBe(1);
      expect(window1.records[1][0]).toBe('France');
      expect(window1.records[1][1]).toBe(2);
      expect(window1.records[1][2]).toBeCloseTo(13, 5);
      expect(window1.records[1][3]).toBe(2);
      expect(window1.records[2][0]).toBe('India');
      expect(window1.records[2][1]).toBe(2);
      expect(window1.records[2][2]).toBeCloseTo(43.5, 5);
      expect(window1.records[2][3]).toBe(1);
      expect(window1.records[3][0]).toBe('Japan');
      expect(window1.records[3][1]).toBe(1);
      expect(window1.records[3][2]).toBeCloseTo(88, 5);
      expect(window1.records[3][3]).toBe(1);
      expect(window1.records[4][0]).toBe('US');
      expect(window1.records[4][1]).toBe(2);
      expect(window1.records[4][2]).toBeCloseTo(31, 5);
      expect(window1.records[4][3]).toBe(2);
    });
  });
});
