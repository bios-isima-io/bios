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
  INGEST_TIMESTAMP_ROLLUP_TWO_VALUES
} from './tutils';


describe('Select single window', () => {

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT,
    });
    await bios.signIn({
      email: 'admin@jsSimpleTest',
      password: 'admin', // we have to support the other users
    });
  });

  afterAll(async () => {
    await bios.signOut();
  });

  describe('Group by X only', () => {
    it('1a. Fundamental', async () => {
      if (!INGEST_TIMESTAMP_ROLLUP_TWO_VALUES) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ROLLUP_TWO_VALUES is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('count()')
        .from('rollupTwoValues')
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ROLLUP_TWO_VALUES, bios.time.minutes(2), bios.time.minutes(1))
        .build();
      const result = await bios.execute(statement);
      // console.log('RESULT', JSON.stringify(result, null, 2));

      expect(result).toBeInstanceOf(SelectISqlResponse);
      expect(result.definitions).toEqual([{ name: 'count()', type: 'INTEGER' }]);
      expect(result.responseType).toBe(ResponseType.SELECT);
      expect(result.dataWindows.length).toBe(1);
      expect(result.dataWindows[0].records.length).toBe(1);
      expect(result.dataWindows[0].records[0][0]).toBe(19);
    });

    it('1b. Group, order, and limit', async () => {
      if (!INGEST_TIMESTAMP_ROLLUP_TWO_VALUES) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ROLLUP_TWO_VALUES is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'count()')
        .from('rollupTwoValues')
        .groupBy('country')
        .orderBy(bios.isql.order('count()').desc())
        .limit(3)
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ROLLUP_TWO_VALUES, bios.time.minutes(2), bios.time.minutes(1))
        .build();
      const result = await bios.execute(statement);
      // console.log('RESULT', JSON.stringify(result, null, 2));

      expect(result).toBeInstanceOf(SelectISqlResponse);
      expect(result.responseType).toBe(ResponseType.SELECT);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'count()', type: 'INTEGER' },
      ]);
      expect(result.dataWindows.length).toBe(1);
      expect(result.dataWindows[0].records.length).toBe(3);
      expect(result.dataWindows[0].records[0]).toEqual(['US', 9]);
      expect(result.dataWindows[0].records[1]).toEqual(['Australia', 5]);
      expect(result.dataWindows[0].records[2]).toEqual(['Japan', 4]);
    });
  });

  describe('Group by X and Y', () => {
    it('2a. Fundamental', async () => {
      if (!INGEST_TIMESTAMP_ROLLUP_TWO_VALUES) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ROLLUP_TWO_VALUES is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'state', 'count()', 'sum(score)', 'max(duration)')
        .from('rollupTwoValues')
        .where("country IN ('US', 'Australia', 'Japan')")
        .groupBy('country', 'state')
        .orderBy(bios.isql.order('count()').desc())
        .limit(4)
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ROLLUP_TWO_VALUES, bios.time.minutes(2), bios.time.minutes(1))
        .build();
      const result = await bios.execute(statement);
      // console.log('RESULT', JSON.stringify(result, null, 2));

      expect(result).toBeInstanceOf(SelectISqlResponse);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'state', type: 'STRING' },
        { name: 'count()', type: 'INTEGER' },
        { name: 'sum(score)', type: 'INTEGER' },
        { name: 'max(duration)', type: 'DECIMAL' },
      ]);

      expect(result.responseType).toBe(ResponseType.SELECT);
      expect(result.dataWindows.length).toBe(1);
      expect(result.dataWindows[0].records.length).toBe(4);
      expect(result.dataWindows[0].records[0]).toEqual(['US', 'California', 5, 131, 111.2]);
      expect(result.dataWindows[0].records[1]).toEqual(['Australia', 'Queensland', 4, 52, 18.1]);
      expect(result.dataWindows[0].records[2]).toEqual(['US', 'Utah', 3, 134, 71.1]);
      expect(result.dataWindows[0].records[3]).toEqual(['Japan', 'Hok海do', 2, 78, 191.4]);
    });
  });

  describe('Group by X and Y with and without filter', () => {
    it('2a. Fundamental', async () => {
      if (!INGEST_TIMESTAMP_ROLLUP_TWO_VALUES) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ROLLUP_TWO_VALUES is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'state', 'count()')
        .from('rollupTwoValues')
        .where("country in ('US', 'Japan', 'Australia', 'India')"
          + " AND state IN ('California', 'Oregon', 'Utah',"
          + " 'Queensland', 'New South Wales',"
          + " '東京', '大阪', 'Hok海do',"
          + " 'Goa')")
        .groupBy('country', 'state')
        .orderBy(bios.isql.order('count()').desc())
        .limit(20)
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ROLLUP_TWO_VALUES, bios.time.minutes(2), bios.time.minutes(1))
        .build();
      const statement2 = bios.iSqlStatement()
        .select('country', 'count()')
        .from('rollupTwoValues')
        .groupBy('country')
        .orderBy(bios.isql.order('count()').desc())
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ROLLUP_TWO_VALUES, bios.time.minutes(2), bios.time.minutes(1))
        .build();
      const results = await bios.multiExecute(statement, statement2);

      expect(results.length).toBe(2);
      const result1 = results[0];
      const result2 = results[1];
      const aggregates = new Map();
      for (let i = 0; i < result1.dataWindows[0].records.length; ++i) {
        const record = result1.dataWindows[0].records[i];
        const key = record[0];
        const count = aggregates[key] || 0;
        const value = count + record[2];
        aggregates[key] = value;
      }

      result2.dataWindows[0].records.forEach((record) => {
        expect(record[1]).toBe(aggregates[record[0]]);
      });
    });
  });

  describe('Negative cases', () => {
    it('Invalid interval', async () => {
      if (!INGEST_TIMESTAMP_ROLLUP_TWO_VALUES) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ROLLUP_TWO_VALUES is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('count()')
        .from('rollupTwoValues')
        .groupBy('country', 'state')
        .orderBy(bios.isql.order('count()').desc())
        .tumblingWindow(bios.time.seconds(90))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ROLLUP_TWO_VALUES, bios.time.minutes(2), bios.time.minutes(1))
        .build();
      try {
        await bios.execute(statement, statement);
      } catch (e) {
        expect(e).toBeInstanceOf(BiosClientError);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }
    });

    it('Invalid snap step size', async () => {
      if (!INGEST_TIMESTAMP_ROLLUP_TWO_VALUES) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_ROLLUP_TWO_VALUES is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('count()')
        .from('rollupTwoValues')
        .groupBy('country', 'state')
        .orderBy(bios.isql.order('count()').desc())
        .tumblingWindow(bios.time.minutes(2))
        .snappedTimeRange(
          INGEST_TIMESTAMP_ROLLUP_TWO_VALUES, bios.time.minutes(2), bios.time.seconds(59))
        .build();
      try {
        await bios.execute(statement, statement);
      } catch (e) {
        expect(e).toBeInstanceOf(BiosClientError);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }
    });
  });
});
