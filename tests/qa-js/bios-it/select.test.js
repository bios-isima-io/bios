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
  INGEST_TIMESTAMP_MINIMUM,
  INGEST_TIMESTAMP_SIMPLE_ROLLUP
} from './tutils';


describe('Select and getInsights', () => {

  beforeAll(async () => {
    await bios.login({
      endpoint: BIOS_ENDPOINT,
      email: 'admin@jsSimpleTest',
      password: 'admin', // we have to support the other users
    });
  });

  afterAll(async () => {
    await bios.logout();
  });

  describe('Global window', () => {
    it('fundamental', async () => {
      if (!INGEST_TIMESTAMP_MINIMUM) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select()
        .from('minimum')
        .timeRange(INGEST_TIMESTAMP_MINIMUM, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result).toBeInstanceOf(SelectISqlResponse);
      expect(result.responseType).toBe(ResponseType.SELECT);
      expect(result.definitions).toEqual([{ name: 'one', type: 'STRING' }]);
      expect(result.dataWindows[0].records.length).toBe(1);
      expect(result.dataWindows[0].records[0][0]).toBe('hello');
    });

    it('Select with specifying the attribute', async () => {
      if (!INGEST_TIMESTAMP_MINIMUM) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('one')
        .from('minimum')
        .timeRange(INGEST_TIMESTAMP_MINIMUM, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.definitions).toEqual([{ name: 'one', type: 'STRING' }]);
      expect(result.getDataWindows()[0].records.length).toBe(1);
      expect(result.getDataWindows()[0].records[0][0]).toBe('hello');
    });

    it('Select with group (sum)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'sum(value)')
        .from('simpleRollup')
        .groupBy('country')
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'sum(value)', type: 'INTEGER' },
      ]);
      expect(result.data[0].records.length).toBe(2);
      expect(result.data[0].records[0][0]).toBe('japan');
      expect(result.data[0].records[0][1]).toBe(7);
      expect(result.data[0].records[1][0]).toBe('us');
      expect(result.data[0].records[1][1]).toBe(7);
    });

    it('Select with group (avg)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'avg(value)')
        .from('simpleRollup')
        .groupBy('country')
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'avg(value)', type: 'DECIMAL' },
      ]);
      expect(result.data[0].records.length).toBe(2);
      expect(result.data[0].records[0][0]).toBe('japan');
      expect(result.data[0].records[0][1]).toBeCloseTo(7);
      expect(result.data[0].records[1][0]).toBe('us');
      expect(result.data[0].records[1][1]).toBeCloseTo(3.5);
    });

    it('Select (min) with group order by the value (asc)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'min(value)')
        .from('simpleRollup')
        .groupBy('country')
        .orderBy({ key: 'min(value)', order: 'asc' })
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'min(value)', type: 'INTEGER' },
      ]);
      expect(result.data[0].records.length).toBe(2);
      expect(result.data[0].records[0][0]).toBe('us');
      expect(result.data[0].records[0][1]).toBe(2);
      expect(result.data[0].records[1][0]).toBe('japan');
      expect(result.data[0].records[1][1]).toBe(7);
    });

    it('Select (min) with group order by the value (desc)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'min(value)')
        .from('simpleRollup')
        .groupBy('country')
        .orderBy({ key: 'min(value)', order: 'desc' })
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'min(value)', type: 'INTEGER' },
      ]);
      expect(result.data[0].records.length).toBe(2);
      expect(result.data[0].records[0][0]).toBe('japan');
      expect(result.data[0].records[0][1]).toBe(7);
      expect(result.data[0].records[1][0]).toBe('us');
      expect(result.data[0].records[1][1]).toBe(2);
    });

    it('Select (min) with group order by the value (asc) with limitation', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'min(value)')
        .from('simpleRollup')
        .groupBy('country')
        .orderBy({ key: 'min(value)', order: 'asc' })
        .limit(1)
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'min(value)', type: 'INTEGER' },
      ]);
      expect(result.data[0].records.length).toBe(1);
      expect(result.data[0].records[0][0]).toBe('us');
      expect(result.data[0].records[0][1]).toBe(2);
    });

    it('Select with group (last)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'last(state)')
        .from('simpleRollup')
        .groupBy('country')
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const results = await bios.multiExecute(statement);
      expect(results.length).toBe(1);
      expect(results[0].definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'last(state)', type: 'STRING' },
      ]);
      expect(results[0].data[0].records.length).toBe(2);
      expect(results[0].data[0].records[0][0]).toBe('japan');
      expect(results[0].data[0].records[0][1]).toBe('東京');
      expect(results[0].data[0].records[1][0]).toBe('us');
      expect(results[0].data[0].records[1][1]).toBe('or');
    });

    it('Select with group (distinctcount)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select('country', 'distinctcount(state)')
        .from('simpleRollup')
        .groupBy('country')
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const results = await bios.multiExecute(statement);
      expect(results.length).toBe(1);
      expect(results[0].definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'distinctcount(state)', type: 'INTEGER' },
      ]);
      expect(results[0].data[0].records.length).toBe(2);
      expect(results[0].data[0].records[0][0]).toBe('japan');
      expect(results[0].data[0].records[0][1]).toBe(1);
      expect(results[0].data[0].records[1][0]).toBe('us');
      expect(results[0].data[0].records[1][1]).toBe(2);
    });

    it('Select with group (metrics with aliases)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set. ' +
          ' Run the test from it-execute.sh');
        return;
      }
      const statement = bios.iSqlStatement()
        .select(
          'country',
          { metric: 'last(state)', as: 'lastState' },
          { metric: 'sum(value)', as: 'total' },
        )
        .from('simpleRollup')
        .groupBy('country')
        .timeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.seconds(1))
        .build();

      const result = await bios.execute(statement);
      expect(result.definitions).toEqual([
        { name: 'country', type: 'STRING' },
        { name: 'lastState', type: 'STRING' },
        { name: 'total', type: 'INTEGER' },
      ]);
      expect(result.dataWindows[0].records.length).toBe(2);
      expect(result.dataWindows[0].records[0][0]).toBe('japan');
      expect(result.dataWindows[0].records[0][1]).toBe('東京');
      expect(result.dataWindows[0].records[0][2]).toBe(7);
      expect(result.dataWindows[0].records[1][0]).toBe('us');
      expect(result.dataWindows[0].records[1][1]).toBe('or');
      expect(result.dataWindows[0].records[1][2]).toBe(7);
    });
  });

  describe('Tumbling window', () => {
    it('Fundamental', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('country', 'count()')
        .from('simpleRollup')
        .groupBy('country')
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(2))
        .build();

      const result = await bios.execute(statement);

      const testInfo = JSON.stringify({
        statement,
        result,
      }, null, 2);

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(2, testInfo);
      expect(result.definitions[0].name).toBe('country');
      expect(result.definitions[0].type).toBe('STRING');
      expect(result.definitions[1].name).toBe('count()');
      expect(result.definitions[1].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(2);
      const dataWindow0 = result.data[0];
      const expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_SIMPLE_ROLLUP / 60000) * 60000;
      expect(dataWindow0.windowBeginTime).toBe(expectedFirstTimestamp);
      expect(dataWindow0.records.length).toBe(2);
      expect(dataWindow0.records[0][0]).toBe('japan');
      expect(dataWindow0.records[0][1]).toBe(1);
      expect(dataWindow0.records[1][0]).toBe('us');
      expect(dataWindow0.records[1][1]).toBe(2);

      const dataWindow1 = result.data[1];
      expect(dataWindow1.windowBeginTime).toBe(expectedFirstTimestamp + 60000);
      expect(dataWindow1.records.length).toBe(0);
    });

    it('with filter', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('country', 'count()')
        .from('simpleRollup')
        .groupBy('country')
        .where("country = 'us'")
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(2))
        .build();

      const result = await bios.execute(statement);

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(2);
      expect(result.definitions[0].name).toBe('country');
      expect(result.definitions[0].type).toBe('STRING');
      expect(result.definitions[1].name).toBe('count()');
      expect(result.definitions[1].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(2);
      const dataWindow0 = result.data[0];
      const expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_SIMPLE_ROLLUP / 60000) * 60000;
      expect(dataWindow0.windowBeginTime).toBe(expectedFirstTimestamp);
      expect(dataWindow0.records.length).toBe(1);
      expect(dataWindow0.records[0][0]).toBe('us');
      expect(dataWindow0.records[0][1]).toBe(2);

      const dataWindow1 = result.data[1];
      expect(dataWindow1.windowBeginTime).toBe(expectedFirstTimestamp + 60000);
      expect(dataWindow1.records.length).toBe(0);
    });

    it('with filter #2', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('country', 'count()')
        .from('simpleRollup')
        .groupBy('country')
        .where("state = 'or'")
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(2))
        .build();

      const results = await bios.multiExecute(statement);

      // verification
      expect(results.length).toBe(1);
      const result = results[0];

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(2);
      expect(result.definitions[0].name).toBe('country');
      expect(result.definitions[0].type).toBe('STRING');
      expect(result.definitions[1].name).toBe('count()');
      expect(result.definitions[1].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(2);
      const dataWindow0 = result.data[0];
      const expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_SIMPLE_ROLLUP / 60000) * 60000;
      expect(dataWindow0.windowBeginTime).toBe(expectedFirstTimestamp);
      expect(dataWindow0.records.length).toBe(1);
      expect(dataWindow0.records[0][0]).toBe('us');
      expect(dataWindow0.records[0][1]).toBe(1);

      const dataWindow1 = result.data[1];
      expect(dataWindow1.windowBeginTime).toBe(expectedFirstTimestamp + 60000);
      expect(dataWindow1.records.length).toBe(0);
    });

    it('with filter #3', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('country', 'state', 'count()')
        .from('simpleRollup')
        .groupBy('country', 'state')
        .where("country = 'us' AND state > 'ca'")
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(2))
        .build();

      const results = await bios.multiExecute(statement);

      // verification
      expect(results.length).toBe(1);
      const result = results[0];

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(3);
      expect(result.definitions[0].name).toBe('country');
      expect(result.definitions[0].type).toBe('STRING');
      expect(result.definitions[1].name).toBe('state');
      expect(result.definitions[1].type).toBe('STRING');
      expect(result.definitions[2].name).toBe('count()');
      expect(result.definitions[2].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(2)
      const dataWindow0 = result.data[0];
      const expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_SIMPLE_ROLLUP / 60000) * 60000;
      expect(dataWindow0.windowBeginTime).toBe(expectedFirstTimestamp);
      expect(dataWindow0.records.length).toBe(1);
      expect(dataWindow0.records[0][0]).toBe('us');
      expect(dataWindow0.records[0][1]).toBe('or');
      expect(dataWindow0.records[0][2]).toBe(1);

      const dataWindow1 = result.data[1];
      expect(dataWindow1.windowBeginTime).toBe(expectedFirstTimestamp + 60000);
      expect(dataWindow1.records.length).toBe(0);
    });

    it('tumbling window with a metric with "as" ', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('country', { metric: 'count()', as: 'count of something' })
        .from('simpleRollup')
        .groupBy('country')
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(2))
        .build();

      const results = await bios.multiExecute(statement);

      // verification
      expect(results.length).toBe(1);
      const result = results[0];

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(2);
      expect(result.definitions[0].name).toBe('country');
      expect(result.definitions[0].type).toBe('STRING');
      expect(result.definitions[1].name).toBe('count of something');
      expect(result.definitions[1].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(2);
      const dataWindow0 = result.data[0];
      const expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_SIMPLE_ROLLUP / 60000) * 60000;
      expect(dataWindow0.windowBeginTime).toBe(expectedFirstTimestamp);
      expect(dataWindow0.records.length).toBe(2);
      expect(dataWindow0.records[0][0]).toBe('japan');
      expect(dataWindow0.records[0][1]).toBe(1);
      expect(dataWindow0.records[1][0]).toBe('us');
      expect(dataWindow0.records[1][1]).toBe(2);
    });

    it('longer time range', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('country', 'count()')
        .from('simpleRollup')
        .groupBy('country')
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(5))
        .build();

      const results = await bios.multiExecute(statement);

      // verification
      expect(results.length).toBe(1);
      const result = results[0];

      // ('RESULT:', JSON.stringify(result, null, 2));

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(2);
      expect(result.definitions[0].name).toBe('country');
      expect(result.definitions[0].type).toBe('STRING');
      expect(result.definitions[1].name).toBe('count()');
      expect(result.definitions[1].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(5);
    });

    it('Count on signal without any numeric attributes', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_MINIMUM is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      let statement = bios.iSqlStatement()
        .select('count()')
        .from('minimum')
        .tumblingWindow(bios.time.seconds(15))
        .snappedTimeRange(INGEST_TIMESTAMP_MINIMUM, bios.time.minutes(1))
        .build();

      let result = await bios.execute(statement);

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(1);
      expect(result.definitions[0].name).toBe('count()');
      expect(result.definitions[0].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(4);
      let expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_MINIMUM / 60000) * 60000;
      expect(result.data[0].windowBeginTime).toBe(expectedFirstTimestamp);
      expect(result.data[0].records.length).toBe(1);
      expect(result.data[0].records[0][0]).toBe(2);
      expect(result.data[1].windowBeginTime).toBe(expectedFirstTimestamp + 15000);
      expect(result.data[1].records.length).toBe(1);
      expect(result.data[1].records[0][0]).toBe(1);
      expect(result.data[2].windowBeginTime).toBe(expectedFirstTimestamp + 30000);
      expect(result.data[2].records.length).toBe(1);
      expect(result.data[2].records[0][0]).toBe(0);

      statement = bios.iSqlStatement()
        .select('count()')
        .from('minimum')
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_MINIMUM, bios.time.minutes(1))
        .build();

      result = await bios.execute(statement);

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(1);
      expect(result.definitions[0].name).toBe('count()');
      expect(result.definitions[0].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(1);
      expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_MINIMUM / 60000) * 60000;
      expect(result.data[0].windowBeginTime).toBe(expectedFirstTimestamp);
      expect(result.data[0].records.length).toBe(1);
      expect(result.data[0].records[0][0]).toBe(3);

      statement = bios.iSqlStatement()
        .select('count()')
        .from('minimum')
        .tumblingWindow(bios.time.minutes(5))
        .snappedTimeRange(INGEST_TIMESTAMP_MINIMUM, bios.time.minutes(5))
        .build();

      result = await bios.execute(statement);

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(1);
      expect(result.definitions[0].name).toBe('count()');
      expect(result.definitions[0].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(1);
      expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_MINIMUM / 300000) * 300000;
      expect(result.data[0].windowBeginTime).toBe(expectedFirstTimestamp);
      expect(result.data[0].records.length).toBe(1);
      expect(result.data[0].records[0][0]).toBe(3);
    });

  });

  describe('Get insights', () => {
    it('Basic', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const originTime = INGEST_TIMESTAMP_SIMPLE_ROLLUP + 60000;
      const getInsightsRequests = [
        {
          insightId: 'd9063f0f-5f03-4979-82b7-e07796b0ad2d',
          metric: 'simpleRollup.count()',
          originTime,
          delta: bios.time.minutes(-1),
          snapStepSize: bios.time.minutes(1),
        },
        {
          insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
          metric: 'simpleRollup.count()',
          originTime,
          delta: bios.time.minutes(-2),
          snapStepSize: bios.time.minutes(1),
        },
        { // expects no data
          insightId: '0039d69e-970b-4798-bd6b-b29fab7e8207',
          metric: 'simpleRollup.count()',
          originTime: INGEST_TIMESTAMP_SIMPLE_ROLLUP,
          delta: -bios.time.minutes(1),
          snapStepSize: bios.time.minutes(1),
        },
      ];
      const insights = await bios.getInsights(getInsightsRequests);
      const expectedTimestamp = (Math.floor(INGEST_TIMESTAMP_SIMPLE_ROLLUP / 60000) + 1) * 60000;
      const testInfo = JSON.stringify({
        getInsightsRequests,
        insights,
        expectedTimestamp,
      }, null, 2);
      expect(insights.length).toBe(3, testInfo);
      expect(insights[0].insightId).toBe('d9063f0f-5f03-4979-82b7-e07796b0ad2d', testInfo);
      expect(insights[0].value).toBe(3, testInfo);
      expect(insights[0].timestamp).toBe(expectedTimestamp, testInfo);
      expect(insights[0].timestamp).toBeLessThan(originTime);
      expect(insights[0].timestamp).toBeGreaterThanOrEqual(originTime - bios.time.minutes(1));
      expect(insights[1].insightId).toBe('c8949cff-2b1b-4e59-b119-9fbec1aa28b7');
      expect(insights[1].value).toBe(3);
      expect(insights[1].timestamp).toBe(expectedTimestamp);
      expect(insights[1].timestamp).toBeLessThan(originTime);
      expect(insights[1].timestamp).toBeGreaterThanOrEqual(originTime - bios.time.minutes(1));
      expect(insights[2].insightId).toBe('0039d69e-970b-4798-bd6b-b29fab7e8207');
      expect((insights[2].value === null) || (insights[2].value === 0)).toBe(true);
    });
  });

  describe('Sliding window', () => {
    it('Fundamental', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('country', 'count()')
        .from('simpleRollup')
        .groupBy('country')
        .slidingWindow(bios.time.minutes(1), 2)
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(2))
        .build();

      const results = await bios.multiExecute(statement);

      // verification
      expect(results.length).toBe(1);
      const result = results[0];

      const testInfo = JSON.stringify({
        statement,
        result,
      }, null, 2);

      // Check the definitions. See $ROOT/sdk-js/setup.py for the signal configuration
      expect(result.definitions.length).toBe(2, testInfo);
      expect(result.definitions[0].name).toBe('country');
      expect(result.definitions[0].type).toBe('STRING');
      expect(result.definitions[1].name).toBe('count()');
      expect(result.definitions[1].type).toBe('INTEGER');

      // Check data
      expect(result.data.length).toBe(2);
      const dataWindow0 = result.data[0];
      const expectedFirstTimestamp = Math.floor(INGEST_TIMESTAMP_SIMPLE_ROLLUP / 60000) * 60000;
      expect(dataWindow0.windowBeginTime).toBe(expectedFirstTimestamp);
      expect(dataWindow0.records.length).toBe(2);
      expect(dataWindow0.records[0][0]).toBe('japan');
      expect(dataWindow0.records[0][1]).toBe(1);
      expect(dataWindow0.records[1][0]).toBe('us');
      expect(dataWindow0.records[1][1]).toBe(2);

      const dataWindow1 = result.data[1];
      expect(dataWindow1.windowBeginTime).toBe(expectedFirstTimestamp + 60000);
      expect(dataWindow1.records.length).toBe(2);
      expect(dataWindow1.records[0][0]).toBe('japan');
      expect(dataWindow1.records[0][1]).toBe(1);
      expect(dataWindow1.records[1][0]).toBe('us');
      expect(dataWindow1.records[1][1]).toBe(2);
    });
  });

  describe('Negative Cases', () => {
    it('Signal does not exist', async () => {
      const statement3 = bios.iSqlStatement()
        .select("min(value)")
        .from("noSuchSignal")
        .orderBy({ key: 'min(value)', order: 'desc' })
        .limit(10)
        .tumblingWindow(300000)
        .snappedTimeRange(bios.time.now(), -43200000)
        .build();

      try {
        await bios.multiExecute(/*statement1,*/ statement3);
        fail('Exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NO_SUCH_STREAM.errorCode);
      }
    });

    it('Invalid grouping dimension', async () => {
      const statement3 = bios.iSqlStatement()
        .select("min(value)")
        .from("simpleRollup")
        .groupBy("simpleRollup") // This is incorrect
        .orderBy({ key: 'min(value)', order: 'desc' })
        .limit(10)
        .tumblingWindow(300000)
        .snappedTimeRange(bios.time.now(), -43200000)
        .build();

      try {
        await bios.multiExecute(/*statement1,*/ statement3);
        fail('Exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
        expect(e.message)
          .toBe('Bad input; Query #0: GroupBy[0]: Dimension must be one defined in a feature: simpleRollup; tenant=jsSimpleTest');
      }
    });

    it('Last function with tumbling window (unsupported yet)', async () => {
      if (!INGEST_TIMESTAMP_SIMPLE_ROLLUP) {
        console.warn(
          'Test "global window" > env var INGEST_TIMESTAMP_SIMPLE_ROLLUP is not set.' +
          ' Run the test from it-execute.sh');
        return;
      }

      const statement = bios.iSqlStatement()
        .select('count()', 'min(value)', 'max(value)', 'sum(value)', 'last(value)')
        .from('simpleRollup')
        .tumblingWindow(bios.time.minutes(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.minutes(2))
        .build();

      try {
        await bios.multiExecute(statement);
        fail('Exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.BAD_INPUT.errorCode);
      }
    });

    it('No metrics', async () => {
      const statement = bios.iSqlStatement()
        .select()
        .from('simpleRollup')
        .tumblingWindow(bios.time.days(1))
        .snappedTimeRange(INGEST_TIMESTAMP_SIMPLE_ROLLUP, bios.time.days(1))
        .build();

      try {
        await bios.multiExecute(statement);
        fail('Exception must happen');
      } catch (e) {
        expect(e instanceof BiosClientError).toBe(true);
        expect(e.errorCode).toBe(BiosErrorType.NOT_IMPLEMENTED.errorCode);
      }
    });

  });
});
