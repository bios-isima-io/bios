/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

import bios from '../../index';

import {
  MetricFunction,
  SelectQuery,
  SelectRequest,
  WindowType,
} from '../../main/codec/proto';
import tsToString from './tsToString';

describe('Statement builder', () => {
  describe('Global window', () => {
    test('Fundamental', async () => {
      const now = bios.time.now();
      const statement = bios.iSqlStatement()
        .select('ipAddress', 'operation')
        .from('accesses')
        .timeRange(now, bios.time.hours(-3))
        .build();

      expect(SelectQuery.verify(statement.query)).toBe(null);

      const since = tsToString(now - 10800000);
      const until = tsToString(now);
      const stringified =
        `SELECT ipAddress, operation FROM accesses SINCE ${since} UNTIL ${until}`;
      expect(statement.toString()).toBe(stringified);
    });

    test('Query with filter', async () => {
      const now = bios.time.now();
      const statement = bios.iSqlStatement()
        .select('ipAddress', 'operation')
        .from('accesses')
        .where("country = 'US'")
        .timeRange(now, bios.time.hours(-3))
        .build();

      expect(SelectQuery.verify(statement.query)).toBe(null);

      const since = tsToString(now - 10800000);
      const until = tsToString(now);
      const stringified = 'SELECT ipAddress, operation FROM accesses'
        + ` WHERE country = 'US' SINCE ${since} UNTIL ${until}`;
      expect(statement.toString()).toBe(stringified);
    });
  });

  describe('Tumbling window', () => {
    test('Fundamental', async () => {
      const now = bios.time.now();
      const statement = bios.iSqlStatement()
        .select(
          'count()', 'sum(score)',
          { metric: 'max(traffic)' },
          { metric: 'min(time)', as: 'minimumTime' },
          bios.isql.metric('last(status)').as('lastStatus')
        )
        .from('mysignal')
        .groupBy('age', 'gender')
        .orderBy({ key: 'count()', order: 'desc' })
        // .orderBy(bios.desc('count()'), bios.asc('age'))
        .limit(10)
        .where("gender = 'FEMALE'")
        .tumblingWindow(bios.time.hours(1))
        .snappedTimeRange(now, bios.time.hours(-3))
        .build();

      const start = now - 3 * 3600 * 1000;
      const windowSize = 3600 * 1000;
      const snappedStart = Math.floor(start / windowSize) * windowSize;
      const snappedEnd = snappedStart + 3 * 3600 * 1000;

      const expectedPayload = {
        queries: [
          {
            startTime: snappedStart,
            endTime: snappedEnd,
            attributes: {},
            metrics: [
              {
                function: MetricFunction.COUNT,
              }, {
                function: MetricFunction.SUM,
                of: 'score',
              }, {
                function: MetricFunction.MAX,
                of: 'traffic',
              }, {
                function: MetricFunction.MIN,
                of: 'time',
                as: 'minimumTime',
              }, {
                function: MetricFunction.LAST,
                of: 'status',
                as: 'lastStatus',
              }
            ],
            from: 'mysignal',
            groupBy: {
              dimensions: ['age', 'gender'],
            },
            where: "gender = 'FEMALE'",
            windows: [
              {
                windowType: WindowType.TUMBLING_WINDOW,
                tumbling: {
                  windowSizeMs: 3600 * 1000
                },
              }
            ],
            orderBy: {
              by: 'count()',
              reverse: true,
              caseSensitive: false,
            },
            limit: 10,
          }
        ],
      };

      expect(SelectQuery.verify(statement.query)).toBe(null);

      const payload = {
        queries: [statement.query]
      };
      expect(SelectRequest.verify(payload)).toBe(null);

      const message = SelectQuery.create(payload);
      const buffer = SelectRequest.encode(message).finish();

      const decoded = SelectRequest.decode(buffer);
      const rebuilt = SelectRequest.toObject(decoded, { longs: Number });

      // console.log('decoded:', JSON.stringify(rebuilt, null, 2));
      expect(rebuilt).toEqual(expectedPayload);

      const since = tsToString(snappedStart);
      const until = tsToString(snappedEnd);
      const stringified = 'SELECT count(), sum(score), max(traffic), min(time) AS minimumTime,'
        + " last(status) AS lastStatus FROM mysignal WHERE gender = 'FEMALE'"
        + ' GROUP BY age, gender ORDER BY count() DESC LIMIT 10'
        + ` WINDOW 3600000 SINCE ${since} UNTIL ${until}`;
      expect(statement.toString()).toBe(stringified);
    });

    test('Specify orderBy using the helper method', async () => {
      const now = bios.time.now();
      const statement = bios.iSqlStatement()
        .select(
          'count()', 'sum(score)',
          bios.isql.metric('min(time)').as('minimumTime')
        )
        .from('mysignal')
        .groupBy('age', 'gender')
        .orderBy(bios.isql.sort('age').desc().ignoreCase())
        .tumblingWindow(bios.time.hours(1))
        .snappedTimeRange(now, bios.time.hours(-3))
        .build();

      const start = now - 3 * 3600 * 1000;
      const windowSize = 3600 * 1000;
      const snappedStart = Math.floor(start / windowSize) * windowSize;
      const snappedEnd = snappedStart + 3 * 3600 * 1000;

      const expectedPayload = {
        queries: [
          {
            startTime: snappedStart,
            endTime: snappedEnd,
            attributes: {},
            metrics: [
              {
                function: MetricFunction.COUNT,
              }, {
                function: MetricFunction.SUM,
                of: 'score',
              }, {
                function: MetricFunction.MIN,
                of: 'time',
                as: 'minimumTime',
              }
            ],
            from: 'mysignal',
            groupBy: {
              dimensions: ['age', 'gender'],
            },
            windows: [
              {
                windowType: WindowType.TUMBLING_WINDOW,
                tumbling: {
                  windowSizeMs: 3600 * 1000
                },
              }
            ],
            orderBy: {
              by: 'age',
              reverse: true,
              caseSensitive: true,
            },
          }
        ],
      };

      expect(SelectQuery.verify(statement.query)).toBe(null);

      const payload = {
        queries: [statement.query]
      };
      expect(SelectRequest.verify(payload)).toBe(null);

      const message = SelectQuery.create(payload);
      const buffer = SelectRequest.encode(message).finish();

      const decoded = SelectRequest.decode(buffer);
      const rebuilt = SelectRequest.toObject(decoded, { longs: Number });

      // console.log('decoded:', JSON.stringify(rebuilt, null, 2));
      expect(rebuilt).toEqual(expectedPayload);

      const since = tsToString(snappedStart);
      const until = tsToString(snappedEnd);
      const stringified = 'SELECT count(), sum(score), min(time) AS minimumTime'
        + ' FROM mysignal GROUP BY age, gender ORDER BY age DESC'
        + ` WINDOW 3600000 SINCE ${since} UNTIL ${until}`;
      expect(statement.toString()).toBe(stringified);
    });
  });

  describe('Sliding window', () => {
    test('Fundamental', async () => {
      const now = bios.time.now();
      const statement = bios.iSqlStatement()
        .select(
          'count()', 'sum(score)',
          { metric: 'max(traffic)' },
          { metric: 'min(time)', as: 'minimumTime' }
        )
        .from('mysignal')
        .groupBy('age', 'gender')
        .orderBy({ key: 'count()', order: 'desc' })
        .limit(10)
        .where("gender = 'FEMALE'")
        .slidingWindow(bios.time.minutes(10), 3)
        .snappedTimeRange(now, bios.time.hours(-3))
        .build();

      const start = now - 3 * 3600 * 1000;
      const windowSize = 600 * 1000;
      const snappedStart = Math.floor(start / windowSize) * windowSize;
      const snappedEnd = snappedStart + 3 * 3600 * 1000;

      const expectedPayload = {
        queries: [
          {
            startTime: snappedStart,
            endTime: snappedEnd,
            attributes: {},
            metrics: [
              {
                function: MetricFunction.COUNT,
              }, {
                function: MetricFunction.SUM,
                of: 'score',
              }, {
                function: MetricFunction.MAX,
                of: 'traffic',
              }, {
                function: MetricFunction.MIN,
                of: 'time',
                as: 'minimumTime',
              }
            ],
            from: 'mysignal',
            groupBy: {
              dimensions: ['age', 'gender'],
            },
            where: "gender = 'FEMALE'",
            windows: [
              {
                windowType: WindowType.SLIDING_WINDOW,
                sliding: {
                  slideInterval: windowSize,
                  windowSlides: 3,
                },
              }
            ],
            orderBy: {
              by: 'count()',
              reverse: true,
              caseSensitive: false,
            },
            limit: 10,
          }
        ],
      };

      expect(SelectQuery.verify(statement.query)).toBe(null);

      const payload = {
        queries: [statement.query]
      };
      expect(SelectRequest.verify(payload)).toBe(null);

      const message = SelectQuery.create(payload);
      const buffer = SelectRequest.encode(message).finish();

      const decoded = SelectRequest.decode(buffer);
      const rebuilt = SelectRequest.toObject(decoded, { longs: Number });

      // console.log('decoded:', JSON.stringify(rebuilt, null, 2));
      expect(rebuilt).toEqual(expectedPayload);

      const since = tsToString(snappedStart);
      const until = tsToString(snappedEnd);
      const stringified = 'SELECT count(), sum(score), max(traffic), min(time) AS minimumTime'
        + " FROM mysignal WHERE gender = 'FEMALE'"
        + " GROUP BY age, gender ORDER BY count() DESC LIMIT 10"
        + ` WINDOW 1800000 SLIDING 600000 SINCE ${since} UNTIL ${until}`;
      expect(statement.toString()).toBe(stringified);
    });
  });
});
