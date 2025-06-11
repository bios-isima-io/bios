/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

import bios from '../../index';

import {
  buildGetInsightsStatements,
  validateGetInsightsRequest,
} from '../../main/sdk/biosHttp.js';
import {
  BiosClientError,
  InvalidArgumentError,
} from '../../main/common/errors.js';

describe('getInsights method unit test', () => {
  describe('getInsights request validator test', () => {
    const now = bios.time.now();
    test('valid', () => {
      const request = [
        {
          insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
          metric: 'orders.sum(price)',
          originTime: now,
          delta: -bios.time.days(7),
          snapStepSize: bios.time.hours(6),
        },
        {
          insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
          metric: 'orders.sum(price)',
          originTime: now,
          delta: -bios.time.days(30),
          snapStepSize: bios.time.hours(12),
        },
        {
          insightId: '3ca42b1f-7619-4de2-a200-41c8e433381f',
          metric: 'clicks.count()',
          originTime: now,
          delta: -bios.time.days(7),
          snapStepSize: bios.time.hours(6),
        },
      ];
      const validated = validateGetInsightsRequest(request);
      expect(validated[0]).toEqual({
        insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        metric: bios.metric('orders', 'sum(price)'),
        originTime: now,
        delta: -7 * 24 * 3600 * 1000,
        snapStepSize: 6 * 3600 * 1000,
      });
      expect(validated[1]).toEqual({
        insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        metric: bios.metric('orders', 'sum(price)'),
        originTime: now,
        delta: -30 * 24 * 3600 * 1000,
        snapStepSize: 12 * 3600 * 1000,
      });
      expect(validated[2]).toEqual({
        insightId: '3ca42b1f-7619-4de2-a200-41c8e433381f',
        metric: bios.metric('clicks', 'count()'),
        originTime: now,
        delta: -7 * 24 * 3600 * 1000,
        snapStepSize: 6 * 3600 * 1000,
      });
    });

    test('for internal signal', () => {
      const request = [
        {
          insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
          metric: '_usage.sum(elapsedTime)',
          originTime: 1123456,
          delta: bios.time.days(7),
          snapStepSize: bios.time.hours(1),
        },
        {
          insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
          metric: '_usage.count()',
          originTime: 1123456,
          delta: bios.time.days(30),
          snapStepSize: bios.time.hours(1),
        },
      ];
      const validated = validateGetInsightsRequest(request);
      expect(validated[0]).toEqual({
        insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        metric: { measurement: '_usage.sum(elapsedTime)' },
        originTime: 1123456,
        delta: 7 * 24 * 3600 * 1000,
        snapStepSize: 3600 * 1000,
      });
      expect(validated[1]).toEqual({
        insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        metric: bios.metric('_usage', 'count()'),
        originTime: 1123456,
        delta: 30 * 24 * 3600 * 1000,
        snapStepSize: 3600 * 1000,
      });
    });

    test('request is undefined', () => {
      const request = undefined;
      expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
    });

    test('request is not array', () => {
      const request = 'somehow mistaken';
      expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
    });

    describe('invalid insightId', () => {
      test('insightsId missing', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
          {
            insightsId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(30),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('invalid insightsId type', () => {
        const request = [
          {
            insightId: 333,
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });
    });

    describe('metric variations', () => {
      test('as string', () => {
        const now = bios.time.now();
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: now,
            delta: -bios.time.days(7),
            snapStepSize: bios.time.minutes(10),
          },
          {
            insightId: '2b9e4bcf-dfcf-4d03-955a-6d14ad85e33f',
            metric: {
              measurement: 'clicks.count()',
              as: 'Click count', // not necessary but OK to have it
            },
            originTime: now,
            delta: -bios.time.days(15),
            snapStepSize: bios.time.hours(12),
          },
        ];
        const validated = validateGetInsightsRequest(request);
        expect(validated.length).toBe(2);
        expect(validated).toEqual([
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: {
              measurement: 'orders.sum(price)',
            },
            originTime: now,
            delta: -bios.time.days(7),
            snapStepSize: bios.time.minutes(10),
          }, {
            insightId: '2b9e4bcf-dfcf-4d03-955a-6d14ad85e33f',
            metric: {
              measurement: 'clicks.count()',
              as: 'Click count',
            },
            originTime: now,
            delta: -bios.time.days(15),
            snapStepSize: bios.time.hours(12),
          }
        ]);
      });

      test('as object', () => {
        const request = [
        ];
        const validated = validateGetInsightsRequest(request);
      });
    });

    describe('invalid metric', () => {
      test('metric missing', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metrics: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('invalid metric type', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 99.23,
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('as object but required property "measurement" is missing', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: {
              measurementx: 'clicks.count()',
              as: 'Click count', // not necessary but OK to have it
            },
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });
    });

    describe('Invalid time spec', () => {
      test('Origin missing', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            // originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('Delta missing', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            // delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('Snap step size missing', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            // snapStepSize: bios.time.hours(6),
          },
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(6),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('invalid delta type', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: '12345',
            snapStepSize: bios.time.hours(1),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('invalid origin type', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: '12345',
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(1),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('invalid origin value', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: -bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: bios.time.hours(1),
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('invalid snap step size type', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: '91',
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });

      test('invalid snap step size value', () => {
        const request = [
          {
            insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
            metric: 'orders.sum(price)',
            originTime: bios.time.now(),
            delta: -bios.time.days(7),
            snapStepSize: 0,
          },
        ];
        expect(() => validateGetInsightsRequest(request)).toThrowError(InvalidArgumentError);
      });
    });
  });

  describe('getInsights statements generator test', () => {
    test('basic', () => {
      const requests = validateGetInsightsRequest([
        {
          insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
          metric: 'orders.sum(price)',
          originTime: bios.time.now(),
          delta: bios.time.days(-7),
          snapStepSize: bios.time.hours(3),
        },
        {
          insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
          metric: 'orders.sum(price)',
          originTime: bios.time.now(),
          delta: bios.time.days(-30),
          snapStepSize: bios.time.hours(6),
        },
        {
          insightId: '3ca42b1f-7619-4de2-a200-41c8e433381f',
          metric: 'clicks.count()',
          originTime: bios.time.now() - bios.time.days(1),
          delta: bios.time.days(3),
          snapStepSize: bios.time.hours(1),
        },
      ]);
      const statements = buildGetInsightsStatements(requests);
      expect(statements.length).toBe(3);
      statements.map((statement) => {
        expect(statement.type).toBe('SELECT');
      });
    });
  });
});
