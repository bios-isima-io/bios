'use strict';

import {Metric, MetricFunction} from '../../../main/codec/proto';
import {
  createMetricPayload,
} from '../../../main/codec/messages';

describe('count message codec', () => {

  test('count, default', () => {
    const metric = createMetricPayload(MetricFunction.COUNT);
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.COUNT);
    expect(metric.of).toBe(undefined);
    expect(metric.as).toBe(undefined);

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.COUNT);
    expect(rebuilt.of).toBe(undefined);
    expect(rebuilt.as).toBe(undefined);
  });

  test('count, as', () => {
    const metric = createMetricPayload(MetricFunction.COUNT, null, 'countOfSomething');
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.COUNT);
    expect(metric.of).toBe(undefined);
    expect(metric.as).toBe('countOfSomething');

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.COUNT);
    expect(rebuilt.of).toBe(undefined);
    expect(rebuilt.as).toBe('countOfSomething');
  });
});

describe('sum message codec', () => {

  test('sum, default', () => {
    const metric = createMetricPayload(MetricFunction.SUM, 'score');
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.SUM);
    expect(metric.of).toBe('score');
    expect(metric.as).toBe(undefined);

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.SUM);
    expect(rebuilt.of).toBe('score');
    expect(rebuilt.as).toBe(undefined);
  });

  test('sum, as', () => {
    const metric = createMetricPayload(MetricFunction.SUM, 'score', 'sumOfScore');
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.SUM);
    expect(metric.of).toBe('score');
    expect(metric.as).toBe('sumOfScore');

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.SUM);
    expect(rebuilt.of).toBe('score');
    expect(rebuilt.as).toBe('sumOfScore');
  });
});

describe('max message codec', () => {

  test('max, default', () => {
    const metric = createMetricPayload(MetricFunction.MAX, 'score');
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.MAX);
    expect(metric.of).toBe('score');
    expect(metric.as).toBe(undefined);

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.MAX);
    expect(rebuilt.of).toBe('score');
    expect(rebuilt.as).toBe(undefined);
  });

  test('max, as', () => {
    const metric = createMetricPayload(MetricFunction.MAX, 'score', 'maxOfScore');
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.MAX);
    expect(metric.of).toBe('score');
    expect(metric.as).toBe('maxOfScore');

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.MAX);
    expect(rebuilt.of).toBe('score');
    expect(rebuilt.as).toBe('maxOfScore');
  });
});

describe('min message codec', () => {

  test('min, default', () => {
    const metric = createMetricPayload(MetricFunction.MIN, 'score');
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.MIN);
    expect(metric.of).toBe('score');
    expect(metric.as).toBe(undefined);

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.MIN);
    expect(rebuilt.of).toBe('score');
    expect(rebuilt.as).toBe(undefined);
  });

  test('min, as', () => {
    const metric = createMetricPayload(MetricFunction.MIN, 'score', 'minOfScore');
    expect(Metric.verify(metric)).toBe(null);
    expect(metric.function).toBe(MetricFunction.MIN);
    expect(metric.of).toBe('score');
    expect(metric.as).toBe('minOfScore');

    const message = Metric.create(metric);

    // run encode and decode
    const buffer = Metric.encode(message).finish();
    const decoded = Metric.decode(buffer);
    const rebuilt = Metric.toObject(decoded);

    expect(rebuilt.function).toBe(MetricFunction.MIN);
    expect(rebuilt.of).toBe('score');
    expect(rebuilt.as).toBe('minOfScore');
  });
});
