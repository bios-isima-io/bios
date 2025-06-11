/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

import bios from '../../index';

import { InvalidArgumentError } from '../../main/common/errors';

describe('dummy', () => {
  test('noop', () => {
  });
});

describe('metric generator', () => {
  test('count', () => {
    expect(bios.metric('signal', 'count()', 'signal count'))
      .toEqual({ measurement: 'signal.count()', as: 'signal count' });
  });

  test('sum', () => {
    expect(bios.metric('signal', 'sum(price)', 'total price'))
      .toEqual({ measurement: 'signal.sum(price)', as: 'total price' });
  });

  test('min', () => {
    expect(bios.metric('signal', 'min(stayLength)'))
      .toEqual({ measurement: 'signal.min(stayLength)' });
  });

  test('max', () => {
    expect(bios.metric('signal', 'max(stayLength)', 'maximum stay length'))
      .toEqual({ measurement: 'signal.max(stayLength)', as: 'maximum stay length' });
  });

  test('unknown function is acceptable at this level', () => {
    expect(bios.metric('signal', 'custom(myParam)'))
      .toEqual({ measurement: 'signal.custom(myParam)' });
  });

  test('no alias', () => {
    const metric = bios.metric('order', 'sum(price)');
    expect(Object.keys(metric).length).toBe(1);
    expect(metric.measurement).toBe('order.sum(price)');
  });

  test('metric description is missing', () => {
    expect(() => bios.metric('operations')).toThrowError(InvalidArgumentError);
  });

  test('invalid metric syntax #1', () => {
    expect(() => bios.metric('operations', 'count')).toThrowError(InvalidArgumentError);
  });

  test('invalid metric syntax #2', () => {
    expect(() => bios.metric('operations', 'sum(')).toThrowError(InvalidArgumentError);
  });

  test('invalid metric syntax #3', () => {
    // bios.metric('operations', 'count()()');
    expect(() => bios.metric('operations', 'count()()')).toThrowError(InvalidArgumentError);
  });

  test('invalid count function', () => {
    expect(() => bios.metric('operations', 'count(some)')).toThrowError(InvalidArgumentError);
  });

  test('sum function does not have argument', () => {
    expect(() => bios.metric('operations', 'sum()')).toThrowError(InvalidArgumentError);
  });

  test('min function does not have argument', () => {
    expect(() => bios.metric('operations', 'min()')).toThrowError(InvalidArgumentError);
  });

  test('max function does not have argument', () => {
    expect(() => bios.metric('operations', 'max()')).toThrowError(InvalidArgumentError);
  });
});
