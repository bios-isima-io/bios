'use strict';

import {OrderBy} from '../../../main/codec/proto';
import {createOrderByPayload} from '../../../main/codec/messages';

describe('OrdeBy message codec', () => {

  test('specified by string', () => {
    const input = 'keyA';
    const expected = {
      by: 'keyA',
      caseSensitive: false,
      reverse: false,
    };
    runTest(input, expected);
  });

  test('specified by object with key', () => {
    const input = {
      key: 'keyB',
    };
    const expected = {
      by: 'keyB',
      caseSensitive: false,
      reverse: false,
    };
    runTest(input, expected);
  });

  test('specified by object with key and order ASC', () => {
    const input = {
      key: 'keyC',
      order: 'ASC',
    };
    const expected = {
      by: 'keyC',
      caseSensitive: false,
      reverse: false,
    };
    runTest(input, expected);
  });

  test('specified by object with key and order DESC', () => {
    const input = {
      key: 'keyD',
      order: 'DESC',
    };
    const expected = {
      by: 'keyD',
      caseSensitive: false,
      reverse: true,
    };
    runTest(input, expected);
  });

  test('specified by object with key and order DESC, case sensitive', () => {
    const input = {
      key: 'keyE',
      order: 'DESC',
      caseSensitive: true,
    };
    const expected = {
      by: 'keyE',
      caseSensitive: true,
      reverse: true,
    };
    runTest(input, expected);
  });
});

const runTest = (input, expected) => {
  const orderBy = createOrderByPayload(input);
  expect(OrderBy.verify(orderBy)).toBe(null);
  expect(orderBy).toEqual(expected);

  const message = OrderBy.create(orderBy);
  // run encode and decode
  const buffer = OrderBy.encode(message).finish();
  const decoded = OrderBy.decode(buffer);
  const rebuilt = OrderBy.toObject(decoded);

  expect(rebuilt).toEqual(expected);
};
