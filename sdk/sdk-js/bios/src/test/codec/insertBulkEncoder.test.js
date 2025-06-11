/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

import bios from '../../../';

import InsertBulkRequestEncoder from '../../main/codec/insertBulkRequest';

describe('insertBulkRequestEncoder test', () => {
  test('simple', async () => {
    const statement = bios.iSqlStatement()
      .insert()
      .into('abcde')
      .csvBulk('one,1', 'two,2', 'three,3', 'four,4', 'five,5')
      .build();

    const buffer = InsertBulkRequestEncoder.encode(statement, 0, 5);
    expect(buffer).toBeInstanceOf(Uint8Array);
  });
});