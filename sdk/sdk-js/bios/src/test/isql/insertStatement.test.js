/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

import bios from '../../index';

import { ContentRepresentation } from '../../main/codec/proto';
import { InvalidArgumentError } from '../../main/common/errors';
import { StatementType } from '../../main/isql/models';
import { InsertContext } from '../../main/isql/insert';
import { ISqlRequest } from '../../main/isql/isqlRequest';

describe('Insert Statement Test', () => {
  describe('Normal cases via csv', () => {
    test('single event', () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into('mySignal')
        .csv('hello,world')
        .build();
      expect(statement).toBeDefined();
      expect(statement.type).toBe(StatementType.INSERT);
      expect(statement.length).toBe(1);
      const data = statement._data;
      expect(data).toBeInstanceOf(InsertContext);
      expect(data.signal).toBe('mySignal');
      expect(data.contentRep).toBe(ContentRepresentation.CSV);
      expect(data.csvs.length).toBe(1);
      expect(data.csvs[0]).toBe('hello,world');
      expect(data.eventIds.length).toBe(1);
      expect(data.eventIds[0].length).toBe(16);
    });

    test('multi events', () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into('mySignal')
        .csvBulk('event,one', 'event,two')
        .build();
      expect(statement).toBeDefined();
      expect(statement.type).toBe(StatementType.INSERT);
      expect(statement.length).toBe(2);
      const data = statement._data;
      expect(data).toBeInstanceOf(InsertContext);
      expect(data.signal).toBe('mySignal');
      expect(data.contentRep).toBe(ContentRepresentation.CSV);
      expect(data.csvs.length).toBe(2);
      expect(data.csvs[0]).toBe('event,one');
      expect(data.csvs[1]).toBe('event,two');
      expect(data.eventIds.length).toBe(2);
      expect(data.eventIds[0].length).toBe(16);
      expect(data.eventIds[1].length).toBe(16);
    });

    test('multi events #2', () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into('mySignal')
        .csv('event,one')
        .csv('event,two')
        .build();
      expect(statement).toBeDefined();
      expect(statement.type).toBe(StatementType.INSERT);
      expect(statement.length).toBe(2);
      const data = statement._data;
      expect(data).toBeInstanceOf(InsertContext);
      expect(data.signal).toBe('mySignal');
      expect(data.contentRep).toBe(ContentRepresentation.CSV);
      expect(data.csvs.length).toBe(2);
      expect(data.csvs[0]).toBe('event,one');
      expect(data.csvs[1]).toBe('event,two');
    });
  });

  describe('Normal cases via values', () => {
    test('Simple', async () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into('ababab')
        .values(['one', 1, true, 3.14])
        .build();
      expect(statement).toBeDefined();
      expect(statement).toBeInstanceOf(ISqlRequest);
      const data = statement.data;
      expect(data).toBeInstanceOf(InsertContext);
      expect(data.signal).toBe('ababab');
      expect(data.contentRep).toBe(ContentRepresentation.CSV);
      expect(data.csvs.length).toBe(1);
      expect(data.csvs[0]).toBe('one,1,true,3.14');
    });

    test('Bulk', async () => {
      const statement = bios.iSqlStatement()
        .insert()
        .into('ababab')
        .valuesBulk(['one', 1, true, 3.14], ['two', 2, false, 2.72])
        .build();
      expect(statement).toBeDefined();
      expect(statement).toBeInstanceOf(ISqlRequest);
      const data = statement.data;
      expect(data).toBeInstanceOf(InsertContext);
      expect(data.signal).toBe('ababab');
      expect(data.contentRep).toBe(ContentRepresentation.CSV);
      expect(data.csvs.length).toBe(2);
      expect(data.csvs[0]).toBe('one,1,true,3.14');
      expect(data.csvs[1]).toBe('two,2,false,2.72');
    });
  });

  describe('Negative cases', () => {
    test('Missing signal', () => {
      expect(() => {
        bios.iSqlStatement().insert().into()
      }).toThrowError(InvalidArgumentError);
    });

    test('Empty signal', () => {
      expect(() => {
        bios.iSqlStatement().insert().into('')
      }).toThrowError(InvalidArgumentError);
    });

    test('Signal as an integer', () => {
      expect(() => {
        bios.iSqlStatement().insert().into(500)
      }).toThrowError(InvalidArgumentError);
    });

    test('Empty csv data', () => {
      expect(() => {
        bios.iSqlStatement()
          .insert()
          .into('mySignal')
          .csv()
          .build();
      }).toThrowError(InvalidArgumentError);
    })

    test('Invalid csv type', () => {
      expect(() => {
        bios.iSqlStatement()
          .insert()
          .into('mySignal')
          .csv(123)
          .build();
      }).toThrowError(InvalidArgumentError);
    })

    test('Multiple entries in csv', () => {
      expect(() => {
        bios.iSqlStatement()
          .insert()
          .into('mySignal')
          .csv('event,one', 'event,two')
          .build();
      }).toThrowError(InvalidArgumentError);
    })
  });
})