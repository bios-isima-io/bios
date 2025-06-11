import bios from '../..';
import { InvalidArgumentError, NotImplementedError } from '../../main/common/errors';
import { ContentRepresentation, StatementType } from '../../main/isql';

describe('Context statements test', () => {
  describe('Context select', () => {
    test('Single entry', () => {
      const statement = bios.iSqlStatement()
        .select().fromContext('myContext').where({ key: 'abc' })
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [['abc']],
      });
    });

    test('Using helper', () => {
      const statement = bios.iSqlStatement()
        .select().fromContext('myContext').where(bios.isql.key('abc'))
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [['abc']],
      });
    });

    test('Multiple key', () => {
      const statement = bios.iSqlStatement()
        .select()
        .fromContext('myContext')
        .where(
          bios.isql.key(1),
          bios.isql.key(2),
          bios.isql.key(3))
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [[1], [2], [3]],
      });
    });

    test('Multi-dimensional keys 1', () => {
      const statement = bios.iSqlStatement()
        .select().fromContext('myContext').where({ key: ['abc', 1] }, { key: ['def', 2] })
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [['abc', 1], ['def', 2]],
      });
    });

    test('Multi-dimensional keys 2', () => {
      const statement = bios.iSqlStatement()
        .select()
        .fromContext('myContext')
        .where(
          bios.isql.key(1.2, true),
          bios.isql.key(2.3, false),
          bios.isql.key(3.4, true))
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [[1.2, true], [2.3, false], [3.4, true]],
      });
    });

    test('Negative: Specify none to where clause', () => {
      expect(
        () => bios.iSqlStatement()
          .select().fromContext('myContext')
          .where()
      ).toThrowError(InvalidArgumentError);
    });

    test('Group by 1', () => {
      const statement = bios.iSqlStatement()
        .select("count()")
        .fromContext('myContext')
        .groupBy("city")
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT_EX);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        metrics: [ {function: "COUNT"} ],
        groupBy: ["city"],
      });
    });

    test('Group by 2', () => {
      const statement = bios.iSqlStatement()
        .select("count()")
        .fromContext('myContext')
        .where("city = 'Menlo Park'")
        .groupBy("city")
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT_EX);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        metrics: [ {function: "COUNT"} ],
        where: "city = 'Menlo Park'",
        groupBy: ["city"],
      });
    });

    test('Order by', () => {
      const statement = bios.iSqlStatement()
        .select("count()", "city", "sum(numProducts)")
        .fromContext('myContext')
        .where("city = 'Menlo Park'")
        .groupBy("city")
        .orderBy({ key: 'city', order: 'desc' })
        .build();
      expect(statement.type).toBe(StatementType.SELECT_CONTEXT_EX);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        attributes: ["city"],
        metrics: [ {function: "COUNT"}, {function: "SUM", of: "numProducts"} ],
        where: "city = 'Menlo Park'",
        groupBy: ["city"],
        orderBy: {by: "city", reverse: true, caseSensitive: false},
      });
    });
  });

  describe('Context upsert', () => {
    test('Single CSV', () => {
      const statement = bios.iSqlStatement()
        .upsert()
        .into('testContext')
        .csv('hello,world')
        .build();
      expect(statement.type).toBe(StatementType.UPSERT);
      expect(statement.getTarget()).toBe('testContext');
      const requestMessage = statement.getRequestMessage();
      expect(requestMessage).toEqual({
        contentRepresentation: ContentRepresentation.CSV,
        entries: ['hello,world']
      });
    });

    test('Bulk CSV', () => {
      const statement = bios.iSqlStatement()
        .upsert()
        .into('testContext')
        .csvBulk([
          'Oregon,Portland',
          'California,Los Angeles',
          'Arizona,Phoenix',
          'Utah,Salt Lake City'])
        .build();
      expect(statement.getTarget()).toBe('testContext');
      const requestMessage = statement.getRequestMessage();
      expect(requestMessage).toEqual({
        contentRepresentation: ContentRepresentation.CSV,
        entries: [
          'Oregon,Portland',
          'California,Los Angeles',
          'Arizona,Phoenix',
          'Utah,Salt Lake City',
        ]
      });
    });

    test('Negative: Missing context name', () => {
      expect(() => {
        bios.iSqlStatement().upsert().into()
      }).toThrowError(InvalidArgumentError);
    });

    test('Negative: Blank context name', () => {
      expect(() => {
        bios.iSqlStatement().upsert().into(' ')
      }).toThrowError(InvalidArgumentError);
    });

    test('Negative: Missing CSV', () => {
      expect(() => {
        bios.iSqlStatement().upsert().into('testContext').csv()
      }).toThrowError(InvalidArgumentError);
    });

    test('Negative: Integer to CSV', () => {
      expect(() => {
        bios.iSqlStatement().upsert().into('testContext').csv(123)
      }).toThrowError(InvalidArgumentError);
    });

    test('Negative: Missing bulk CSV', () => {
      expect(() => {
        bios.iSqlStatement().upsert().into('testContext').csvBulk()
      }).toThrowError(InvalidArgumentError);
    });

    test('Negative: Empty bulk CSV', () => {
      expect(() => {
        bios.iSqlStatement().upsert().into('testContext').csvBulk([])
      }).toThrowError(InvalidArgumentError);
    });

    test('Negative: Integer into bulk CSVs', () => {
      expect(() => {
        bios.iSqlStatement().upsert().into('testContext').csvBulk([123])
      }).toThrowError(InvalidArgumentError);
    });
  });

  describe('Context update', () => {
    test('Bare minimum', () => {
      const statement = bios.iSqlStatement()
        .update('geoIp')
        .set({ name: 'zipCode', value: 94061 })
        .where(bios.isql.key('10.20.30.40'))
        .build();
      expect(statement.type).toBe(StatementType.UPDATE);
      expect(statement.getTarget()).toBe('geoIp');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKey: ['10.20.30.40'],
        attributes: [{ name: 'zipCode', value: 94061 }],
      });
    });

    test('Direct key', () => {
      const statement = bios.iSqlStatement()
        .update('geoIp')
        .set({ name: 'zipCode', value: 94061 })
        .where({ key: '10.20.30.40' })
        .build();
      expect(statement.type).toBe(StatementType.UPDATE);
      expect(statement.getTarget()).toBe('geoIp');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKey: ['10.20.30.40'],
        attributes: [{ name: 'zipCode', value: 94061 }],
      });
    });

    test('Direct multi-dimensional key', () => {
      const statement = bios.iSqlStatement()
        .update('geoIp')
        .set({ name: 'zipCode', value: 94061 })
        .where({ key: [12, true] })
        .build();
      expect(statement.type).toBe(StatementType.UPDATE);
      expect(statement.getTarget()).toBe('geoIp');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKey: [12, true],
        attributes: [{ name: 'zipCode', value: 94061 }],
      });
    });

    test('Using helper method', () => {
      const statement = bios.iSqlStatement()
        .update('geoIp')
        .set(
          bios.isql.attribute('zipCode', 94061),
          bios.isql.attribute('city', 'Redwood City'))
        .where(bios.isql.key('10.20.30.40'))
        .build();
      expect(statement.type).toBe(StatementType.UPDATE);
      expect(statement.getTarget()).toBe('geoIp');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKey: ['10.20.30.40'],
        attributes: [
          { name: 'zipCode', value: 94061 },
          { name: 'city', value: 'Redwood City' },
        ],
      });
    });
  });

  describe('Context delete', () => {
    test('Single entry', () => {
      const statement = bios.iSqlStatement()
        .delete().fromContext('myContext').where({ key: 'abc' })
        .build();
      expect(statement.type).toBe(StatementType.DELETE);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [['abc']],
      });
    });

    test('Using helper', () => {
      const statement = bios.iSqlStatement()
        .delete().fromContext('myContext').where(bios.isql.key('abc'))
        .build();
      expect(statement.type).toBe(StatementType.DELETE);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [['abc']],
      });
    });

    test('Multiple key', () => {
      const statement = bios.iSqlStatement()
        .delete()
        .fromContext('myContext')
        .where(
          bios.isql.key(1),
          bios.isql.key(2),
          bios.isql.key(3))
        .build();
      expect(statement.type).toBe(StatementType.DELETE);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [[1], [2], [3]],
      });
    });

    test('Multi-dimensional key', () => {
      const statement = bios.iSqlStatement()
        .delete()
        .fromContext('myContext')
        .where(
          bios.isql.key(1.2, true),
          bios.isql.key(2.3, false),
          bios.isql.key(3.4, true))
        .build();
      expect(statement.type).toBe(StatementType.DELETE);
      expect(statement.getTarget()).toBe('myContext');
      expect(statement.getRequestMessage()).toEqual({
        contentRepresentation: ContentRepresentation.UNTYPED,
        primaryKeys: [[1.2, true], [2.3, false], [3.4, true]],
      });
    });

    test('Negative: Specify none to where clause', () => {
      expect(
        () => bios.iSqlStatement()
          .delete().fromContext('myContext')
          .where()
      ).toThrowError(InvalidArgumentError);
    });
  });
});
