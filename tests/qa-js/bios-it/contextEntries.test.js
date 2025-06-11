/* eslint no-console: ["warn", { allow: ["debug", "log", "warn", "error"] }] */

'use strict';

import bios, { BiosClientError, BiosErrorType, VoidISqlResponse } from 'bios-sdk';

import { BIOS_ENDPOINT, } from './tutils';

async function getContextEntries(contextName, primaryKeys) {
  const statement = bios.iSqlStatement().select().fromContext(contextName)
    .where(...primaryKeys.map((pkey) => bios.isql.key(...pkey)))
    .build();
  return await bios.execute(statement);
}

describe('Context entries operations', () => {
  const key = bios.isql.key;

  beforeAll(async () => {
    bios.initialize({
      endpoint: BIOS_ENDPOINT
    });
    await bios.signIn({
      email: 'admin@jsSimpleTest',
      password: 'admin', // we have to support the other users
    });
  });

  afterAll(async () => {
    await bios.signOut();
  });

  describe('GetContextEntries test', () => {
    beforeEach(async () => {
      await bios.execute(bios.iSqlStatement()
        .upsert().into('contextLeast').csvBulk([
          '172.0.0.1,localhost',
          '10.20.30.40,bios',
          '20.30.40.50,tfos',
        ]).build());
    });

    it('Fundamental get', async () => {
      const statement = bios.iSqlStatement()
        .select()
        .fromContext('contextLeast')
        .where(bios.isql.key('172.0.0.1'), bios.isql.key('20.30.40.50'))
        .build();
      const response = await bios.execute(statement);

      expect(response).toBeDefined();
      expect(response.contentRepresentation).toBe('UNTYPED');
      expect(response.primaryKey).toEqual(['ip']);
      expect(response.definitions).toEqual([
        { attributeName: 'ip', type: 'String' },
        { attributeName: 'site', type: 'String' },
      ]);
      expect(response.entries.length).toBe(2);
      expect(response.entries[0].attributes).toEqual(['172.0.0.1', 'localhost']);
      expect(response.entries[1].attributes).toEqual(['20.30.40.50', 'tfos']);
    });

    it('one key is missing', async () => {
      const statement = bios.iSqlStatement()
        .select().fromContext('contextLeast')
        .where(key('10.20.30.40'), key('5.4.3.2'), key('172.0.0.1'))
        .build();
      const response = await bios.execute(statement);

      expect(response).toBeDefined();
      expect(response.contentRepresentation).toBe('UNTYPED');
      expect(response.primaryKey).toEqual(['ip']);
      expect(response.definitions).toEqual([
        { attributeName: 'ip', type: 'String' },
        { attributeName: 'site', type: 'String' },
      ]);
      expect(response.entries.length).toBe(2);
      expect(response.entries[0].attributes).toEqual(['10.20.30.40', 'bios']);
      expect(response.entries[1].attributes).toEqual(['172.0.0.1', 'localhost']);
    });
  });

  describe('Upsert ContextEntries test', () => {
    afterEach(async () => {
      const resp = await bios.execute(bios.iSqlStatement().select().fromContext('contextLeast')
        .where(
          key('172.0.0.1'),
          key('10.20.30.40,'),
          key('20.30.40.50'))
        .build());
      await bios.execute(bios.iSqlStatement().delete().fromContext('contextLeast')
        .where(...resp.entries.map((entry) => key(entry.attributes[0])))
        .build());
    });

    it('Overwrite', async () => {
      const statement = bios.iSqlStatement()
        .upsert().into('contextLeast')
        .csvBulk([
          '172.0.0.1,localhost',
          '20.30.40.50,tfos',
        ]).build();
      const response = await bios.execute(statement);
      expect(response).toBeInstanceOf(VoidISqlResponse);

      const fetched = await bios.execute(bios.iSqlStatement().select().fromContext('contextLeast')
        .where(
          key('20.30.40.50'),
          key('172.0.0.1'))
        .build());
      expect(fetched.entries[0].attributes).toEqual(['20.30.40.50', 'tfos']);
      expect(fetched.entries[1].attributes).toEqual(['172.0.0.1', 'localhost']);
      await bios.execute(bios.iSqlStatement()
        .upsert().into('contextLeast').csvBulk([
          '20.30.40.50,bios',
          '10.20.30.40,abcde',
        ]).build());
      const fetched2 = await getContextEntries('contextLeast', [
        ['172.0.0.1'],
        ['10.20.30.40'],
        ['20.30.40.50'],
      ]);
      expect(fetched2.entries[0].attributes).toEqual(['172.0.0.1', 'localhost']);
      expect(fetched2.entries[1].attributes).toEqual(['10.20.30.40', 'abcde']);
      expect(fetched2.entries[2].attributes).toEqual(['20.30.40.50', 'bios']);
    });
  });

  describe('Delete ContextEntries test', () => {
    it('Basic deletion', async () => {
      await bios.execute(bios.iSqlStatement()
        .upsert().into('contextLeast').csvBulk([
          '213.43.50.50,site1',
          '213.43.50.51,site2',
          '213.43.50.52,site3',
        ]).build());
      const fetched = await getContextEntries('contextLeast', [
        ['213.43.50.50'],
        ['213.43.50.51'],
        ['213.43.50.52'],
      ]);
      expect(fetched.entries.length).toBe(3);

      // delete
      const statement = bios.iSqlStatement()
        .delete()
        .fromContext('contextLeast')
        .where(
          bios.isql.key('213.43.50.50'),
          bios.isql.key('213.43.50.51'),
          bios.isql.key('213.43.50.52'),
        ).build();
      const response = await bios.execute(statement);
      expect(response).toBeInstanceOf(VoidISqlResponse);

      const fetchedAgain = await getContextEntries('contextLeast', [
        ['213.43.50.50'],
        ['213.43.50.51'],
        ['213.43.50.52'],
      ]);
      expect(fetchedAgain.entries.length).toBe(0);
    });

    it('Delete twice', async () => {
      await bios.execute(
        bios.iSqlStatement()
          .upsert().into('contextLeast').csv('213.43.50.50,site1').build());
      const fetched = await getContextEntries('contextLeast', [
        ['213.43.50.50'],
      ]);
      expect(fetched.entries.length).toBe(1);

      const statement = bios.iSqlStatement().delete().fromContext('contextLeast')
        .where(key('213.43.50.50')).build();
      // first
      await bios.execute(statement);

      // second, double deletion is fine, it wouldn't fail
      await bios.execute(statement);
    });

    it('Non-existing primary keys should be ignored in deletion', async () => {
      await bios.execute(bios.iSqlStatement()
        .upsert().into('contextLeast').csvBulk([
          '213.43.50.50,site1',
          '213.43.50.51,site1',
          '213.43.50.52,site1',
          '213.43.50.53,site1',
          '213.43.50.54,site1',
        ]).build());
      const fetched = await getContextEntries('contextLeast', [
        ['213.43.50.50'],
        ['213.43.50.51'],
        ['213.43.50.52'],
        ['213.43.50.53'],
        ['213.43.50.54'],
      ]);
      expect(fetched.entries.length).toBe(5);

      await bios.execute(bios.iSqlStatement().delete().fromContext('contextLeast')
        .where(
          key('213.43.50.50'),
          key('213.43.50.58'),
          key('213.43.50.52'),
          key('213.43.50.59')).build());

      const fetched2 = await getContextEntries('contextLeast', [
        ['213.43.50.50'],
        ['213.43.50.51'],
        ['213.43.50.52'],
        ['213.43.50.53'],
        ['213.43.50.54'],
      ]);
      expect(fetched2.entries.length).toBe(3);
    });
  });

  describe('UpdateContextEntry test', () => {
    afterEach(async () => {
      const resp = await getContextEntries('contextAllTypes', [
        [33241],
      ]);
      await bios.execute(bios.iSqlStatement().delete().fromContext('contextAllTypes')
        .where(...resp.entries.map((entry) => key(entry.attributes[0]))).build());
    });

    it('Fundamental', async () => {
      await bios.execute(bios.iSqlStatement()
        .upsert().into('contextAllTypes').csv('33241,US,38.21,150.3,true,aGVsbG8=')
        .build());
      const fetched = await getContextEntries('contextAllTypes', [
        [33241],
      ]);

      expect(fetched.entries[0].attributes).toEqual([
        33241, 'US', 38.21, 150.3, true, 'aGVsbG8=',
      ]);

      const statement = bios.iSqlStatement()
        .update('contextAllTypes')
        .set(
          bios.isql.attribute('photo', 'd29ybGQ='),
          bios.isql.attribute('altitude', 40.83),
          bios.isql.attribute('flagged', false))
        .where(bios.isql.key(33241))
        .build();
      const response = await bios.execute(statement);
      expect(response).toBeInstanceOf(VoidISqlResponse);

      const updated = await getContextEntries('contextAllTypes', [
        [33241],
      ]);
      expect(updated.entries[0].attributes).toEqual([
        33241,
        'US',
        40.83,
        150.3,
        false,
        'd29ybGQ=',
      ]);
    });
  });

  describe('Large scale upserting and selecting', () => {
    const numContextEntries = 10000;
    const entries = []
    const keyValues = {}
    for (let i = 0; i < numContextEntries; ++i) {
      const key = `900.40.50.${i}`;
      const value = `${i}a`;
      const entry = `${key},${value}`;
      entries.push(entry);
      keyValues[key] = value;
    }
    const keys = Object.keys(keyValues).map((k) => bios.isql.key(k));

    afterEach(async () => {
      await bios.execute(bios.iSqlStatement()
        .delete()
        .fromContext('contextLeast')
        .where(...keys)
        .build());
    });

    it('Upsert and Select', async () => {
      const statement = bios.iSqlStatement()
        .upsert()
        .into('contextLeast')
        .csvBulk(entries).build();
      const response = await bios.execute(statement);
      expect(response).toBeInstanceOf(VoidISqlResponse);

      const fetched = await bios.execute(
        bios.iSqlStatement()
          .select().fromContext('contextLeast')
          .where(...keys)
          .build());

      expect(fetched.entries.length).toBe(numContextEntries);
      for (let i = 0; i < numContextEntries; ++i) {
        const attributes = fetched.entries[i].attributes;
        const key = attributes[0];
        const value = attributes[1];
        expect(value).toBe(keyValues[key]);
      }
    });
  });

  describe('Context feature and sketch selects', () => {
    it('Feature Select', async () => {
      let response = await bios.execute(
        bios.iSqlStatement()
          .select("count()")
          .fromContext('contextWithMultiDimensionalFeature')
          .build());
      expect(response.entries.length).toBe(1);

      response = await bios.execute(
        bios.iSqlStatement()
          .select("count()", "sum(numProducts)", "city")
          .fromContext('contextWithMultiDimensionalFeature')
          .groupBy("city")
          .orderBy("city")
          .build());
      expect(response.entries.length).toBe(3);
      expect(response.entries[0].city).toBe("Menlo Park");
      expect(response.entries[0]["count()"]).toBe(4);
      expect(response.entries[0]["sum(numProducts)"]).toBe(46);

      response = await bios.execute(
        bios.iSqlStatement()
          .select("count()", "sum(numProducts)", "city")
          .fromContext('contextWithMultiDimensionalFeature')
          .groupBy("city")
          .orderBy("city")
          .build());
      expect(response.entries.length).toBe(3);
      expect(response.entries[0].city).toBe("Menlo Park");
      expect(response.entries[0]["count()"]).toBe(4);
      expect(response.entries[0]["sum(numProducts)"]).toBe(46);

      response = await bios.execute(
        bios.iSqlStatement()
          .select("count()", "sum(numProducts)", "city")
          .fromContext('contextWithMultiDimensionalFeature')
          .where("storeType = 101")
          .groupBy("city")
          .orderBy("city")
          .build());
      expect(response.entries.length).toBe(2);
      expect(response.entries[0].city).toBe("Menlo Park");
      expect(response.entries[0]["count()"]).toBe(2);
      expect(response.entries[0]["sum(numProducts)"]).toBe(19);

      response = await bios.execute(
        bios.iSqlStatement()
          .select("count()", "sum(numProducts)", "city")
          .fromContext('contextWithMultiDimensionalFeature')
          .where("City IN ('Menlo Park', 'Palo Alto') AND storeType = 101")
          .groupBy("city")
          .orderBy("city")
          .build());
      expect(response.entries.length).toBe(1);
      expect(response.entries[0].city).toBe("Menlo Park");
      expect(response.entries[0]["count()"]).toBe(2);
      expect(response.entries[0]["sum(numProducts)"]).toBe(19);
    });

    it('Sketch Select', async () => {
      let response = await bios.execute(
        bios.iSqlStatement()
          .select("count()", "sum2(numProducts)", "distinctCount(city)", "median(quantity)")
          .fromContext('contextWithMultiDimensionalFeature')
          .build());

      expect(response.entries.length).toBe(1);
      expect(response.entries[0]["count()"]).toBe(7);
      expect(response.entries[0]["sum2(numProducts)"]).toBe(839);
      expect(Math.round(response.entries[0]["distinctcount(city)"])).toBe(3);
      expect(response.entries[0]["median(quantity)"]).toBe(1946);

      response = await bios.execute(
        bios.iSqlStatement()
          .select("sampleCounts(city)")
          .fromContext('contextWithMultiDimensionalFeature')
          .build());

      expect(response.entries.length).toBe(3);
      let totalCount = 0;
      for (let i = 0; i < 3; ++i) {
        totalCount += response.entries[i]["_sampleCount"];
      }
      expect(totalCount).toBe(7);
    });
  });
});
