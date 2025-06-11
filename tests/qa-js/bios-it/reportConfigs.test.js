'use strict';

import axios from 'axios';

import bios, { BiosClientError, BiosErrorType } from 'bios-sdk';

import { BIOS_ENDPOINT } from './tutils';

describe('Report Config', () => {
  describe('one report', () => {

    const vizConfig = {
      description: 'Extra configuration',
      validation: 'Should not be rejected',
      persistency: 'Should be restorable',
    };

    beforeAll(async () => {
      await bios.global.login({
        endpoint: BIOS_ENDPOINT,
        email: 'admin+report@jsSimpleTest',
        password: 'admin',
      });
      const response = await bios.putReportConfig({
        reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        reportName: 'Revenue in USD / 米国ドルでの売上高',
        metrics: [
          {
            measurement: 'orders.sum(price)',
            as: 'Revenue/売上高'
          },
          {
            measurement: 'customers.min(age)',
            as: 'Minimum Age'
          },
          bios.metric('customers', 'max(stay_length)', 'Maximum Age')
        ],
        dimensions: ['country', 'state'],
        defaultTimeRange: 604800000,
        defaultWindowLength: 43200000,
        vizConfig,
      });
      expect(response).not.toBeDefined();
    });

    afterAll(async () => {
      await bios.deleteReportConfig('c8949cff-2b1b-4e59-b119-9fbec1aa28b7');
      await bios.logout();
    });

    it('basic', async () => {
      const response = await bios.getReportConfigs();
      expect(response).toBeDefined();
      expect(response).not.toBeNull();
      expect(response.reportConfigs).toBeDefined();
      expect(response.reportConfigs.length).toBe(1);
      expect(Object.keys(response.reportConfigs[0]).length).toBe(2);
      expect(response.reportConfigs[0].reportId).toBe('c8949cff-2b1b-4e59-b119-9fbec1aa28b7');
      expect(response.reportConfigs[0].reportName).toBe('Revenue in USD / 米国ドルでの売上高');
    });

    it('detail', async () => {
      const response = await bios.getReportConfigs({
        detail: true
      });
      expect(response).toBeDefined();
      expect(response).not.toBeNull();
      expect(response.reportConfigs).toBeDefined();
      expect(response.reportConfigs.length).toBe(1);
      expect(Object.keys(response.reportConfigs[0]).length).not.toBeLessThanOrEqual(2);
      expect(response.reportConfigs[0].reportId).toBe('c8949cff-2b1b-4e59-b119-9fbec1aa28b7');
      expect(response.reportConfigs[0].reportName).toBe('Revenue in USD / 米国ドルでの売上高');
      expect(response.reportConfigs[0].defaultTimeRange).toBe(604800000);
      expect(response.reportConfigs[0].vizConfig).toEqual(vizConfig);
    });
  });

  describe('two reports', () => {

    beforeAll(async () => {
      await bios.login({
        endpoint: BIOS_ENDPOINT,
        email: 'admin+report@jsSimpleTest',
        password: 'admin',
      });
      await bios.putReportConfig({
        reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        reportName: 'Revenue in USD / 米国ドルでの売上高',
        metrics: [
          {
            measurement: 'orders.sum(price)',
            as: 'Revenue/売上高'
          },
          {
            measurement: 'customers.min(age)',
            as: 'Minimum Age'
          },
          bios.metric('customers', 'max(stay_length)', 'Maximum Age')
        ],
        dimensions: ['country', 'state'],
        defaultTimeRange: 604800000,
        defaultWindowLength: 43200000,
      });
      await bios.putReportConfig({
        reportId: '89891289-c12f-4f56-902f-b914339a81c6',
        reportName: 'Click Count',
        metrics: [
          {
            measurement: 'clicks.count()',
            as: 'clicks'
          },
        ],
        dimensions: ['age', 'gender'],
        defaultTimeRange: 604800000,
        defaultWindowLength: 43200000,
      });
    });

    afterAll(async () => {
      await bios.deleteReportConfig('c8949cff-2b1b-4e59-b119-9fbec1aa28b7');
      await bios.deleteReportConfig('89891289-c12f-4f56-902f-b914339a81c6');
      await bios.logout();
    });

    it('get all', async () => {
      const response = await bios.getReportConfigs({
        detail: true
      });
      // console.log('REPORTS:', JSON.stringify(response3, null, 2));
      expect(response.reportConfigs.length).toBe(2);
      const reports = {};
      response.reportConfigs.forEach((report) => {
        reports[report.reportId] = report;
      });
      const rep31 = reports['c8949cff-2b1b-4e59-b119-9fbec1aa28b7'];
      expect(rep31).toBeDefined();
      expect(rep31.reportName).toBe('Revenue in USD / 米国ドルでの売上高');
      expect(rep31.dimensions).toEqual(['country', 'state']);

      const rep32 = reports['89891289-c12f-4f56-902f-b914339a81c6'];
      expect(rep32).toBeDefined();
      expect(rep32.reportName).toBe('Click Count');
      expect(rep32.metrics[0]).toEqual({ measurement: 'clicks.count()', as: 'clicks' });
    });

    it('get one', async () => {
      const response = await bios.getReportConfigs({
        reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
        detail: true
      });
      // console.log('REPORTS:', JSON.stringify(response3, null, 2));
      expect(response.reportConfigs.length).toBe(1);

      const rep31 = response.reportConfigs[0];
      expect(rep31).toBeDefined();
      expect(rep31.reportId).toBe('c8949cff-2b1b-4e59-b119-9fbec1aa28b7');
      expect(rep31.reportName).toBe('Revenue in USD / 米国ドルでの売上高');
      expect(rep31.dimensions).toEqual(['country', 'state']);
    });

    it('get two', async () => {
      const response = await bios.getReportConfigs({
        reportId: ['c8949cff-2b1b-4e59-b119-9fbec1aa28b7', '89891289-c12f-4f56-902f-b914339a81c6'],
        detail: true
      });
      expect(response.reportConfigs.length).toBe(2);
      const reports = {};
      response.reportConfigs.forEach((report) => {
        reports[report.reportId] = report;
      });
      const rep31 = reports['c8949cff-2b1b-4e59-b119-9fbec1aa28b7'];
      expect(rep31).toBeDefined();
      expect(rep31.reportName).toBe('Revenue in USD / 米国ドルでの売上高');

      const rep32 = reports['89891289-c12f-4f56-902f-b914339a81c6'];
      expect(rep32).toBeDefined();
      expect(rep32.reportName).toBe('Click Count');
    });
  });
});
