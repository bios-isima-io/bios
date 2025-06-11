'use strict';

import bios, {
  InvalidArgumentError
} from 'bios-sdk';
import { BIOS_ENDPOINT } from './tutils';



describe('Insight Config', () => {

  const signalInsights = "signal";
  const contextInsights = "context";

  beforeAll(async () => {
    bios.initialize({ endpoint: BIOS_ENDPOINT });
    await bios.signIn({
      email: 'admin+report@jsSimpleTest',
      password: 'admin',
    });
  });

  afterAll(async () => {
    await bios.signOut();
  });

  afterEach(async () => {
    await Promise.all([
      bios.deleteInsightConfigs(signalInsights),
      bios.deleteInsightConfigs(contextInsights)
    ]);
  });

  describe('Normal cases', () => {
    it('one insight', async () => {
      const putResponse = await bios.putInsightConfigs(signalInsights, {
        sections: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfigs: [
              {
                insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
            ],
          },
        ],
      });
      expect(putResponse).not.toBeDefined();

      const response = await bios.getInsightConfigs(signalInsights);
      expect(response).toBeDefined();
      expect(response).not.toBeNull();
      expect(response.sections).toBeDefined();
      expect(response.sections.length).toBe(1);
      expect(response.sections[0].sectionId).toBe('74bddeac-cd3d-4d7d-8352-96b06b85b2be');
      expect(response.sections[0].timeRange).toBe(604800000);
      expect(response.sections[0].insightConfigs.length).toBe(1);
      expect(response.sections[0].insightConfigs[0].insightId)
        .toBe('2db598e5-3df1-4be6-b363-a7cd173399c1');
      expect(response.sections[0].insightConfigs[0].reportId)
        .toBe('c8949cff-2b1b-4e59-b119-9fbec1aa28b7');
    });

    it('multiple insights', async () => {
      const configOrig = {
        sections: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfigs: [
              {
                insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
              {
                insightId: 'd9063f0f-5f03-4979-82b7-e07796b0ad2d',
                reportId: 'a827df30-06b2-4ec2-aaac-691e7081f732',
              },
              {
                insightId: '01d1a59a-681c-4748-8f82-15e61421d939',
                reportId: '5a0daff2-198a-4585-9c9b-b730301a8603',
              },
            ],
          },
          {
            sectionId: '49552e37-4620-4831-ab5d-f981c24e0f15',
            timeRange: 2592000000,
            insightConfigs: [
              {
                insightId: 'b07a4e46-0ec1-46da-af91-fb1945a01f21',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
              {
                insightId: '06f38b23-f8f8-45c3-be6e-297eaeabf1b2',
                reportId: '47c3dc84-4185-45a9-88bf-b6430997bcbb',
              },
            ],
          },
        ],
      };
      await bios.putInsightConfigs(signalInsights, configOrig);

      const configRetrieved = await bios.getInsightConfigs(signalInsights);
      expect(configRetrieved).toEqual(configOrig);
    });

    it('zero insights', async () => {
      const response = await bios.getInsightConfigs(signalInsights);
      expect(response).toBeDefined();
      expect(response).not.toBeNull();
      expect(response.sections).toBeDefined();
      expect(response.sections.length).toBe(0);
    });

    it('multiple insights', async () => {
      const signalInsightConfigs = {
        sections: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfigs: [
              {
                insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
              {
                insightId: 'd9063f0f-5f03-4979-82b7-e07796b0ad2d',
                reportId: 'a827df30-06b2-4ec2-aaac-691e7081f732',
              },
              {
                insightId: '01d1a59a-681c-4748-8f82-15e61421d939',
                reportId: '5a0daff2-198a-4585-9c9b-b730301a8603',
              },
            ],
          },
          {
            sectionId: '49552e37-4620-4831-ab5d-f981c24e0f15',
            timeRange: 2592000000,
            insightConfigs: [
              {
                insightId: 'b07a4e46-0ec1-46da-af91-fb1945a01f21',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
              {
                insightId: '06f38b23-f8f8-45c3-be6e-297eaeabf1b2',
                reportId: '47c3dc84-4185-45a9-88bf-b6430997bcbb',
              },
            ],
          },
        ],
      };
      await bios.putInsightConfigs(signalInsights, signalInsightConfigs);

      const contextInsightConfigs = {
        sections: [
          {
            sectionId: 'd29d52a7-ed82-4fb0-9112-fdb896492996',
            insightConfigs: [
              {
                insightId: 'd59ec8a2-ce65-436a-b29f-30e9166bd6e0',
                reportId: '314203b3-93da-41df-a09f-59ed048233a8',
              },
              {
                insightId: 'ff523d0a-ec19-4c4e-8dd1-12fd84fdb7a5',
                reportId: '38e72908-5834-4646-9058-dffda666fc2c',
              },
            ],
          },
          {
            sectionId: '1cbc8250-4b1a-4649-b484-5de928d5cd5e',
            insightConfigs: [
              {
                insightId: 'bd714d1d-eafc-4095-b2e3-4d034489d5af',
                reportId: 'c5afc6e7-d76e-4bff-bf8c-bb644268dcec',
              },
            ],
          },
        ],
      };
      await bios.putInsightConfigs(contextInsights, contextInsightConfigs);

      const retrievedSignalConfigs = await bios.getInsightConfigs(signalInsights);
      expect(retrievedSignalConfigs).toEqual(signalInsightConfigs);

      const retrievedContextConfigs = await bios.getInsightConfigs(contextInsights);
      expect(retrievedContextConfigs).toEqual(contextInsightConfigs);

      await bios.deleteInsightConfigs(signalInsights);
      const deletedSignalConfigs = await bios.getInsightConfigs(signalInsights);
      expect(deletedSignalConfigs).toEqual({ sections: [] });

      // context insights shouldn't be affected
      const retrievedContextConfigs2 = await bios.getInsightConfigs(contextInsights);
      expect(retrievedContextConfigs2).toEqual(contextInsightConfigs);

    });
  });

  describe('Negative cases', () => {
    it('sections misspelled', async () => {
      const configs = {
        section: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfigs: [
              {
                insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
            ],
          },
        ],
      };
      await expectAsync(bios.putInsightConfigs(signalInsights, configs))
        .toBeRejectedWithError(InvalidArgumentError);
    });

    it('insightConfigs missing (misspelled)', async () => {
      const configs = {
        section: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfig: [
              {
                insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
            ],
          },
        ],
      };
      await expectAsync(bios.putInsightConfigs(signalInsights, configs))
        .toBeRejectedWithError(InvalidArgumentError);
    });

    it('insightId missing', async () => {
      const configs = {
        section: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfigs: [
              {
                // insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
            ],
          },
        ],
      };
      await expectAsync(bios.putInsightConfigs(signalInsights, configs))
        .toBeRejectedWithError(InvalidArgumentError);
    });

    it('reportId missing', async () => {
      const configs = {
        section: [
          {
            sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
            timeRange: 604800000,
            insightConfigs: [
              {
                insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
                // reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
              },
            ],
          },
        ],
      };
      await expectAsync(bios.putInsightConfigs(signalInsights, configs))
        .toBeRejectedWithError(InvalidArgumentError);
    });
  });
});
