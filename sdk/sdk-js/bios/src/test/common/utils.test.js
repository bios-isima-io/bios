/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */

import { roleToName } from '../../main/common/utils';

describe('Utils test', () => {
  describe('Role to name conversion', () => {
    test('System Admin', () => {
      expect(roleToName('SystemAdmin')).toBe('System Administrator');
    });

    test('Tenant Admin', () => {
      expect(roleToName('TenantAdmin')).toBe('Administrator');
    });

    test('Machine Learning Engineer', () => {
      expect(roleToName('SchemaExtractIngest')).toBe('ML Engineer');
    });

    test('Data Scientist', () => {
      expect(roleToName('Extract')).toBe('Data Scientist');
    });

    test('Data Engineer', () => {
      expect(roleToName('Ingest')).toBe('Data Engineer');
    });

    test('Business User', () => {
      expect(roleToName('Report')).toBe('Business User');
    });

    test('Unknown', () => {
      expect(roleToName('NoSuchRole')).toBe('');
    });
  });
});
