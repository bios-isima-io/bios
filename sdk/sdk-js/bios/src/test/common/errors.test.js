/* eslint no-console: ["warn", { allow: ["log", "warn", "error"] }] */

import {handleErrorResponse, BiosClientError, BiosErrorType} from '../../main/common/errors';

describe('error handler test', () => {

  describe('auth errors', () => {
    test('login failure for wrong password', () => {
      const response = {
        status: 401,
        headers: {
          'content-type': 'application/json',
        },
        data : {
          errorCode: 'AUTH02',
          message: 'we ignore this message',
        },
        // additionalMessage: 'test@example.com',
      };
      const error = handleErrorResponse({response});
      expect(error instanceof BiosClientError).toBe(true);
      expect(error.errorCode).toBe('INVALID_PASSWORD');
      expect(error.message).toBe('Login failed, email and password did not match');
    });

    test('login failure for unknown reason', () => {
      const response = {
        status: 401,
        headers: {
          'content-type': 'plain-text',
        },
        data: 'Unauthorized',
      };
      const error = handleErrorResponse({response});
      expect(error instanceof BiosClientError).toBe(true);
      expect(error.errorCode).toBe('UNAUTHORIZED');
      expect(error.message).toBe('Not authorized');
    });
  });

  describe('no such entity', () => {
    test('no such tenant', () => {
      const response = {
        status: 404,
        headers: {
          'content-type': 'application/json',
        },
        data : {
          errorCode: 'ADMIN03',
          status: "NOT_FOUND",
          message: 'Tenant not found: jsSimpleTest',
        },
      };
      const error = handleErrorResponse({response});
      expect(error instanceof BiosClientError).toBe(true);
      expect(error.errorCode).toBe('NO_SUCH_TENANT');
      expect(error.errorNumber).toBe(BiosErrorType.NO_SUCH_TENANT.errorNumber);
      expect(error.additionalMessage).toBe('jsSimpleTest');
    });
  });

  describe('unknown HTTP error code', () => {
    test('unknown 4xx', () => {
      const response = {
        status: 410,
        headers: {
          'content-type': 'plain-text'
        },
        data: '410'
      };
      const error = handleErrorResponse({response});
      expect(error instanceof BiosClientError).toBe(true);
      expect(error.errorCode).toBe('GENERIC_CLIENT_ERROR');
      expect(error.message).toBe('An unexpected problem happened on the client side');
    });

    test('unknown 5xx', () => {
      const response = {
        response: 555,
        headers: {
          'content-type': 'plain-text'
        },
        data: '555'
      };
      const error = handleErrorResponse({response});
      expect(error instanceof BiosClientError).toBe(true);
      expect(error.errorCode).toBe('GENERIC_SERVER_ERROR');
      expect(error.message).toBe('An unexpected problem happened on the server side');
    });
  });

});
