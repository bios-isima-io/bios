'use strict';

export {
  handleErrorResponse,
  BiosClientError,
  BiosErrorType,
  InvalidArgumentError,
  NotImplementedError,
};

class BiosClientError extends Error {
  constructor(errorInfo, additionalMessage) {
    super(BiosClientError.buildMessage(errorInfo.message, additionalMessage));
    this.name = 'BiosClientError';
    this.errorCode = errorInfo.errorCode;
    this.errorNumber = errorInfo.errorNumber;
    this.additionalMessage = additionalMessage;
  }

  static buildMessage(message, additionalMessage) {
    return additionalMessage ? (message + '; ' + additionalMessage) : message;
  }
}

/**
 * List of operation statuses.
 */
const OP_STATUSES = [
  {
    errorCode: 'OK',
    errorNumber: 0,
    message: 'OK',
  },
  //////////////////////////////////////
  // 0x1nnnnn Error from client ////////
  {
    errorCode: 'GENERIC_CLIENT_ERROR',
    errorNumber: 0x100001,
    message: 'An unexpected problem happened on the client side',
  },
  {
    errorCode: 'INVALID_ARGUMENT',
    errorNumber: 0x100002,
    message: 'Invalid argument',
  },
  {
    errorCode: 'SESSION_INACTIVE',
    errorNumber: 0x100003,
    message: 'A request for an operation was issued to the client, but it is closed already',
  },
  {
    errorCode: 'PARSER_ERROR',
    errorNumber: 0x100004,
    message: 'Failed to convert a string to a TFOS request object',
  },
  {
    errorCode: 'CLIENT_ALREADY_STARTED',
    errorNumber: 0x100005,
    message: 'Tried to start already started client',
  },
  {
    errorCode: 'REQUEST_TOO_LARGE',
    errorNumber: 0x100006,
    message: 'Request entity too large',
  },
  {
    errorCode: 'INVALID_STATE',
    errorNumber: 0x100007,
    message: 'Client is in invalid state',
  },
  {
    errorCode: 'CLIENT_CHANNEL_ERROR',
    errorNumber: 0x100008,
    message: 'SDK failed to communicate with peer for an unexpected reason',
  },
  {
    errorCode: 'NOT_LOGGED_IN',
    errorNumber: 0x100009,
    message: 'Not logged in',
  },
  //////////////////////////
  // 0x2nnnnn Error from server

  // 0x20nnnn Service level errors
  {
    errorCode: 'SERVER_CONNECTION_FAILURE',
    errorNumber: 0x200001,
    message: 'Failed to connect to the server',
  },
  {
    errorCode: 'SERVER_CHANNEL_ERROR',
    errorNumber: 0x200002,
    message: 'Server communication error',
  },
  {
    errorCode: 'UNAUTHORIZED',
    errorNumber: 0x200003,
    message: 'Not authorized',
  },
  {
    errorCode: 'FORBIDDEN',
    errorNumber: 0x200004,
    message: 'Permission denied',
  },
  {
    errorCode: 'SERVICE_UNAVAILABLE',
    errorNumber: 0x200005,
    message: 'Service is currently unavailable',
  },
  {
    errorCode: 'GENERIC_SERVER_ERROR',
    errorNumber: 0x200006,
    message: 'An unexpected problem happened on the server side',
  },
  {
    errorCode: 'TIMEOUT',
    errorNumber: 0x200007,
    message: 'Operation has timed out',
  },
  {
    errorCode: 'BAD_GATEWAY',
    errorNumber: 0x200008,
    message: 'Load balancer proxy failed',
  },
  {
    errorCode: 'SERVICE_UNDEPLOYED',
    errorNumber: 0x200009,
    message: 'Requested resource was not found on the server',
  },
  {
    errorCode: 'OPERATION_CANCELLED',
    errorNumber: 0x20000a,
    message: 'Operation has been cancelled by peer',
  },
  {
    errorCode: 'OPERATION_UNEXECUTABLE',
    errorNumber: 0x20000b,
    message: 'Unable to execute the requested operation',
  },
  {
    errorCode: 'USERNAME_NOT_FOUND',
    errorNumber: 0x20000b,
    message: 'Login failed, invalid user name',
  },
  {
    errorCode: 'SESSION_EXPIRED',
    errorNumber: 0x20000c,
    message: 'Session has expired',
  },
  {
    errorCode: 'INVALID_PASSWORD',
    errorNumber: 0x20000d,
    message: 'Login failed, email and password did not match',
  },
  // 0x21nnnn Semantic error
  {
    errorCode: 'NO_SUCH_TENANT',
    errorNumber: 0x210001,
    message: 'No such tenant',
  },
  {
    errorCode: 'NO_SUCH_STREAM',
    errorNumber: 0x210002,
    message: 'No such stream',
  },
  {
    errorCode: 'NOT_FOUND',
    errorNumber: 0x210003,
    message: 'Specified resource is not found',
  },
  {
    errorCode: 'TENANT_ALREADY_EXISTS',
    errorNumber: 0x210004,
    message: 'Specified tenant already exists',
  },
  {
    errorCode: 'SIGNAL_ALREADY_EXISTS',
    errorNumber: 0x210005,
    message: 'Specified stream already exists',
  },
  {
    errorCode: 'RESOURCE_ALREADY_EXISTS',
    errorNumber: 0x210006,
    message: 'Specified resource already exists',
  },
  {
    errorCode: 'BAD_INPUT',
    errorNumber: 0x210007,
    message: 'Bad input',
  },
  {
    errorCode: 'NOT_IMPLEMENTED',
    errorNumber: 0x210008,
    message: 'Not implemented',
  },
  {
    errorCode: 'CONSTRAINT_WARNING',
    errorNumber: 0x210009,
    message: 'Constraint warning. Set force option to override',
  },
  {
    errorCode: 'SCHEMA_VERSION_CHANGED',
    errorNumber: 0x21000b,
    message: 'Specified stream version no longer exists',
  },
  {
    errorCode: 'INVALID_REQUEST',
    errorNumber: 0x21000c,
    message: 'Server rejected request due to invalid request',
  },
  {
    errorCode: 'BULK_INGEST_FAILED',
    errorNumber: 0x21000d,
    message: 'Bulk ingest failed',
  },
  {
    errorCode: 'SERVER_DATA_ERROR',
    errorNumber: 0x21000e,
    message: 'Server returned malformed data',
  },
  {
    errorCode: 'SCHEMA_MISMATCHED',
    errorNumber: 0x21000f,
    message: 'Schema mismatched',
  },
  {
    errorCode: 'USER_ID_NOT_VERIFIED',
    errorNumber: 0x210010,
    message: 'User ID is not verified yet',
  },
];

/**
 *  Operation status enum.
 */
const BiosErrorType = {};
OP_STATUSES.forEach((entry) => {
  BiosErrorType[entry.errorCode] = entry;
});

const HTTP_STATUS_CODES = {
  400: BiosErrorType.BAD_INPUT,
  401: BiosErrorType.UNAUTHORIZED,
  403: BiosErrorType.FORBIDDEN,
  404: BiosErrorType.NOT_FOUND,
  408: BiosErrorType.TIMEOUT,
  409: BiosErrorType.RESOURCE_ALREADY_EXISTS,
  413: BiosErrorType.REQUEST_TOO_LARGE,
  500: BiosErrorType.GENERIC_SERVER_ERROR,
  501: BiosErrorType.NOT_IMPLEMENTED,
  502: BiosErrorType.BAD_GATEWAY,
  503: BiosErrorType.SERVICE_UNAVAILABLE,
  504: BiosErrorType.TIMEOUT,
};

const SERVER_ERROR_CODES = {
  ADMIN03: BiosErrorType.NO_SUCH_TENANT,
  ADMIN04: BiosErrorType.NO_SUCH_STREAM,
  ADMIN08: BiosErrorType.TENANT_ALREADY_EXISTS,
  ADMIN09: BiosErrorType.STREAM_ALREADY_EXISTS,
  ADMIN14: BiosErrorType.STREAM_VERSION_MISMATCH,
  AUTH02: BiosErrorType.INVALID_PASSWORD,
  AUTH0a: BiosErrorType.SESSION_EXPIRED,
  AUTH11: BiosErrorType.USER_ID_NOT_VERIFIED,
  GENERIC05: BiosErrorType.CONSTRAINT_WARNING,
  GENERIC0b: BiosErrorType.OPERATION_UNEXECUTABLE,
};

function handleErrorResponse(error) {
  if (!error.response) {
    if (!!error.code) {
      return new BiosClientError(
        BiosErrorType.CLIENT_CHANNEL_ERROR, `${error.code}: ${error.message}`);
    }
    return new BiosClientError(
      BiosErrorType.GENERIC_CLIENT_ERROR, `Unexpected error: ${error.message}`);
  }
  const status = error.response.status;
  let data = error.response.data;
  const contentType = error.response.headers['content-type'];
  let payload;
  if (contentType.startsWith('application/json')) {
    if (data instanceof ArrayBuffer) {
      payload = String.fromCharCode.apply(null, new Uint8Array(data, 0, data.byteLength));
    } else if (data instanceof Buffer) {
      payload = String.fromCharCode.apply(null, data);
    }
  }
  if (payload) {
    data = JSON.parse(payload);
  }
  const errorInfo = determineError(status, data);
  let additionalMessage;
  let index;
  if (!!data && !!data.message && (index = data.message.indexOf(':')) >= 0) {
    additionalMessage = data.message.substr(index + 1).trim();
  }
  if (contentType && contentType.startsWith('text/')) {
    additionalMessage = data;
  }
  return new BiosClientError(errorInfo, additionalMessage);
}

function determineError(status, data) {
  return (data && SERVER_ERROR_CODES[data.errorCode]) || httpStatus2Error(status);
}

function httpStatus2Error(status) {
  return HTTP_STATUS_CODES[status] ||
    (Math.floor(status / 100) === 4 ?
      BiosErrorType.GENERIC_CLIENT_ERROR : BiosErrorType.GENERIC_SERVER_ERROR);
}

class InvalidArgumentError extends BiosClientError {
  constructor(message) {
    super(BiosErrorType.INVALID_ARGUMENT, message);
  }
}

class NotImplementedError extends BiosClientError {
  constructor(message) {
    super(BiosErrorType.NOT_IMPLEMENTED, message);
  }
}
