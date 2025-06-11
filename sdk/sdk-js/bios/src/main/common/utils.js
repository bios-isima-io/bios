import { v1 as uuidv1 } from 'uuid';
import { InvalidArgumentError } from './errors';


export function requiresParam(target, name, ...types) {
  const typeOfTarget = typeof (target);
  if (typeOfTarget === 'undefined') {
    throw new InvalidArgumentError(`Required parameter '${name}' of type ${types} is missing`);
  } else if ((types.includes('array') && Array.isArray(target)) || types.includes(typeOfTarget)) {
    return;
  }
  throw new InvalidArgumentError(
    `Parameter '${name}' must be of type ${types}, but ${typeof (target)} is specified (${target})`);
}

export function requiresNumParams(numParams, argumentsGiven) {
  if (argumentsGiven.length !== numParams) {
    throw new InvalidArgumentError(`Expected ${numParams} parameters but received ${argumentsGiven.length}`);
  }
}

export function requiresProperty(parentObject, parentObjectName, propertyName, ...types) {
  const prop = parentObject[propertyName];
  const typeOfTarget = typeof (prop);
  if (typeOfTarget === 'undefined') {
    throw new InvalidArgumentError(
      `Required property '${parentObjectName}.${propertyName}' of type ${types} is missing`
      + ` (${JSON.stringify(parentObject)})`);
  } else if ((types.includes('array') && Array.isArray(prop)) || types.includes(typeOfTarget)) {
    return;
  }
  throw new InvalidArgumentError(
    `Property '${parentObjectName}.${propertyName}' must be of type ${types},`
    + ` but ${typeOfTarget} is specified (${prop})`);
}

/**
 * Validate a non-empty string.
 *
 * @param value Value to validate
 * @param {string} name - Parameter name
 * @return {string} - Validated string
 */
export function validateNonEmptyString(value, name) {
  requiresParam(value, name, 'string');
  const out = value.trim();
  if (out === '') {
    throw new InvalidArgumentError(`Parameter ${name} must be a non-empty string`);
  }
  return out;
}

export function validateMetric(metric) {
  if (typeof (metric) === 'object') {
    requiresProperty(metric, 'metric', 'measurement', 'string');
    metric.type === 'derived' ? validateDerivedMeasurement(metric.measurement) : validateMeasurement(metric.measurement);
    return metric;
  } else if (typeof (metric) === 'string') {
    validateMeasurement(metric);
    return {
      measurement: metric
    };
  }
  // shouldn't happen
  return null;
}

const reMeasurement =
  new RegExp('^([a-zA-Z0-9_]+)\\.([a-zA-Z0-9][a-zA-Z0-9_]+)\\(([a-zA-Z0-9_]*)\\)$');

export function validateMeasurement(measurement) {
  const matched = reMeasurement.exec(measurement);
  if (!matched) {
    throw new InvalidArgumentError(
      "Syntax error: Parameter 'metric' must have a format of <signal>.<function>(<parameter>).");
  }
  const func = matched[2].toLowerCase();
  const domain = matched[3];
  switch (func) {
    case 'count':
      if (domain) {
        throw new InvalidArgumentError("Function 'count()' must not take parameters");
      }
      break;
    case 'sum':
    case 'min':
    case 'max':
      if (!domain) {
        throw new InvalidArgumentError(`Function '${func}()' must take a parameter`);
      }
      break;
    default:
    // we defer the validation to later phase.
  }
}

export function validateDerivedMeasurement(measurement) {
  var regex = /(?:[\w]*\.[a-zA-Z0-9_]+\(.*?\))/g;
  const metricValues = measurement.match(regex);
  metricValues.forEach((entity) => {
    validateMeasurement(entity);
  });
}

/**
 * Time management utilities.
 */
export const timeUtils = {
  seconds: (value) => value * 1000,
  minutes: (value) => value * 60000,
  hours: (value) => value * 3600000,
  days: (value) => value * 24 * 3600 * 1000,
  now: () => Date.now(),
};

/**
 * Generates a v1 UUID.
 *
 * @returns {Uint8Array} UUID as a Buffer.
 */
export function generateTimeUuid() {
  const buffer = new Uint8Array(16);
  uuidv1(null, buffer, 0);
  return buffer;
}

/**
 * Canonicalize a UUID of type Uint8Array.
 *
 * @param {Uint8Array} uuid - Input UUID buffer
 * @returns {string} Canonical UUID
 */
export function canonicalizeUuid(uuid) {
  let out = Buffer.from(uuid.subarray(0, 4)).toString('hex');
  out += '-';
  out += Buffer.from(uuid.subarray(4, 6)).toString('hex');
  out += '-';
  out += Buffer.from(uuid.subarray(6, 8)).toString('hex');
  out += '-';
  out += Buffer.from(uuid.subarray(8, 10)).toString('hex');
  out += '-';
  out += Buffer.from(uuid.subarray(10, 16)).toString('hex');
  return out;
}

/**
 * Determine whether the passed value is an actual min value or a placeholder for missing data;
 * returns the min value if it is real data and null if the data is missing.
 */
export function minIfExists(maybeMin) {
  if (maybeMin >= Number.POSITIVE_INFINITY) {
    return null;
  }
  return maybeMin;
}

/**
 * Determine whether the passed value is an actual max value or a placeholder for missing data;
 * returns the max value if it is real data and null if the data is missing.
 */
export function maxIfExists(maybeMax) {
  if (maybeMax <= Number.NEGATIVE_INFINITY) {
    return null;
  }
  return maybeMax;
}

const ROLE_TO_NAME = {
  SystemAdmin: 'System Administrator',
  TenantAdmin: 'Administrator',
  SchemaExtractIngest: 'ML Engineer',
  Extract: 'Data Scientist',
  Ingest: 'Data Engineer',
  Report: 'Business User'
};

/**
 * Converts a role to its name.
 *
 * @param role Role
 * @returns Name
 */
export function roleToName(role) {
  requiresParam(role, 'role', 'string');
  return ROLE_TO_NAME[role] || '';
}

export function isDebugLogEnabled(logItem) {
  /* eslint-disable no-console */
  let isLogEnabled = false;
  let biosDebug;
  // For browser, use URL query parameter.
  const isBrowser = typeof window !== 'undefined';
  const queryString = isBrowser ? window.location.search : '';
  const urlParams = new URLSearchParams(queryString);
  if (!!urlParams.get('biosDebug')) {
    biosDebug = urlParams.get('biosDebug');
    const logItems = biosDebug.split('-');
    if (logItems.includes(logItem)) {
      isLogEnabled = true;
      console.log(logItem + " included in biosDebug logItems = " + JSON.stringify(logItems));
    }
    return isLogEnabled;
  }
  // To accommodate non-browser case e.g. integration tests, use environment variable.
  let env;
  try {
    // requires karma-env-preprocessor
    env = window.__env__;
  } catch (e) { }
  if (env && !!env.BIOS_DEBUG) {
    biosDebug = env.BIOS_DEBUG;
    const logItems = biosDebug.split('-');
    if (logItems.includes(logItem)) {
      isLogEnabled = true;
      console.log(logItem + " included in BIOS_DEBUG logItems = " + JSON.stringify(logItems));
    }
  }
  /* eslint-enable no-console */
  return isLogEnabled;
}

export function debugLog(logItem, object, sequenceNumber) {
  if (isDebugLogEnabled(logItem)) {
    const type = typeof object;
    let message;
    switch (type) {
      case 'function':
        message = object();
        break;
      case 'string':
        message = object;
        break;
      default:
        message = JSON.stringify(object);
    }
    /* eslint-disable no-console */
    if (sequenceNumber !== null) {
      console.log(`[${sequenceNumber}]:`, message);
    } else {
      console.log(message);
    }
    /* eslint-enable no-console */
  }
}
