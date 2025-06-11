'use strict';

import bios, {
  BiosClientError,
  BiosErrorType
} from 'bios-sdk';

export {
  BIOS_ENDPOINT,
  INGEST_TIMESTAMP_MINIMUM,
  INGEST_TIMESTAMP_SIMPLE_ROLLUP,
  INGEST_TIMESTAMP_SKETCH,
  INGEST_TIMESTAMP_ROLLUP_TWO_VALUES,
  INGEST_TIMESTAMP_ON_THE_FLY_TEST,
  clearTenant,
  sleep,
  canonicalizeUuid,
};

// requires karama-env-preprocessor
const env = window.__env__;

// This sets the Jasmine default timeout before the test starts.
// This config is expected to be effective to all test cases, assuming every
// test implementation imports this file.
jasmine.DEFAULT_TIMEOUT_INTERVAL = 1200000;

const BIOS_ENDPOINT = env.BIOS_ENDPOINT_JS || env.TFOS_ENDPOINT || 'https://172.18.0.11:8443';

// timestamps set by integration test executor
// console.debug('TIMESTAMP_MINIMUM', env.TIMESTAMP_MINIMUM);
const INGEST_TIMESTAMP_MINIMUM = parseInt(env.TIMESTAMP_MINIMUM || "0");
const INGEST_TIMESTAMP_SIMPLE_ROLLUP = parseInt(env.TIMESTAMP_SIMPLE_ROLLUP || "0");
const INGEST_TIMESTAMP_SKETCH = parseInt(env.TIMESTAMP_SKETCH || "0");
const INGEST_TIMESTAMP_ROLLUP_TWO_VALUES = parseInt(env.TIMESTAMP_ROLLUP_TWO_VALUES || "0");
const INGEST_TIMESTAMP_ON_THE_FLY_TEST = parseInt(env.TIMESTAMP_ON_THE_FLY_TEST || "0");


async function clearTenant(tenantName, warnOnFailure) {
  try {
    await bios.deleteTenant(tenantName);
    // console.log(`Done deleting ${tenantName}`);
  } catch (e) {
    if (!(e instanceof BiosClientError) || e.errorCode != BiosErrorType.NO_SUCH_TENANT.errorCode) {
      throw e;
    }
    if (warnOnFailure) {
      console.warn('Tenant deletion error happened on teardown:', e);
    }
  }
}

/**
 * Sleep asynchronously.
 *
 * @param {number} ms - Sleep milliseconds
 * @returns {Promise} A promise fulfilled the sleep milliseconds later.
 */
async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Canonicalize a UUID of type Uint8Array.
 *
 * @param {Uint8Array} uuid - Input UUID buffer
 * @returns {string} Canonical UUID
 */
function canonicalizeUuid(uuid) {
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
