import Long from 'long';
import { WindowType, Uuid } from './proto';

/**
 * Method that creates a protobuf message of type SlidingWindow.
 *
 * @param {number} slideInterval - Slide interval in milliseconds
 * @param {number} windowSlides  - Number of slides per window
 */
export function createSlidingWindowPayload(slideInterval, windowSlides) {
  return {
    windowType: WindowType.SLIDING_WINDOW,
    sliding: {
      slideInterval,
      windowSlides,
    }
  };
};

/**
 * Method that creates a protobuf message of type TumblingWindow .
 *
 * @param {number} windowSizeMs - Tumbling window size in milliseconds.
 */
export function createTumblingWindowPayload(windowSizeMs) {
  return {
    windowType: WindowType.TUMBLING_WINDOW,
    tumbling: {
      windowSizeMs,
    }
  };
}

export function createAttributeListPayload(...attributes) {
  // sanity check
  attributes.forEach((attribute, i) => {
    const type = typeof (attribute);
    if (type !== 'string') {
      throw new Error(`attributes[${i}]: Unsupported type ${type} is specified; it must be string`);
    }
  });
  return {
    attributes
  };
}

export function createMetricPayload(func, of, as) {
  const message = {
    function: func
  };
  if (of) {
    message.of = of;
  }
  if (as) {
    message.as = as;
  }
  return message;
};

export function createDimensionsPayload(...dimensions) {
  // sanity check
  dimensions.forEach((attribute, i) => {
    const type = typeof (attribute);
    if (type !== 'string') {
      throw new Error(`dimensions[${i}]: Unsupported type ${type} is specified; it must be string`);
    }
  });
  return {
    dimensions
  };
}

/**
 * Create an OrderBy payload object.
 *
 * @param {(string|object)} param - Sort specification. The value specifies the sort key if
 *   the type is string.
 * @property {string} param.key - Sort key
 * @property {string} param.order - Sort order; 'ASC' for ascending or 'DESC' or descending.
 *   default: 'ASC'
 * @property {boolean} caseSensitive - Sort case sensitivity in case the attribute is string.
 *   default: false
 */
export function createOrderByPayload(param) {
  let by;
  let reverse = false;
  let caseSensitive = false;
  const type = typeof (param);
  switch (type) {
    case 'string':
      by = param;
      break;
    case 'object':
      by = param.key;
      reverse = (typeof (param.order) === 'string') && (param.order.toUpperCase() === 'DESC');
      caseSensitive = (param.caseSensitive === true);
      break;
    default:
      throw new Error(`createOrderByPayload: ${param}: Invalid parameter`);
  }
  return {
    by,
    reverse,
    caseSensitive,
  };
};

/**
 * Method to create a UUID protobuf payload object from a UUID of type Uint8Array.
 *
 * @param {Uint8Array} uuid - Input UUID
 * @returns {{hi: Long, lo: Long}} UUID protobuf payload
 */
export function createUuidPayload(uuid) {
  const hi = Long.fromBytesBE(uuid.subarray(0, 8), true);
  const lo = Long.fromBytesBE(uuid.subarray(8, 16), true);
  return { hi, lo };
}

/**
 *  Method to parse a UUID protobuf message to convert to a UUID of type Uint8Array
 * @param {Uuid} message - UUID protobuf message
 * @returns {Uint8Array} Parsed UUID of type Uint8Array
 */
export function parseUuidMessage(message) {
  const hi = message.hi;
  const lo = message.lo;
  const buffer = new Uint8Array(16);
  buffer.set(hi.toBytesBE());
  buffer.set(lo.toBytesBE(), 8);
  return buffer;
}
