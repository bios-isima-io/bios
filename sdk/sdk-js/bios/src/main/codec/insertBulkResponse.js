import {
  InsertBulkResponse as InsertBulkResponseProto,
  InsertSuccessResponse as InsertSuccessResponseProto,
} from './proto';
import { createUuidPayload, parseUuidMessage } from './messages';
import { canonicalizeUuid } from '../common/utils';

const insertBulkResponseDecoder = {
  decode,
};
export default insertBulkResponseDecoder;

/**
 * Decodes insertBulk response byte array and parse it into the results array.
 *
 * @param {Uint8Array} buffer - Response data buffer
 * @param {number} offset - Offset in the results array where the method should fill the response.
 * @param {array} responses - Output responses array. The method fills the responses into this array from the offset index.
 *   Slots where the results are filled should be pre-allocated.
  */
function decode(buffer, offset, responses) {
  const insertBulkResponseProto = InsertBulkResponseProto.decode(buffer);
  insertBulkResponseProto.responses.forEach((response, i) => {
    const insertSuccessResponse = InsertSuccessResponseProto.toObject(response);
    const eventId = canonicalizeUuid(response.eventId);
    const timestamp = insertSuccessResponse.insertTimestamp;
    responses[offset + i] = {
      eventId,
      timestamp,
    };
  });
}