import { InsertBulkRequest as InsertBulkRequestProto } from './proto';
import { createUuidPayload } from './messages';

const insertBulkRequestEncoder = {
  encode,
};
export default insertBulkRequestEncoder;

/**
 * Method to encode single-event insert request.
 *
 * @param {InsertISqlRequest} insertRequest - Insert iSQL request.
 * @param {number} offset
 * @param {number} length
 * @returns {Uint8Array} Encoded data.
 */
function encode(insertRequest, offset, length) {
  const records = [];
  for (let i = offset; i < offset + length; ++i) {
    const record = {
      eventId: insertRequest.data.eventIds[i],
      stringValues: [insertRequest.data.csvs[i]],
    };
    records.push(record);
  }
  const payload = {
    contentRep: insertRequest.data.contentRep,
    record: records,
    signal: insertRequest.data.signal,
  };
  const errorMessage = InsertBulkRequestProto.verify(payload);
  if (errorMessage) {
    throw new Error(`insertBulkRequest verification error: ${errorMessage}`); // TODO throw better error
  }

  const message = InsertBulkRequestProto.create(payload);
  const initialBuffer = InsertBulkRequestProto.encode(message).finish();
  // InitialBuffer may be backed by a larger array that may cause content length miscalculation
  // in Axios. We recreate the array with the data length.
  return new Uint8Array(initialBuffer, 0, initialBuffer.byteLength);
}