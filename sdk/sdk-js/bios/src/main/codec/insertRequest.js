import {InsertRequest as InsertRequestProto} from './proto';
import {createUuidPayload} from './messages';

const insertRequestCodec = {
  encode,
};
export default insertRequestCodec;

/**
 * Method to encode single-event insert request.
 *
 * @param {InsertISqlRequest} insertRequest - Insert iSQL request.
 * @returns {Uint8Array} Encoded data.
 */
function encode(insertRequest) {
  const record = {
    eventId: createUuidPayload(insertRequest.data.eventIds[0]),
    stringValues: insertRequest.data.csvs,
  };
  const payload = {
    contentRep: insertRequest.data.contentRep,
    record,
  };
  const errorMessage = InsertRequestProto.verify(payload);
  if (errorMessage) {
    throw new Error(`insert verification error: ${errorMessage}`); // TODO throw better error
  }

  const message = InsertRequestProto.create(payload);
  const initialBuffer = InsertRequestProto.encode(message).finish();
  // InitialBuffer may be backed by a larger array that may cause content length miscalculation
  // in Axios. We recreate the array with the data length.
  return new Uint8Array(initialBuffer, 0, initialBuffer.byteLength);
}