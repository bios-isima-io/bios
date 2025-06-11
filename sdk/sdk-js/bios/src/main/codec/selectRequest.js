import {SelectRequest} from './proto';

const selectRequest = {
  encode,
};
export default selectRequest;

function encode(payload) {
  const errorMessage = SelectRequest.verify(payload);
  if (errorMessage) {
    throw new Error(`Statement verification error: ${errorMessage}`); // TODO throw better error
  }

  // make the posting data
  const message = SelectRequest.create(payload);
  const initialBuffer = SelectRequest.encode(message).finish();
  // InitialBuffer may be backed by a larger array that may cause content length miscalculation
  // in Axios. We recreate the array with the data length.
  return new Uint8Array(initialBuffer, 0, initialBuffer.byteLength);
}
