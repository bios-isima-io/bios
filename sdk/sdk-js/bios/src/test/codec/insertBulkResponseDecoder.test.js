
'use strict';

const { default: insertBulkResponseDecoder } = require("../../main/codec/insertBulkResponse");

const data = [
  10, 31, 10, 22, 8, 234, 163, 156,
  189, 140, 136, 172, 224, 142, 1, 16,
  145, 215, 196, 242, 188, 160, 175, 211,
  178, 1, 16, 196, 194, 187, 203, 181, 46];

xdescribe('decode a ingest bulk response', () => {
  test('fundamental', () => {
    const buffer = new Uint8Array(data);
    const decoded = new Array(1);
    insertBulkResponseDecoder.decode(buffer, 0, decoded);
    // console.log('out', decoded);
    expect(decoded[0].eventId).toBe('8ec0b040-c7a7-11ea-b2a6-bd03ce512b91');
    expect(decoded[0].timestamp).toBe(1594933305668);
  });
});