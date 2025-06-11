import { canonicalizeUuid } from '../common/utils';
import { SelectISqlResponse } from '../isql';
import {
  AttributeType,
  ColumnDefinition,
  QueryResult,
  Record,
  SelectResponse,
  WindowType
} from './proto';

const selectResponse = {
  decode,
}
export default selectResponse;

/**
 * Decode and parse select operation response.
 *
 * @param response Axios response.
 * @returns {[]}
 */
function decode(response) {
  const selectResponse = SelectResponse.decode(
    new Uint8Array(response.data, 0, response.headers['content-length']));
  return parseSelectResponse(selectResponse);
}

function parseSelectResponse(message) {
  if (!message) {
    throw new Error('Illegal argument: Empty message');
  }
  const responses = [];
  message.responses.forEach((response) => {
    const definitionsSrc = response.definitions;

    const data = [];
    const dataSrc = response.data;
    dataSrc.forEach((result) => {
      const dataWindowSource = QueryResult.toObject(result, {
        longs: Number,
      });
      const windowBeginTime = dataWindowSource.windowBeginTime;
      const records = (!!dataWindowSource.records) ?
        dataWindowSource.records.map((record) => fetchMetrics(definitionsSrc, record)) : [];
      data.push({ windowBeginTime, records });
    });

    // convert definitions to obj, attribute types are protobuf enums
    const definitions =
      definitionsSrc.map((definitionSrc) => {
        const definition = ColumnDefinition.toObject(definitionSrc, { enums: String });
        return {
          name: definition.name,
          type: definition.type,
        };
      });
    responses.push(new SelectISqlResponse(definitions, data, response.isWindowedResponse, response.requestQueryNum));
  });

  return responses;
}

const fetchMetrics = (definitions, record) => {
  let idxString = 0;
  let idxLong = 0;
  let idxDouble = 0;
  let idxBoolean = 0;

  return definitions.map((definition) => {
    switch (definition.type) {
      case AttributeType.STRING:
        return record.stringValues[idxString++];
      case AttributeType.INTEGER:
        return record.longValues[idxLong++];
      case AttributeType.DECIMAL:
        return record.doubleValues[idxDouble++];
      case AttributeType.BLOB:
        return null; // TODO do proper decoding
      case AttributeType.BOOLEAN:
        return record.booleanValues[idxBoolean++];
      default:
        return null; // TODO or raise an exception?
    }
  });
};
