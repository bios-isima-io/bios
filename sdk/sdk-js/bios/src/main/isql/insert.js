import { ContentRepresentation } from '../codec/proto';
import { InvalidArgumentError } from '../common/errors';
import { generateTimeUuid, requiresNumParams, requiresParam, validateNonEmptyString } from '../common/utils';
import { ISqlRequest } from './isqlRequest';
import { StatementType } from './models';

export class Insert {
  static insert() {
    const context = new InsertContext();
    return new Into(context);
  }
}

export class InsertContext {
  constructor() {
    this.contentRep = null;
    this.csv = null;
  }
}

export class Into {
  constructor(context) {
    this._context = context;
  }

  into(signal) {
    this._context.signal = validateNonEmptyString(signal, 'signal');
    return new InsertData(this._context);
  }
}

class InsertData {
  constructor(context) {
    this._context = context;
  }

  /**
   * Put CSV value set.
   *
   * @param  {string} event - Value set as a CSV. One CSV string represents an event.
   * @return {InsertData} Self.
   */
  csv(event) {
    requiresParam(event, 'event', 'string');
    requiresNumParams(1, arguments);
    return this.csvBulk(event);
  }

  /**
   * Put one or more CSV value sets.
   *
   * @param  {...string} events - Value sets as CSVs. One CSV string represents an event.
   * @return {InsertData} Self.
   */
  csvBulk(...events) {
    requiresParam(events, 'events', 'array');
    if (events.length === 0) {
      throw new InvalidArgumentError("Parameters of method 'csv()' must not be empty");
    }
    if (!this._context.eventIds) {
      this._context.eventIds = [];
    }
    events.forEach((event, i) => {
      requiresParam(event, `events[${i}]`, 'string');
      this._context.eventIds.push(generateTimeUuid());
    });
    this._context.csvs = !!this._context.csvs ? this._context.csvs.concat(events) : events;
    this._context.contentRep = ContentRepresentation.CSV;
    return this;
  }

  /**
   * Put one value set.
   *
   * @param  {Array<any>} event - Event value set as an array. One array entry represents an event.
   *   The array should consist of signal attributes listed in the attribute order in its configuration.
   * @return {InsertData} Self.
   */
  values(event) {
    requiresParam(event, 'event', 'array');
    requiresNumParams(1, arguments);
    if (event.length === 0) {
      throw new InvalidArgumentError("Parameter of method 'values()' must not be empty");
    }
    // TODO(Naoki): We should build the event on the server side.
    let delimiter = '';
    let csv = '';
    event.forEach((value, j) => {
      if (value === undefined || value === null) {
        throw new InvalidArgumentError(`Parameter 'event[${j}]' must be set`);
      }
      csv += delimiter + escapedForCsv(value.toString());
      delimiter = ',';
    });
    return this.csv(csv);
  }

  /**
   * Put one or more value sets.
   *
   * @param  {...Array<any>} events - Event value sets as arrays. One array entry represents an event.
   *   Each array should consist of signal attributes listed in the attribute order in its configuration.
   * @return {InsertData} Self.
   */
  valuesBulk(...events) {
    requiresParam(events, 'events', 'array');
    if (events.length === 0) {
      throw new InvalidArgumentError("Parameters of method 'values()' must not be empty");
    }
    // TODO(Naoki): We should build the event on the server side.
    const csvs = events.map((event, i) => {
      requiresParam(event, `events[${i}]`, 'array');
      let delimiter = '';
      let csv = '';
      event.forEach((value, j) => {
        if (value === undefined || value === null) {
          throw new InvalidArgumentError(`Parameter 'events[${i}][${j}]' must be set`);
        }
        csv += delimiter + escapedForCsv(value.toString());
        delimiter = ',';
      });
      return csv;
    });
    return this.csvBulk(...csvs);
  }

  /**
   * Builds the insert request object.
   *
   * @return {InsertISqlRequest} The request object.
   */
  build() {
    return new InsertISqlRequest(this._context);
  }
}

class InsertISqlRequest extends ISqlRequest {
  constructor(context) {
    super(StatementType.INSERT);
    this._data = context;
  }

  get data() {
    return this._data;
  }

  get length() {
    return this._data.csvs.length;
  }
}

function escapedForCsv(src) {
  if (src.match(/[,"\s]/)) {
    return '"' + src.replaceAll('"', '""') + '"';
  }
  return src;
}
