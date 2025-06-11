import { requiresParam, validateNonEmptyString } from '../common/utils';
import { InvalidArgumentError } from '../common/errors';
import { ISqlRequest } from './isqlRequest';
import { ContentRepresentation, StatementType } from './models';

export class Upsert {
  static upsert() {
    const data = new UpsertStatementData();
    return new Into(data);
  }
}

class UpsertStatementData {
  constructor() {
    this.statementType = StatementType.UPSERT;
  }
}

export class Into {
  constructor(data) {
    this._data = data;
  }

  /**
   * Specifies the context name to insert against.
   *
   * @param {string} context - The context name
   */
  into(context) {
    this._data.contextName = validateNonEmptyString(context, 'context');
    return new UpsertData(this._data);
  }
}

class UpsertData {
  constructor(data) {
    this._data = data;
  }

  /**
   * Specifies an entry in CSV format.
   *
   * @param {string} eventText - Record to upsert in CSV.
   */
  csv(eventText) {
    requiresParam(eventText, 'eventText', 'string');
    this._data.texts = [eventText];
    this._data.contentRepresentation = ContentRepresentation.CSV;
    return new UpsertBuilder(this._data);
  }

  /**
   * Specifies entries in CSV format.
   *
   * @param {list<string>} events  - Records to upsert in CSV.
   */
  csvBulk(events) {
    requiresParam(events, 'events', 'array');
    if (events.length === 0) {
      throw new InvalidArgumentError(`Parameter 'events' must be a non-empty list string`);
    }
    this._data.texts = events.map((event, i) => validateNonEmptyString(event, `events[${i}]`));
    this._data.contentRepresentation = ContentRepresentation.CSV;
    return new UpsertBuilder(this._data);
  }
}

class UpsertBuilder {
  constructor(data) {
    this._data = data;
  }

  build() {
    return new UpsertISqlRequest(this._data);
  }
}

class UpsertISqlRequest extends ISqlRequest {
  constructor(data) {
    super(data.statementType);
    this._data = data;
  }

  getTarget() {
    return this._data.contextName;
  }

  getRequestMessage() {
    return {
      contentRepresentation: this._data.contentRepresentation,
      entries: this._data.texts,
    };
  }
}
