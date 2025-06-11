import {requiresParam, requiresProperty, validateNonEmptyString} from '../common/utils';
import {InvalidArgumentError} from '../common/errors';
import {ISqlRequest} from './isqlRequest';
import {ContentRepresentation, StatementType} from './models';

export class Update {
  static update(contextName) {
    return new UpdateSet(validateNonEmptyString(contextName, contextName));
  }
}

class UpdateRequestData {
  constructor(contextName) {
    this.statementType = StatementType.UPDATE;
    this.contextName = contextName;
  }
}

export class UpdateSet {
  constructor(contextName) {
    this._requestData = new UpdateRequestData(contextName);
  }

  /**
   * Specifies the target context name
   *
   * @param {string} contextName - The context name
   */
  set(...attributes) {
    if (attributes.length === 0) {
      throw new InvalidArgumentError('At least one attribute entry must be specified');
    }
    attributes.forEach((entry, i) => {
      const entryName = `attributes[${i}]`;
      requiresProperty(entry, entryName, 'name', 'string');
      requiresProperty(entry, entryName, 'value', 'string', 'number', 'boolean');
    });
    this._requestData.attributes = attributes;
    return new UpdateWhere(this._requestData);
  }
}

class UpdateWhere {
  constructor(selectContextRequest) {
    this._requestData = selectContextRequest;
  }

  /**
   * Specifies context entries to update.
   *
   * @param  {object} spec - Entry specification
   * @property {list<string|number|boolean>} spec.key - Primary key attributes of an entry
   */
  where(spec) {
    requiresParam(spec, 'spec', 'object');
    requiresProperty(spec, 'spec', 'key', 'string', 'number', 'boolean', 'array');
    if (typeof spec.key === 'object') {
      this._requestData.primaryKey = spec.key;
    } else {
      this._requestData.primaryKey = [spec.key];
    }
    return new UpdateISqlRequest(this._requestData);
  }
}

class UpdateISqlRequest extends ISqlRequest {
  constructor(params) {
    super(params.statementType);
    this._requestData = params;
  }

  getTarget() {
    return this._requestData.contextName;
  }

  getRequestMessage() {
    return {
      contentRepresentation: ContentRepresentation.UNTYPED,
      primaryKey: this._requestData.primaryKey,
      attributes: this._requestData.attributes,
    };
  }

  /**
   * Method to build a execute statement object.
   */
  build() {
    return this;
  }
}
