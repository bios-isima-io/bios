import {requiresParam, requiresProperty, validateNonEmptyString} from '../common/utils';
import {InvalidArgumentError} from '../common/errors';
import {ISqlRequest} from './isqlRequest';
import {ContentRepresentation, StatementType} from './models';

export class Delete {
  static delete() {
    return new DeleteFrom();
  }
}

class DeleteRequestData {
  constructor() {
    this.statementType = StatementType.DELETE;
  }
}

export class DeleteFrom {
  constructor() {
    this._requestData = new DeleteRequestData()
  }

  /**
   * Specifies the target context name
   *
   * @param {string} contextName - The context name
   */
  fromContext(contextName) {
    this._requestData.contextName = validateNonEmptyString(contextName, 'contextName');
    return new DeleteContextWhere(this._requestData);
  }
}

class DeleteContextWhere {
  constructor(selectContextRequest) {
    this._requestData = selectContextRequest;
  }

  /**
   * Specifies entries to delete.
   *
   * @param  {...object} specs - Entry specifications
   * @property {list<string|number|boolean>} specs.key - Primary keys of one or more entries to
   * delete
   */
  where(...specs) {
    if (specs.length === 0) {
      throw new InvalidArgumentError('At least one primary key must be specified');
    }
    this._requestData.primaryKeys = specs.map((spec, i) => {
      requiresParam(spec, `spec[${i}]`, 'object');
      requiresProperty(spec, `spec[${i}]`, 'key', 'string', 'number', 'boolean', 'array');
      if (typeof spec.key === 'object') {
        return spec.key;
      } else {
        return [spec.key];
      }
    })
    return new ContextDeleteISqlRequest(this._requestData);
  }
}

class ContextDeleteISqlRequest extends ISqlRequest {
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
      primaryKeys: this._requestData.primaryKeys
    };
  }

  /**
   * Method to build a execute statement object.
   */
  build() {
    return this;
  }
}
