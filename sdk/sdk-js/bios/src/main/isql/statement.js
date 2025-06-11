// statement builders ////////////////////////////////////////

import { Delete } from './delete';
import { Insert } from './insert';
import { Select } from './select';
import { Update } from './update';
import { Upsert } from './upsert';
import { requiresParam, validateNonEmptyString } from '../common/utils';
import { Metric, SortSpecification } from './models';

/**
 * Class that starts building an isql statement.
 */
export class ISqlStatementBuilder {
  constructor() {
  }

  /**
   * Initiates an insert statement.
   */
  insert() {
    return Insert.insert();
  }

  /**
   * Initiates an upsert statement.
   */
  upsert() {
    return Upsert.upsert();
  }

  /**
   * Initiates a select statement.
   *
   * @param  {...any} selectTargets - Select target attribute or metrics
   */
  select(...selectTargets) {
    return Select.select(...selectTargets);
  }

  /**
   * Initiates an update statement.
   *
   * @param {string} contextName - Name of the target context.
   */
  update(contextName) {
    return Update.update(contextName);
  }

  /**
   * Initiates a delete statement.
   */
  delete() {
    return Delete.delete();
  }
}

/**
 * Helper class for building iSql statements.
 */
export class ISqlStatementHelper {
  /**
   * Helper method to build a metric specification object.
   *
   * @param {string} metricString - Metric string specification.
   * @returns {Metric} The metric specification object
   */
  static metric(metricString) {
    return new Metric(metricString);
  }

  /**
   * Helper method to build a sort specification object used for 'orderBy' clause.
   *
   * @param {string} by - The sort key.
   * @returns {SortSpecification} The sort specification object
   */
  static sort(by) {
    return new SortSpecification(by);
  }
  static order(by) {
    return new SortSpecification(by);
  }

  /**
   * Helper method to build a primary key entry object.
   *
   * @param  {...string|number|boolean} primaryKeyAttributes - Values of attributes of a primary key
   */
  static key(...primaryKeyAttributes) {
    const validated = primaryKeyAttributes.map((attributeValue, i) => {
      requiresParam(attributeValue, `primaryKeyAttributes[${i}]`, 'string', 'number', 'boolean');
      return attributeValue;
    });
    return {
      key: validated,
    };
  }

  /**
   * Helper method to build an attribute entry.
   *
   * @param {string} name - Attribute name
   * @param {string|number|boolean} value - Attribute value
   * @returns {object} Attribute object
   */
  static attribute(name, value) {
    const validated = validateNonEmptyString(name, 'name');
    requiresParam(value, 'value', 'string', 'number', 'boolean');
    return {
      name: validated,
      value,
    };
  }
};
