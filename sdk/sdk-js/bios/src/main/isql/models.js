import { validateNonEmptyString } from '../common/utils';

export const StatementType = {
  INSERT: 'INSERT',
  UPSERT: 'UPSERT',
  SELECT: 'SELECT',
  SELECT_CONTEXT: 'SELECT_CONTEXT',
  UPDATE: 'UPDATE',
  DELETE: 'DELETE',
  SELECT_CONTEXT: 'SELECT_CONTEXT_EX',
};

export const ResponseType = {
  INSERT: 'INSERT',
  SELECT: 'SELECT',
  SELECT_CONTEXT: 'SELECT_CONTEXT',
  VOID: 'VOID',
};

export const ContentRepresentation = {
  CSV: 'CSV',
  UNTYPED: 'UNTYPED',
};

export class Metric {
  constructor(metricString) {
    this.metric = validateNonEmptyString(metricString, 'metricString');
  }

  /**
   * Sets alias of the metric.
   *
   * @param {string} alias - The alias.
   */
  as(alias) {
    this.as = validateNonEmptyString(alias, 'alias');
    return this;
  }
}

export class SortSpecification {
  constructor(by) {
    this.key = validateNonEmptyString(by);
    this.order = 'ASC';
  }

  /**
   * Set the sort order descending.
   *
   * @returns {SortSpecification} self
   */
  desc() {
    this.order = 'DESC';
    return this;
  }

  /**
   * Set the sort order ascending
   *
   * @return {SortSpecification} self
   */
  asc() {
    this.order = 'ASC';
    return this;
  }

  /**
   * Makes the sort case insensitive.
   *
   * @returns {SortSpecification} self.
   */
  ignoreCase() {
    this.caseSensitive = true;
    return this;
  }
}
