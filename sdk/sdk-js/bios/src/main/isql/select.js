import {
  createDimensionsPayload,
  createMetricPayload,
  createOrderByPayload,
  createSlidingWindowPayload,
  createTumblingWindowPayload
} from '../codec/messages';

import { MetricFunction, SelectQuery, WindowType } from '../codec/proto';
import { requiresParam, requiresProperty, validateNonEmptyString } from '../common/utils';
import { InvalidArgumentError, NotImplementedError } from '../common/errors';
import { ISqlRequest } from './isqlRequest';
import { ContentRepresentation, StatementType } from './models';

export class Select {
  static select(...selectTargets) {
    return new From(...selectTargets);
  }
}

class SelectSignalRequestData {
  constructor(targetSignal, ...selectTargets) {
    this.statementType = StatementType.SELECT;
    var { attributes, metrics } = getAttributesAndMetrics(selectTargets);
    this.query = {
      from: targetSignal,
      attributes: attributes,
    };
    if (metrics.length > 0) {
      this.query.metrics = metrics;
    }
  }
}

class SelectContextRequestData {
  constructor(contextName, ...selectTargets) {
    this.statementType = StatementType.SELECT_CONTEXT;
    this.contextName = contextName;
    var { attributes, metrics } = getAttributesAndMetrics(selectTargets, true);
    this.attributes = attributes;
    this.metrics = metrics;
    this.query = {};
    if ((attributes !== null) && (attributes.attributes.length > 0)) {
      this.query.attributes = attributes.attributes;
    }
    this.query.metrics = metrics;
  }
}

function getAttributesAndMetrics(selectTargets, useMetricFunctionName = false) {
  let attributes = null;
  const metrics = [];
  selectTargets.forEach((metricSpec) => {
    if (!attributes) {
      attributes = { attributes: [] };
    }
    const validated = validateMetricSpec(metricSpec, useMetricFunctionName);
    if (!!validated.name) {
      attributes.attributes.push(validated.name);
    } else {
      metrics.push(validated);
    }
  });
  return { attributes, metrics };
}

function validateMetricSpec(metricSpec, useMetricFunctionName) {
  const specType = typeof metricSpec;
  switch (specType) {
    case 'object':
      requiresProperty(metricSpec, 'metric', 'metric', 'string');
      const metricPayload = parseMetricString(metricSpec.metric, useMetricFunctionName);
      if (!!metricSpec.as) {
        requiresProperty(metricSpec, 'metric', 'as', 'string');
        metricPayload.as = metricSpec.as;
      }
      return metricPayload;
    case 'string':
      return parseMetricString(metricSpec, useMetricFunctionName);
    default:
      throw new InvalidArgumentError('select() method parameter must be object or string');
  }
}

const re = new RegExp('([a-zA-Z0-9][a-zA-Z0-9_]+)\\((.*)\\)');

function parseMetricString(metricString, useMetricFunctionName) {
  let matched = re.exec(metricString);
  if (matched) {
    let func = matched[1];
    const domain = matched[2].trim();

    if (!MetricFunction.hasOwnProperty(func.toUpperCase())) {
      throw new InvalidArgumentError(`${func}: unsupported function`);
    }
    if ((func.toUpperCase() === 'COUNT') && (domain !== '')) {
      throw new InvalidArgumentError('count() function must not take parameter');
    }

    if (useMetricFunctionName) {
      return createMetricPayload(func.toUpperCase(), domain)
    }
    return createMetricPayload(MetricFunction[func.toUpperCase()], domain)
  }
  return {
    name: metricString.trim()
  };
}

export class From {
  constructor(...selectTargets) {
    this._selectTargets = selectTargets;
  }

  /**
   * Specifies the target signal name
   *
   * @param {string} signalName - The signal name
   */
  from(signalName) {
    const target = validateNonEmptyString(signalName, 'signalName');
    const requestData = new SelectSignalRequestData(target, ...this._selectTargets);
    return new Where(requestData);
  }

  /**
   * Specifies the target context name
   *
   * @param {string} contextName - The context name
   */
  fromContext(contextName) {
    const target = validateNonEmptyString(contextName, 'contextName');
    const requestData = new SelectContextRequestData(target, ...this._selectTargets);
    return new ContextWhere(requestData);
  }
}

class SummarizeRequest {
  constructor(params) {
    this._requestData = params;
  }

  /**
   * Specifies to run the operation in on-the-fly mode.
   *
   * @returns self
   */
  onTheFly() {
    this._requestData.query.onTheFly = true;
    return this;
  }

  /**
   * Method to build a execute statement object.
   *
   * The object consists of following properties:
   *     * {string} type - Statement type, 'SELECT' or 'INSERT'
   *     * {object} query - Request query message
   */
  build() {
    return new SelectISqlRequest(this._requestData);
  }
}

class SnappedTimeRange {
  constructor(selectSignalRequest) {
    this._requestData = selectSignalRequest;
  }

  snappedTimeRange(origin, delta, snapStepSize) {
    const windows = this._requestData.query.windows;
    _assert(windows !== undefined && windows.length > 0);
    const window = windows[0];
    if (!snapStepSize) {
      switch (window.windowType) {
        case WindowType.TUMBLING_WINDOW:
          snapStepSize = window.tumbling.windowSizeMs;
          break;
        case WindowType.SLIDING_WINDOW:
          snapStepSize = window.sliding.slideInterval;
          break;
      }
    }
    this._requestData.origin = origin;
    this._requestData.delta = delta;
    this._requestData.snapStepSize = snapStepSize;
    return new SummarizeRequest(this._requestData);
  }
}

class AnyTimeRange extends SnappedTimeRange {
  constructor(selectSignalRequest) {
    super(selectSignalRequest);
  }

  timeRange(origin, delta) {
    let startTime = origin;
    let endTime = origin + delta;
    if (endTime < startTime) {
      let temp = endTime;
      endTime = startTime;
      startTime = temp;
    }
    this._requestData.query.startTime = startTime;
    this._requestData.query.endTime = endTime;
    return new SelectISqlRequest(this._requestData);
  }
}

class TimeWindow extends AnyTimeRange {
  constructor(selectSignalRequest) {
    super(selectSignalRequest);
  }

  /**
   * Specifies tumbling window.
   *
   * @param {number} windowSizeMs - Tumbling window size in milliseconds.
   */
  tumblingWindow(windowSizeMs) {
    this._requestData.query.windows = [createTumblingWindowPayload(windowSizeMs)];
    return new SnappedTimeRange(this._requestData);
  }

  /**
   * Specifies sliding window.
   *
   * @param {number} slideIntervalMs - Slide interval in milliseconds.
   * @param {number} windowSlides - Number of slides in a window.
   */
  slidingWindow(slideIntervalMs, windowSlides) {
    this._requestData.query.windows = [createSlidingWindowPayload(slideIntervalMs, windowSlides)];
    return new SnappedTimeRange(this._requestData);
  }
}

class Limit extends TimeWindow {
  constructor(selectSignalRequest) {
    super(selectSignalRequest);
  }

  limit(limitation) {
    this._requestData.query.limit = limitation;
    return this;
  }
}

class StatementOptions extends Limit {
  constructor(selectSignalRequest) {
    super(selectSignalRequest);
  }

  groupBy(...dimensions) {
    this._requestData.query.groupBy = createDimensionsPayload(...dimensions);
    return this;
  }

  orderBy(sortSpec) {
    this._requestData.query.orderBy = createOrderByPayload(sortSpec);
    return this;
  }
}

class Where extends StatementOptions {
  constructor(selectSignalRequest) {
    super(selectSignalRequest);
  }

  where(filter) {
    this._requestData.query.where = filter;
    return new StatementOptions(this._requestData);
  }
}

class SelectISqlRequest extends ISqlRequest {
  constructor(params) {
    super(StatementType.SELECT);
    const { origin, delta, snapStepSize } = params;
    if (origin && delta && snapStepSize) {
      const { startTime, endTime } =
        _calculateSnappedTimeRange(origin, delta, snapStepSize, params.query.onTheFly);
      params.query.startTime = startTime;
      params.query.endTime = endTime;
    }
    this._requestData = params;
  }

  get query() {
    const query = this._requestData.query;
    const errorMessage = SelectQuery.verify(query);
    if (errorMessage) {
      throw new Error(`Query verification error: ${errorMessage}`); // TODO throw better error
    }
    return query;
  }

  toString() {
    const elements = ['SELECT '];
    const query = this._requestData.query;
    if (query.attributes && query.attributes.attributes) {
      elements.push(query.attributes.attributes.join(', '));
    }
    if (query.metrics) {
      let delim = '';
      elements.push(query.metrics
        .map((metric) => {
          const func = this._getFunctionName(metric.function);
          const of = metric.of ? metric.of : '';
          const as = metric.as ? ` AS ${metric.as}` : '';
          return `${func}(${of})${as}`;
        })
        .join(', '));
    }
    elements.push(' FROM ', query.from);
    if (query.where) {
      elements.push(' WHERE ', query.where);
    }
    if (query.groupBy && query.groupBy.dimensions) {
      elements.push(' GROUP BY ');
      elements.push(query.groupBy.dimensions.join(', '));
    }
    if (query.orderBy) {
      elements.push(' ORDER BY ', query.orderBy.by);
      if (query.orderBy.reverse) {
        elements.push(' DESC');
      }
    }
    if (query.limit) {
      elements.push(' LIMIT ', query.limit);
    }
    if (query.windows) {
      if (query.windows[0].tumbling) {
        elements.push(' WINDOW ', query.windows[0].tumbling.windowSizeMs);
      }
      if (query.windows[0].sliding) {
        const interval = query.windows[0].sliding.slideInterval;
        elements.push(' WINDOW ', interval * query.windows[0].sliding.windowSlides);
        elements.push(' SLIDING ', interval);
      }
    }
    elements.push(' SINCE ', new Date(query.startTime).toISOString(), ' (' + query.startTime + ')');
    elements.push(' UNTIL ', new Date(query.endTime).toISOString(), ' (' + query.endTime + ')');
    if (query.onTheFly) {
      elements.push(' ON THE FLY');
    }
    return elements.join('');
  }

  _getFunctionName(functionNumber) {
    const entry = Object.entries(MetricFunction).find(([name, value]) => value === functionNumber);
    return entry && entry[0].toLowerCase();
  }

  /**
   * Method to build a execute statement object.
   *
   * The object consists of following properties:
   *     * {string} type - Statement type, 'SELECT' or 'INSERT'
   *     * {object} query - Request query message
   */
  build() {
    return this;
  }
}

class ContextSelectISqlRequestEx extends ISqlRequest {
  constructor(params) {
    super(StatementType.SELECT_CONTEXT_EX);
    this._requestData = params;
  }

  getTarget() {
    return this._requestData.contextName;
  }

  getRequestMessage() {
    return this._requestData.query;
  }

  /**
   * Method to build a execute statement object.
   */
  build() {
    return this;
  }
}

class ContextOrderBy extends ContextSelectISqlRequestEx {
  constructor(selectContextRequest) {
    super(selectContextRequest);
  }

  orderBy(sortSpec) {
    this._requestData.query.orderBy = createOrderByPayload(sortSpec);
    return new ContextSelectISqlRequestEx(this._requestData);
  }
}

class ContextGroupBy extends ContextOrderBy {
  constructor(selectContextRequest) {
    super(selectContextRequest);
  }

  groupBy(...dimensions) {
    this._requestData.query.groupBy = createDimensionsPayload(...dimensions).dimensions;
    return new ContextOrderBy(this._requestData);
  }
}

class ContextWhere extends ContextGroupBy {
  constructor(selectContextRequest) {
    super(selectContextRequest);
  }

  /**
   * Specifies entries to fetch.
   *
   * @param  {...object} specs - Entry specifications
   * @property {list<string|number|boolean>} specs.key - Primary keys of one or more entries to
   * select
   */
  where(...specs) {
    if (specs.length === 0) {
      throw new InvalidArgumentError('A condition or at least one primary key must be specified');
    }
    if ((specs.length === 1) && (typeof specs[0] === 'string')) {
      this._requestData.query.where = specs[0];
      return new ContextGroupBy(this._requestData);
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
    return new ContextSelectISqlRequest(this._requestData);
  }
}

class ContextSelectISqlRequest extends ISqlRequest {
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

function _round(timestamp, step) {
  return Math.floor(timestamp / step) * step;
}

function _calculateSnappedTimeRange(origin, delta, snapStepSize, onTheFly) {
  let left = origin;
  let right = origin + delta;
  if (left > right) {
    [left, right] = [right, left];
  }
  return {
    startTime: _round(left, snapStepSize),
    endTime: onTheFly ? right : _round(right, snapStepSize),
  };
}

function _assert(condition) {
  if (!condition) {
    throw new Error('assertion error');
  }
}
