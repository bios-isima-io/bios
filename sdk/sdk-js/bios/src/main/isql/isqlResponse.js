import { ResponseType } from '.';

/**
 * ISql operation request class
 */
export class ISqlResponse {
  /**
   * The constructor.
   *
   * @param {string} type - Response type.
   */
  constructor(type) {
    this.responseType = type;
  }
}

/**
 *
 * @param {list<object>} records - List of record
 * @property {number} record.eventId - eventId
 * @property {number} record.timestamp - timestamp value
 *
 * @returns
 *   e.g.
 *     {
 *       responseType: 'INSERT',
 *       records: [
 *         {
 *           eventId : 32327772,
 *           timestamp : 1601560539882
 *         },
 *         {
 *           eventId : 32327672,
 *           timestamp : 1601560989882
 *         }
 *       ]
 *     }
 */
export class InsertISqlResponse extends ISqlResponse {
  constructor(records) {
    super(ResponseType.INSERT);
    this.records = records;
  }

  getRecords() {
    return this.records;
  }
}

/**
 *
 * @param {list<object>} definitions - List of definition
 * @property {string} definition.name - name of individual definition
 * @property {string} definition.type - timestamp for individual definition
 *
 * @param {list<object>} dataWindows - List of dataWindow
 * @property {number} dataWindow.windowBeginTime - windowBeginTime
 * @property {list<number>} dataWindow.records - list of record
 *
 * @returns
 *   e.g.
 *     {
 *     "responseType": "SELECT",
 *     "definitions": [
 *       {"name": "order_id","type": "INTEGER"},
 *       {"name": "count()","type": "INTEGER"}
 *     ],
 *     "dataWindows": [
 *       {
 *         "windowBeginTime": 1601551800000,
 *         "records": [
 *           [143,5],
 *           [3486,5]
 *         ]
 *       }
 *     ]
 *   }
 */
export class SelectISqlResponse extends ISqlResponse {
  constructor(definitions, dataWindows, isWindowedResponse, requestQueryNum) {
    super(ResponseType.SELECT);
    this.definitions = definitions;
    this.dataWindows = dataWindows;
    this.isWindowedResponse = isWindowedResponse;
    this.requestQueryNum = requestQueryNum;
    this._propMapping = null;
  }

  /**
   * Gets data windows.
   *
   * @returns {list<object>} List of definitions
   * @deprected Access by property dataWindows or use method getDataWindows().
   */
  get data() {
    return this.dataWindows;
  }

  /**
   * Gets definitions.
   *
   * @returns
   */
  getDefinitions() {
    return this.definitions;
  }

  getDataWindows() {
    return this.dataWindows;
  }

  isWindowedResponse() {
    return this.isWindowedResponse;
  }

  getRequestQueryNum() {
    return this.requestQueryNum;
  }

  get(record, propertyName) {
    if (this._propMapping === null) {
      this._propMapping = this.definitions.reduce((map, definition, i) => {
        map[definition.name] = i;
        return map;
      }, {});
    }
    const index = this._propMapping[propertyName];
    if (index === undefined || index === null) {
      return null;
    }
    return record[index];
  }
}

/**
 *
 * @param {object} data - data object
 * @property {string} data.contentRepresentation - context representation
 * @property {list<string>} data.primaryKey - List of primary key attributes
 * @property {list<object>} data.definitions - list of definition object
 *      @property {string} definitions.attributeName - attribute name
 *      @property {string} definitions.type - attribute type
 *
 * @returns
 *   e.g.
 *   {
 *     "responseType": "SELECT_CONTEXT",
 *     "contentRepresentation": "UNTYPED",
 *     "primaryKey": ["ip"],
 *     "definitions": [
 *       {"attributeName": "ip","type": "String"},
 *       {"attributeName": "site","type": "String"}
 *     ],
 *     "entries": [
 *       ["172.0.0.1","localhost"],
 *       ["20.30.40.50","tfos"]
 *     ]
 *   }
 */
export class SelectContextISqlResponse extends ISqlResponse {
  constructor(data) {
    super(ResponseType.SELECT_CONTEXT);
    this.contentRepresentation = data.contentRepresentation;
    this.primaryKey = data.primaryKey;
    this.definitions = data.definitions;
    this.entries = data.entries;
  }
}

/**
 *
 * @returns
 *   e.g.
 *     {"responseType": "VOID"}
 *
 */
export class VoidISqlResponse extends ISqlResponse {
  constructor() {
    super(ResponseType.VOID);
  }

  static get INSTANCE() {
    return voidResponseInstance;
  }
}

const voidResponseInstance = new VoidISqlResponse();
