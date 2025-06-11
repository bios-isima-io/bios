'use strict';

import { BiosClientError, BiosErrorType, InvalidArgumentError } from '../common/errors';
import {
  debugLog, maxIfExists, minIfExists, requiresParam, requiresProperty, roleToName, timeUtils, validateMeasurement
} from '../common/utils';
import { ISqlStatementBuilder, ISqlStatementHelper } from '../isql/statement';
import {
  AttributeCategory, DataPoints, InformationType, PositiveIndicator, UnitPosition, ViewType
} from "./analysis";
import HttpImpl from './biosHttp';
import {
  TenantAppendixCategory
} from './tenantAppendix';

export {
  debugLog, maxIfExists, minIfExists, AttributeCategory, BiosClientError, BiosErrorType, DataPoints,
  InformationType, PositiveIndicator, UnitPosition, ViewType
};


let _globalSession = null;

/** @class BiosClient biOS client */
export class BiosSession {

  /**
   * Initializes a biOS session.
   *
   * @param {object} config - Login configuration object
   * @property {string} config.endpoint - Server endpoint URL (required)
   * @property {boolean} config.nonGlobal - Make the session non-global (optional)
   * @property {function(error)} config.unauthorizedErrorInterceptor - Function called on
   *   receiving an unauthorized error (optional)
   */
  static initialize(config) {
    BiosSession._require(config, 'config', 'object');
    BiosSession._requireProperty(config, 'config', 'endpoint', 'string');
    if (!!config.unauthorizedErrorInterceptor) {
      BiosSession._requireProperty(config, 'config', 'unauthorizedErrorInterceptor', 'function');
    }

    const session = HttpImpl.initialize(config);
    if (!config.nonGlobal) {
      _globalSession = session;
    }
    return session;
  }

  /**
   * Signs in to a session.
   *
   * @param {object} config - Login configuration object
   * @property {string} config.email - User's email address. (required)
   * @property {string} config.password - User's password. (required)
   * @property {string} config.appName - Application name (optional)
   * @property {string} config.appType - Application type. Possible values are
   *   'BATCH', 'REALTIME', and 'ADHOC' (optional)
   */
  static async signIn(config) {
    return this._getGlobalSession().signIn(config);
  }

  /**
   * Getter used for global login.
   * @deprecated This method will be removed in later version. Replace by
   * bios.initialize() followed by bios.signIn().
   */
  static get global() {
    return GlobalBiosClient;
  }

  /**
   * Clear the session, use only for testing.
   */
  static __clear() {
    _globalSession = null;
  }

  static get time() {
    return timeUtils;
  }

  /**
   * Conducts login to start a session.
   *
   * @param {object} config - Login configuration object
   * @property {string} config.email - User's email address. (required)
   * @property {string} config.password - User's password. (required)
   * @property {string} config.endpoint - Server endpoint URL. (required)
   * @deprecated Use the method bios.initialize() followed by bios.signIn().
   */
  static async login(config) {
    await this.initialize(config);
    return this.signIn(config);
  }

  /**
   * Binds an established Axios instance to the BIOS Client.
   *
   * @param {object} axios - The instance to attach.
   * @returns {Promise<BiosSessionHttp>}
   * @deprecated
   */
  static async bindAxios(axios) {
    _globalSession = await HttpImpl.bind(axios);
    return _globalSession;
  }

  /**
   * Sign out.
   *
   * @returns {Promise<void>}
   */
  static async signOut() {
    if (!!_globalSession) {
      await _globalSession.signOut();
    }
  }

  /**
   * Logout from the session.
   *
   * @returns {Promise<void>}
   * @deprecated Use the method signOut()
   */
  static async logout() {
    return this.signOut();
  }

  static _getGlobalSession() {
    if (!_globalSession) {
      throw new BiosClientError(BiosErrorType.NOT_LOGGED_IN, 'Session has not been initialized');
    }
    return _globalSession;
  }

  static get _default() {
    const globalSession = this._getGlobalSession();
    if (!globalSession.isSignedIn()) {
      throw new BiosClientError(BiosErrorType.NOT_LOGGED_IN, 'Login to start the session');
    }
    return globalSession;
  }

  /**
   * Returns tenant name the session is signing in.
   *
   * @returns {string} The tenant name.
   */
  static get tenantName() {
    return BiosSession._default.tenantName;
  }

  /**
   * Provides the user's information.
   *
   * @returns {object} User info
   */
  static async getUserInfo() {
    return this._getGlobalSession().getUserInfo();
  }

  /**
   * Invite a user in a tenant
   *
   * @param {object} request - User invitation request object (required).
   * @property {string} request.email - Email address (required).
   * @property {array<string>} request.roles - Roles (required).
  */
  static async inviteUser(request) {
    return BiosSession._default.inviteUser(request);
  }

  /**
   * Create a tenant.
   *
   * @param {object} tenantConfig - Tenant configuration object (required).
   * @property {string} tenantConfig.tenantName - Tenant name (required).
   * @param {boolean} registerApp - Registers and deploys biosApps if true
   * @returns {Promise<void>}
   */
  static async createTenant(tenantConfig, registerApps) {
    return BiosSession._default.createTenant(tenantConfig, registerApps);
  }

  /**
   * Lists tenant names.
   *
   * @param {object} options - Options (optional)
   * @property {array<string>} options.names - Tenant names to list
   * @returns {Promise<array<object>>} Promise to the tenant objects with properties
   *     tenantName and version.
   */
  static async listTenantNames(options) {
    return BiosSession._default.listTenantNames(options);
  }

  /**
   * Deletes a tenant.
   *
   * @param {string} tenantName - Name of the tenant to delete.
   * @returns {Promise<void>}
   */
  static async deleteTenant(tenantName) {
    return BiosSession._default.deleteTenant(tenantName);
  }

  /**
   * Gets a tenant configuration.
   *
   * @param {object} options - Request options (optional).
   * @property {boolean} detail - If true, the method returns details of the tenant configuration
   * @property {boolean} options.inferredTags - If true, the method includes inferred tags
   *    in attributes
   * @returns {Promise<object>} Promise to the tenant configuration.
   */
  static async getTenant(options) {
    return BiosSession._default.getTenant(options);
  }

  /**
   * Creates a signal.
   *
   * @param {object} signalConfig - Signal configuration to create.
   * @returns {Promise<object>} Promise to the created signal configuration.
   */
  static async createSignal(signalConfig) {
    return BiosSession._default.createSignal(signalConfig);
  }

  /**
   * Gets signals.
   *
   * @param {object} options Object that specifies optional parameters.
   * @property {list<string>} options.signals Signals to retrieve. The method would return all
   *    signals if not specified.
   * @property {boolean} options.detail If true, the method returns full content of signals,
   *    otherwise the method returns only the property signalName. (_default=false)
   * @property {boolean} options.includeInternal - If true, the method includes
   *    internal signals in the reply. (_default=false)
   * @property {boolean} options.inferredTags If true, the method includes inferred tags
   *    in attributes
   * @returns {Promise.<object>} A promise to the list of signal objects.
   * @throws {BiosClientError} when the operation fails.
   */
  static async getSignals(options) {
    return BiosSession._default.getSignals(options);
  }

  /**
   * Gets a signal configuration.
   *
   * @param { string } signalName - The name of the signal.
   * @param {object} options Object that specifies optional parameters.
   * @property {boolean} options.inferredTags If true, the method includes inferred tags
   *    in attributes
   */
  static async getSignal(signalName, options) {
    if (!!options) {
      requiresParam(options, 'options', 'object');
      if (options.inferredTags !== undefined && options.inferredTags !== null) {
        requiresProperty(options, 'options', 'inferredTags', 'boolean');
      }
    } else {
      options = {};
    }
    const reply = await BiosSession.getSignals({
      ...options,
      detail: true,
      signals: [signalName]
    });
    return reply.length > 0 ? reply[0] : {};
  }

  /**
   * Updates a signal configuration.
   *
   * @param {string} signalName - Name of the signal to update.
   * @param {object} signalConfig - Signal configuration to create.
   * @returns {Promise<object>} Promise to the modified signal configuration.
   */
  static async updateSignal(signalName, signalConfig) {
    return BiosSession._default.updateSignal(signalName, signalConfig);
  }

  /**
   * Deletes a signal.
   *
   * @param {string} signalName - Name of the signal to delete.
   */
  static async deleteSignal(signalName) {
    return BiosSession._default.deleteSignal(signalName);
  }

  /**
   * Creates a context.
   *
   * @param {object} contextConfig - Context configuration to create.
   * @returns {Promise<object>} Promise to the created signal configuration.
   */
  static async createContext(contextConfig) {
    return BiosSession._default.createContext(contextConfig);
  }

  /**
   * Gets contexts.
   *
   * @param {object} options - Object that specifies optional parameters.
   * @property {list<string>} options.contexts - Context names to retrieve. The method would
   *    return all contexts if not specified.
   * @property {boolean} options.detail - If true, the method returns full content of signals,
   *    otherwise the method returns only the property signalName. (_default=false)
   * @property {boolean} options.includeInternal - If true, the method includes
   *    internal contexts in the reply. (_default=false)
   * @property {boolean} options.inferredTags - If true, the method includes inferred tags
   *    in attributes
   * @returns {Promise.<object>} A promise to the list of context configuration objects.
   * @throws {BiosClientError} when the operation fails.
   */
  static async getContexts(options) {
    return BiosSession._default.getContexts(options);
  }

  /**
   * Gets a context configuration.
   *
   * @param { string } contextName - The name of the context.
   * @param {object} options - Object that specifies optional parameters.
   * @property {boolean} options.inferredTags - If true, the method includes inferred tags
   *    in attributes
   * @returns {Promise.<object>} A promise to the context configuration object.
   */
  static async getContext(contextName, options) {
    if (!!options) {
      requiresParam(options, 'options', 'object');
      if (options.inferredTags !== undefined && options.inferredTags !== null) {
        requiresProperty(options, 'options', 'inferredTags', 'boolean');
      }
    } else {
      options = {};
    }
    const reply = await BiosSession.getContexts({
      ...options,
      detail: true,
      contexts: [contextName]
    });
    return reply.length > 0 ? reply[0] : {};
  }

  /**
   * Updates a context configuration.
   *
   * @param {string} contextName - Name of the context to update.
   * @param {object} contextConfig - Signal configuration to create.
   * @returns {Promise<object>} Promise to the modified context configuration.
   */
  static async updateContext(contextName, contextConfig) {
    return BiosSession._default.updateContext(contextName, contextConfig);
  }

  /**
   * Deletes a context.
   *
   * @param {string} contextName - Name of the context to delete.
   */
  static async deleteContext(contextName) {
    return BiosSession._default.deleteContext(contextName);
  }

  /**
   * Returns a statement builder.
   *
   * @returns {ISqlStatementBuilder} ISqlStatement builder.
   */
  static iSqlStatement() {
    return new ISqlStatementBuilder();
  }

  /**
   * Returns a statement helper.
   *
   * @returns {ISqlStatementHelper} ISqlStatement builder.
   */
  static get isql() {
    return ISqlStatementHelper;
  }

  /**
   * Method to execute a statement.
   * @param {ISqlRequest} statement - SELECT, INSERT, UPSERT, UPSERT, or DELETE statement
   * @returns {Promise.<ISqlResponse>} - Promise to the list of responses.
   *
   * Following is an example of returned data format based on different type of query:
   *
   * 1. SELECT Query Response for Signal:
   *
   * @returns {Promise.<SelectISqlResponse>} - Promise to the list of responses of instance type SelectISqlResponse
   *
   * <pre><code>
   *  {
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
   * </code></pre>
   * Note : For non GLOBAL_WINDOW query, dataWindows.records will be a like:
   *  records = [{
   *      eventId,
   *      timestamp,
   *      values,
   *  }]
   *
   * 2. SELECT Query Response for context:
   *
   * @returns {Promise.<SelectContextISqlResponse>} - Promise to the list of responses of instance type SelectContextISqlResponse
   *
   * <pre><code>
   *  {
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
   * </code></pre>
   *
   * 3. Response for UPSERT, DELETE, UPDATE query is an instance of VoidISqlResponse:
   *
   * @returns {Promise.<VoidISqlResponse>} - Promise to the list of responses of instance type VoidISqlResponse
   *
   * {"responseType": "VOID"}
   *
   * 4. Response for INSERT Query
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
   *
   */
  static async execute(statement) {
    return BiosSession._default.execute(statement);
  };

  /**
   * Method to execute multiple SELECT statements.
   * @param {list<ISqlRequest>} statements - List of SELECT statements.
   * @returns {Promise.list<ISqlResponse>} - Promise to the list of responses.
   *
   * Note: Response depends on type of query and will be Array of execute query responses.
   * For more info, check out individual execute query.
   */
  static async multiExecute(...statements) {
    return BiosSession._default.multiExecute(...statements);
  };

  /**
   *
   * @param {object} options           - Object to carry options
   * @property {string} options.detail - Returns all report properties if true, the least set otherwise.
   *                                     The least set properties are reportId and reportName.
   * @property {string|list<string>}
   *                  options.reportId - Specifies explicit report ID(s) to fetch (_default: all)
   *
   * FOLLOWS ARE SUPPORTED IN FUTURE
   * @property {number} options.limit  - Limits number of entries to return (_default: no limitation)
   * @property {string} options.nextOf - Used for paging, the method returns entries from the next of the
   *                                     specified ID.
   *
   * @returns {object} Metadata and reports
   *   e.g.
   *     {
   *       more: true, // FUTURE: used for paging
   *       warning: "Name 'revenueInUSD' is used by multiple reports. Please consider renaming",
   *       reports: [
   *         {
   *           reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
   *           reportName: 'Revenue in USD',
   *           metrics: [
   *             {
   *               measurement: 'orders.sum(price)',
   *               as: 'Revenue/売上高'
   *             }, {
   *               measurement: 'customers.min(age)',
   *               as: 'Minimum Age'
   *             }, {
   *               measurement: 'customers.max(age)',
   *               as: 'Maximum Age'
   *             }
   *           ],
   *           dimensions: ['country', 'state'],
   *           defaultTimeRange: 604800000,
   *           defaultWindowLength: 43200000,
   *         }
   *       ]
   *     }
   */
  static async getReportConfigs(options) {
    return BiosSession._default.getReportConfigs(options);
  }

  /**
   * The method puts a report config to the backbone storage. If the backbone storage has an entry with
   * the same ID already, the method overwrites it.
   *
   * Example usage:
   *   bios.putReportConfig({
   *     reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
   *     reportName: 'Revenue in USD',
   *     metrics: [
   *       {
   *         measurement: 'orders.sum(price)',
   *         as: 'Revenue/売上高'
   *       },
   *       {
   *         measurement: 'customers.min(age)',
   *         as: 'Minimum Age'
   *       },
   *       bios.metric('customers', 'min(stay_length)', 'minimum Stay Length')
   *     ],
   *     dimensions: ['country', 'state'],
   *     filters: [
   *       {
   *         filterName: 'filter by color',
   *         values: [''],
   *       }
   *     ],
   *     defaultStartTime: bios.time.now(), // optional
   *     defaultTimeRange: 604800000,
   *     defaultWindowLength: 43200000,
   *   });
   *
   * @param {object} config - Report config to put. Required properties are listed as follows.
   *                          Other properties are acceptable; they are stored as is (e.g. vizConfig).
   *
   * @property {string} config.reportId            - Report ID (UUID)
   * @property {string} config.reportName          - Report name (display name)
   * @property {list<metric>} config.metrics       - Report metrics (has to include signals)
   * @property {string} metric.measurement - Measurement description, e.g. 'clicks.count()'
   * @property {string} metric.as          - Display name
   * @property {list<string>} config.dimensions    - Report dimensions
   * @property {list<filter>} config.filters       - Filters
   * @property {string} filter.filterName  - Filter name
   * @property {list<string>} filter.values - Filter values
   * @property {number} config.defaultStartTime - Default start time of the report (milliseconds since Epoch)
   * @property {number} config.defaultTimeRange - Default time range for the report (milliseconds)
   * @property {number} config.defaultWindowLength - Default window length for the report (ms)
   */
  static async putReportConfig(config) {
    return BiosSession._default.putReportConfig(config);
  }

  static async deleteReportConfig(reportId) {
    return BiosSession._default.deleteReportConfig(reportId);
  }

  /**
   * Returns the user's insight configuration objects
   *
   * @param {string} insightName - The name of the insight
   * @returns {object} Metadata and insights
   *   e.g.
   *   {
   *     sections: [
   *       {
   *         timeRange: 604800000,
   *         insightConfigs: [
   *           {
   *             insightId: 'd9063f0f-5f03-4979-82b7-e07796b0ad2d',
   *             reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
   *           }, {
   *             insightId: '834857fe-56ce-4fbf-99d7-91ac50f22fb3',
   *             reportId: '2e4146fe-e71a-4f06-8fdf-ad359cbc161d',
   *           },
   *         ],
   *       },
   *     ],
   *   }
   */
  static async getInsightConfigs(insightName) {
    requiresParam(insightName, 'insightName', 'string');
    return BiosSession._default.getInsightConfigs(insightName);
  }

  /**
   * The method puts an insight config to the backbone storage. If the backbone storage has an entry with
   * the same ID already, the method overwrites it.
   *
   * Example usage:
   *   bios.putInsightConfigs('signal', {
   *     sections: [
   *       {
   *         sectionId: '74bddeac-cd3d-4d7d-8352-96b06b85b2be',
   *         timeRange: 604800000,
   *         insightConfigs: [
   *           {
   *             insightId: '2db598e5-3df1-4be6-b363-a7cd173399c1',
   *             reportId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
   *           },
   *         ],
   *       },
   *     ],
   *   });
   *
   * @param {string} insightName - The name of the insight
   * @param {object} insightConfigs - The configuration to put
   * @property {list<section>} sections
   * @property {string} section.sectionId - Unique ID within the user
   * @property {number} section.timeRange - (optional) Time range of the insights used in this
   *                                        section
   * @property {list<insight>} section.insightConfigs - Insight configs
   * @property {string} insight.insightId - Insight ID
   * @property {string} insight.reportId - Report ID
   */
  static async putInsightConfigs(insightName, insightConfigs) {
    requiresParam(insightName, 'insightName', 'string');
    requiresParam(insightConfigs, 'insightConfigs', 'object');
    return BiosSession._default.putInsightConfigs(insightName, insightConfigs);
  }

  /**
   * Deletes insight configs.
   *
   * @param {string} insightName - The name of the insight configurations to delete
   */
  static async deleteInsightConfigs(insightName) {
    requiresParam(insightName, 'insightName', 'string');
    return BiosSession._default.deleteInsightConfigs(insightName);
  }

  /**
   * Method to generate insights summaries data.
   *
   * Following is an example of method call:
   *
   * <pre>
   * const summaries = getInsightSummaries([
   *   {
   *     insightId: 'd9063f0f-5f03-4979-82b7-e07796b0ad2d',
   *     metric: 'orders.sum(price)',
   *     originTime: bios.time.now(),
   *     delta: bios.time.days(-7),
   *     snapStepSize: bios.time.hours(1),
   *   }, {
   *     insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
   *     metric: {
   *       measurement: 'orders.sum(price)',
   *     },
   *     originTime: bios.time.now(),
   *     delta: bios.time.days(-30),
   *     snapStepSize: bios.time.hours(4),
   *   }, {
   *     insightId: '0039d69e-970b-4798-bd6b-b29fab7e8207',
   *     metric: {
   *       measurement: 'clicks.count()',
   *     },
   *     originTime: bios.time.now(),
   *     delta: bios.time.minutes(-1),
   *     snapStepSize: bios.time.minutes(1),
   *   }
   * ]);
   * </pre>
   *
   * Following is an example of returned value:
   * <pre>
   * {
   *   insights: [
   *     {
   *       insightId: 'd9063f0f-5f03-4979-82b7-e07796b0ad2d',
   *       value: 170256,
   *       timestamp: 1592628113000,
   *       trend: 23.5,
   *     }, {
   *       insightId: 'c8949cff-2b1b-4e59-b119-9fbec1aa28b7',
   *       value: 392558,
   *       timestamp: 1592628113000,
   *     }, {
   *       insightId: '0039d69e-970b-4798-bd6b-b29fab7e8207',
   *       value: null,
   *       timestamp: null,
   *     }
   *   ]
   * }
   * </pre>
   *
   * @param {list<insightRequest>} insightRequests - Insight info that consists of following:
   * @property {string} insightRequest.insightId     - Insight ID
   * @property {string|metric} insightRequest.metric - The metric that is used for the summary value
   * @property {string} insightRequest.metric.measurement - Metric measurement, e.g., 'clicks.count()'
   * @property {number} insightRequest.originTime    - Time origin to collect the metrics in milliseconds
   * @property {number} insightRequest.delta         - Time delta to collect the metrics in milliseconds
   * @property {number} insightRequest.snapStepSize  - Step size in milliseconds to snap the query start time
   * @return {list<object>} List of insights info. If the server does not have data for the
   *   specified time range, null values are returned for unknown data.
   */
  static async getInsights(insightRequests) {
    return BiosSession._default.getInsights(insightRequests);
  }

  static async getSignalSynopsis(signalName, currentTime, recallTimeMs, windowSizeMs) {
    return BiosSession._default.getSignalSynopsis(signalName, currentTime, recallTimeMs, windowSizeMs);
  }

  static async getAttributeSynopsis(signalName, attributeName, currentTime, recallTime, windowSizeMs) {
    return BiosSession._default.getAttributeSynopsis(signalName, attributeName, currentTime, recallTime, windowSizeMs);
  }

  /**
   * Gets synopses of all contexts.
   *
   * @returns {object} All context synopses.
   */
  static async getAllContextSynopses() {
    return BiosSession._default.getAllContextSynopses();
  }

  /**
   * Gets synopsis of a context.
   *
   * @param {string} contextName - Target context name
   * @returns {object} Context synopsis.
   */
  static async getContextSynopsis(contextName) {
    return BiosSession._default.getContextSynopsis(contextName);
  }

  static async getAvailableMetrics2() {
    return BiosSession._default.getAvailableMetrics2();
  }

  static async getSupportedTags() {
    return BiosSession._default.getSupportedTags();
  }

  /**
   * Create an import source.
   *
   * @param {object} importSourceConfig Import source configuration object
   * @returns Registered import configuration object. The object includes assigned source ID.
   *     Use this ID for further access to the config entry.
   */
  static async createImportSource(importSourceConfig) {
    requiresParam(importSourceConfig, 'importSourceConfig', 'object');
    const reply = await BiosSession._default.createTenantAppendix(
      TenantAppendixCategory.IMPORT_SOURCES, importSourceConfig, importSourceConfig.importSourceId);
    return reply.content;
  }

  /**
   * Get an import source configuration entry.
   *
   * For fetching available source configurations in bulk, use getTenant API.
   *
   * @param {string} id - Import source ID
   * @returns Specified configuration object.
   */
  static async getImportSource(id) {
    requiresParam(id, 'id', 'string');
    const reply = await BiosSession._default.getTenantAppendix(
      TenantAppendixCategory.IMPORT_SOURCES, id);
    return reply.content;
  }

  /**
   * Update an import source configuration entry.
   *
   * @param {string} id - Import source ID
   * @param {object} importSourceConfig
   * @returns Modified configuration object.
   */
  static async updateImportSource(id, importSourceConfig) {
    requiresParam(id, 'id', 'string');
    requiresParam(importSourceConfig, 'importSourceConfig', 'object');
    const reply = await BiosSession._default.updateTenantAppendix(
      TenantAppendixCategory.IMPORT_SOURCES, importSourceConfig, id);
    return reply.content;
  }

  /**
   * Delete an import source configuration entry.
   *
   * @param {string} id - Import source ID
   */
  static async deleteImportSource(id) {
    requiresParam(id, 'id', 'string');
    return BiosSession._default.deleteTenantAppendix(
      TenantAppendixCategory.IMPORT_SOURCES, id);
  }

  /**
   * Create an import destination.
   *
   * For fetching available destination configurations in bulk, use getTenant API.
   *
   * @param {object} importDestinationConfig - Import destination configuration object
   * @returns Created import configuration. The configuration has assigned ID. Use
   *     the id for further access to the entry.
   */
  static async createImportDestination(importDestinationConfig) {
    requiresParam(importDestinationConfig, 'importDestinationConfig', 'object');
    const reply = await BiosSession._default.createTenantAppendix(
      TenantAppendixCategory.IMPORT_DESTINATIONS,
      importDestinationConfig,
      importDestinationConfig.importDestinationId
    );
    return reply.content;
  }

  /**
   * Get configuration of an import destination.
   *
   * For fetching available destination configurations in bulk, use getTenant API.
   *
   * @param {string} id - Configuration entry ID
   */
  static async getImportDestination(id) {
    requiresParam(id, 'id', 'string');
    const reply = await BiosSession._default.getTenantAppendix(
      TenantAppendixCategory.IMPORT_DESTINATIONS, id);
    return reply.content;
  }

  /**
   * Update an import destination configuration entry.
   *
   * @param {string} id - Import destination ID
   * @param {object} importDestinationConfig
   * @returns Modified configuration object.
   */
  static async updateImportDestination(id, importDestinationConfig) {
    requiresParam(id, 'id', 'string');
    requiresParam(importDestinationConfig, 'importDestinationConfig', 'object');
    const reply = await BiosSession._default.updateTenantAppendix(
      TenantAppendixCategory.IMPORT_DESTINATIONS, importDestinationConfig, id);
    return reply.content;
  }

  /**
   * Delete an import destination.
   *
   * @param {string} id - Configuration entry ID to be deleted
   */
  static async deleteImportDestination(id) {
    requiresParam(id, 'id', 'string');
    return BiosSession._default.deleteTenantAppendix(
      TenantAppendixCategory.IMPORT_DESTINATIONS, id);
  }

  /**
   * Create an import flow.
   *
   * @param {object} importFlowSpec - Import flow specification
   * @returns Created import flow configuration entry. The entry has assigned ID. Use the ID for
   *     further access to the entry.
   */
  static async createImportFlowSpec(importFlowSpec) {
    requiresParam(importFlowSpec, 'importFlowSpec', 'object');
    const reply = await BiosSession._default.createTenantAppendix(
      TenantAppendixCategory.IMPORT_FLOW_SPECS, importFlowSpec, importFlowSpec.importFlowId);
    return reply.content;
  }

  /**
   * Get an import flow.
   *
   * For fetching available import flow configurations in bulk, use getTenant API.
   *
   * @param {string} id - Import flow ID
   * @returns Specified import flow configuration entry
   */
  static async getImportFlowSpec(id) {
    requiresParam(id, 'id', 'string');
    const reply = await BiosSession._default.getTenantAppendix(
      TenantAppendixCategory.IMPORT_FLOW_SPECS, id);
    return reply.content;
  }

  /**
   * Update an import flow.
   *
   * @param {string} id - Import flow ID
   * @param {object} importFlowSpec - Import flow specification
   * @returns Updated import flow configuration entry
   */
  static async updateImportFlowSpec(id, importFlowSpec) {
    const reply = await BiosSession._default.updateTenantAppendix(
      TenantAppendixCategory.IMPORT_FLOW_SPECS, importFlowSpec, id);
    return reply.content;
  }

  /**
   * Delete an import flow.
   *
   * @param {number} id Import flow ID
   */
  static async deleteImportFlowSpec(id) {
    return BiosSession._default.deleteTenantAppendix(
      TenantAppendixCategory.IMPORT_FLOW_SPECS, id);
  }

  /**
   * Create an import data processor source code in Python.
   *
   * @param {string} name - Processor name
   * @param {string} code - Source code string
   */
  static async createImportDataProcessor(name, code) {
    requiresParam(name, 'name', 'string');
    requiresParam(code, 'code', 'string');
    const appendix = {
      processorName: name,
      encoding: 'Base64',
      code: Buffer.from(code).toString('base64')
    };
    return BiosSession._default.createTenantAppendix(
      TenantAppendixCategory.IMPORT_DATA_PROCESSORS, appendix, name
    );
  }

  /**
   * Get an import data processor source in Python.
   *
   * For fetching available processor names in bulk, use getTenant API.
   *
   * @param {string} name - Processor name
   * @returns Specified source code
   */
  static async getImportDataProcessor(name) {
    requiresParam(name, 'name', 'string');
    const reply = await BiosSession._default.getTenantAppendix(
      TenantAppendixCategory.IMPORT_DATA_PROCESSORS, name
    );
    return Buffer.from(reply.content.code, 'base64').toString();
  }

  /**
   * Update an import data processor source code in Python.
   *
   * @param {string} name - Processor name
   * @param {string} code - Source code string
   */
  static async updateImportDataProcessor(name, code) {
    requiresParam(name, 'name', 'string');
    requiresParam(code, 'code', 'string');
    const appendix = {
      processorName: name,
      encoding: 'Base64',
      code: Buffer.from(code).toString('base64')
    };
    return BiosSession._default.updateTenantAppendix(
      TenantAppendixCategory.IMPORT_DATA_PROCESSORS, appendix, name
    );
  }

  /**
   * Delete an import processor source in Python.
   *
   * @param {string} name - Processor name
   */
  static async deleteImportDataProcessor(name) {
    requiresParam(name, 'name', 'string');
    return BiosSession._default.deleteTenantAppendix(
      TenantAppendixCategory.IMPORT_DATA_PROCESSORS, name
    );
  }

  /**
   * Create an export destination.
   *
   * @param {object} - exportDestinationConfig Export destination configuration object
   * @returns Registered export destination configuration object
   */
  static async createExportDestination(exportDestinationConfig) {
    requiresParam(exportDestinationConfig, 'exportDestinationConfig', 'object');
    requiresProperty(
      exportDestinationConfig, 'exportDestinationConfig', 'exportDestinationName', 'string');
    return BiosSession._default.createExportDestination(exportDestinationConfig);
  }

  /**
   * Get an export destination configuration entry.
   *
   * For fetching available destination configurations in bulk, use getTenant API.
   *
   * @param {string} name - export destination name
   * @returns Specified configuration object.
   */
  static async getExportDestination(name) {
    requiresParam(name, 'name', 'string');
    return BiosSession._default.getExportDestination(name);
  }

  /**
   * Update an export destination configuration entry.
   *
   * @param {string} name - export destination name
   * @param {object} exportDestinationConfig
   * @returns Modified configuration object.
   */
  static async updateExportDestination(name, exportDestinationConfig) {
    requiresParam(name, 'name', 'string');
    requiresParam(exportDestinationConfig, 'exportDestinationConfig', 'object');
    return BiosSession._default.updateExportDestination(name, exportDestinationConfig);
  }

  /**
   * Delete an export destination configuration entry.
   *
   * @param {string} name - export destination name
   */
  static async deleteExportDestination(name) {
    requiresParam(name, 'name', 'string');
    return BiosSession._default.deleteExportDestination(name);
  }

  /**
   * Discovers schema from an import source.
   *
*
   * Example output:
   * ```
   * {
   *   importSourceName: 'Kafka event provider',
   *   importSourceId: '210',
   *   subjects: [
   *     {
   *       subjectName: 'events',
   *       subjectType: 'Topic',
   *       payloadType: 'Csv',
   *       sourceAttributes: [
   *         {
   *           sourceAttributeName: 'column1',
   *           originalType: null,
   *           suggestedType: 'String',
   *           isNullable: false,
   *           exampleValue: '25/06/2020'
   *         },
   *         {
   *           sourceAttributeName: 'column2',
   *           originalType: null,
   *           suggestedType: 'String',
   *           isNullable: false,
   *           exampleValue: 'AF'
   *         },
   *         {
   *           sourceAttributeName: 'column3',
   *           originalType: null,
   *           suggestedType: 'Integer',
   *           isNullable: false,
   *           exampleValue: '234'
   *         },
   *         {
   *           sourceAttributeName: 'column4',
   *           originalType: null,
   *           suggestedType: 'Decimal',
   *           isNullable: false,
   *           exampleValue: '21.7'
   *         }
   *       ]
   *     }
   *   ]
   * }
   * ```
   *
   * @param {string} importSourceId - Import source ID
   * @param {number} timeout - (optional, default=60) Discovery timeout in seconds. The server
   *   stops the source schema discovery after the timeout seconds. The potential subjects
   *   where the schema could not have been resolved are reported in the warnings field.
   * @returns {object} Discovered schema
   */
  static async discoverImportSubjects(importSourceId, timeout) {
    requiresParam(importSourceId, 'importSourceId', 'string');
    if (timeout !== undefined && timeout !== null) {
      if (!Number.isInteger(timeout) || timeout <= 0) {
        throw new InvalidArgumentError(`Parameter 'timeout' value must be a positive integer`);
      }
    }
    return BiosSession._default.discoverImportSubjects(importSourceId, timeout);
  }

  /**
   * Changes a user's password.
   *
   * This method changes the user's own password. TenantAdmin user also can change a password of
   * another user in the tenant.
   *
   * Parameters are specified with an object. In case of changing the user's own password,
   * properties 'currentPassword' and 'newPassword' are required to be set. In case of changing
   * another user's password, properties 'currentPassword' and 'email' are required.
   *
   * @param {object} request Request object
   * @property {string} request.newPassword - New password -- always required
   * @property {string} request.currentPassword - The user's current password -- required for self password change
   * @property {string} request.email - The target user's email address -- required for cross-user password change
   */
  static async changePassword(request) {
    await BiosSession._default.changePassword(request);
  }

  /**
   * Creates a user.
   *
   * @param {object} user - User configuration
   * @property {string} user.email - Email address (required)
   * @property {string} user.password - Password (required)
   * @property {string} user.tenantName - Tenant name (required)
   * @property {string} user.fullName - Full name of the user (required)
   * @property {list<string>} user.roles - List of user roles (optional), possible values are:
   *    SystemAdmin, TenantAdmin, SchemaExtractIngest, Extract, Ingest, and Report
   * @property {string} user.status - User status (optional), possible values are:
   *    Active and Suspended
   * @returns {object} Configuration of the created user
   */
  static async createUser(user) {
    return BiosSession._default.createUser(user);
  }

  /**
   * Gets all users in the tenant.
   *
   * @return {list<object>} List of user configurations
   */
  static async getUsers() {
    return BiosSession._default.getUsers();
  }

  /**
   * Modify a user.
   *
   * Only the properties that are included in the request user configuration are modified.
   *
   * @param {object} user - User configuration
   * @property {string} user.userId - User ID (required)
   * @property {string} user.fullName - User's full name (required)
   * @property {list<string>} user.roles - List of user roles (optional), possible values are:
   *    SystemAdmin, TenantAdmin, SchemaExtractIngest, Extract, Ingest, and Report
   * @property {string} user.status - User status (optional), possible values are:
   *    Active and Suspended
   * @returns {object} Configuration of the modified user
   */
  static async modifyUser(user) {
    return BiosSession._default.modifyUser(user);
  }

  /**
   * Delete a user.
   *
   * @param {object} user - User configuration. Either of properties email or id is required
   * @property {string} user.email - Email address
   * @property {string} user.userId - User ID
   */
  static async deleteUser(user) {
    return BiosSession._default.deleteUser(user);
  }

  /**
   * Registers biOS Apps service for a tenant.
   *
   * @param {object} appsInfo - Apps information
   * @property {string} tenantName - Tenant name (required).
   * @property {list<string>} hosts - Host name where the biOS Apps service runs (optional).
   * @property {number} controlPort - The biOS Apps control port number (optional).
   *     The server allocates an available one if omitted.
   * @property {number} webhookPort - The biOS Apps webhook port number (optional).
   *     The server allocates an available one if omitted.
   * @returns Registered biOS Apps information.
   */
  static async registerAppsService(appsInfo) {
    return BiosSession._default.registerAppsService(appsInfo);
  }

  /**
   * Gets biOS Apps service information for a tenant.
   *
   * @param {string} tenantName - Tenant name
   * @returns The biOS Apps information for the specified tenant.
   */
  static async getAppsInfo(tenantName) {
    return BiosSession._default.getAppsInfo(tenantName);
  }

  /**
   * Deregisters biOS Apps service for a tenant.
   *
   * @param {string} tenantName - Tenant name
   */
  static async deregisterAppsService(tenantName) {
    return BiosSession._default.deregisterAppsService(tenantName);
  }

  /**
   * Sets a shared property value.
   *
   * @param {string} key - Property key
   * @param {string} value - Property value
   */
  static async setProperty(key, value) {
    return this._default.setProperty(key, value);
  }

  /**
   * Gets a shared property value.
   *
   * @param {string} key - Property key
   * @returns The property value. If the specified property is not set, an empty string is returned.
   */
  static async getProperty(key) {
    return this._default.getProperty(key);
  }

  /**
   * Initiates a user signup process.
   *
   * @param {string} email - Email address of new user
   * @param {string} source - Traffic source (optional)
   * @property {string} config.gcpMarketplaceToken - GCP Marketplace Token (optional)
   */
  static async initiateSignup(email, source, gcpMarketplaceToken) {
    requiresParam(email, 'email', 'string');
    if (!!source) {
      requiresParam(source, 'sid', 'string');
    }
    if (!!gcpMarketplaceToken) {
      requiresParam(gcpMarketplaceToken, 'gcpMarketplaceToken', 'string');
    }
    return this._getGlobalSession().initiateSignup(email, source, gcpMarketplaceToken);
  }

  /**
   * Initiates a service registration process.
   *
   * @param {string} primaryEmail - Email address of the primary user
   * @param {string} service - Service name
   * @param {string} source - Traffic source
   * @param {string} domain - Traffic source
   * @param {list<string>} additionalEmails - (optional) Email addresses of additional users
   */
  static async initiateServiceRegistration(primaryEmail, service, source, domain,
    additionalEmails) {
    requiresParam(primaryEmail, 'primaryEmail', 'string');
    requiresParam(service, 'service', 'string');
    requiresParam(source, 'string', 'string');
    requiresParam(domain, 'domain', 'string');
    if (additionalEmails !== undefined) {
      requiresParam(additionalEmails, 'additionalEmails', 'array');
    }
    return this._getGlobalSession().initiateServiceRegistration(primaryEmail, service, source,
      domain, additionalEmails);
  }

  /**
   * Verifies a signup token.
   *
   * @param {string} token - Signup token
   */
  static async verifySignupToken(token) {
    requiresParam(token, 'token', 'string');
    return this._getGlobalSession().verifySignupToken(token);
  }

  /**
   * Completes signing up.
   *
   * @param {string} token - Signup token
   * @param {string} password - Password to register
   * @param {string} username - User name
   */
  static async completeSignup(token, password, username) {
    requiresParam(token, 'token', 'string');
    requiresParam(password, 'password', 'string');
    if (username !== null && username !== undefined) {
      requiresParam(username, 'username', 'string');
    } else {
      username = '';
    }
    return this._getGlobalSession().completeSignup(token, password, username);
  }

  /**
   * Request to reset password.
   *
   * @param {string} email - Email address of the user
   */
  static async initiateResetPassword(email) {
    requiresParam(email, 'email', 'string');
    return this._getGlobalSession().initiateResetPassword(email);
  }

  /**
   * Completes resetting password.
   *
   * @param {string} token - Auth token
   * @param {string} password - New password
   */
  static async completeResetPassword(token, password) {
    requiresParam(token, 'token', 'string');
    requiresParam(password, 'password', 'string');
    return this._getGlobalSession().completeResetPassword(token, password);
  }

  /**
   * Converts a role to its name.
   *
   * @param role Role
   * @returns Name
   */
  static getRoleName(role) {
    return roleToName(role);
  }

  /**
   * Get the status of a feature - time until when it has been updated.
   *
   * @param {string} stream - the stream the feature belongs to.
   * @param {string} feature - (optional) the feature to be checked.
   * @returns featureStatusResponse containing:
   *          doneSince (integer): not applicable to context features
   *          doneUntil (integer): time upto which the feature has been updated.
   *          refreshRequested (boolean): whether a pending refresh has been requested.
   */
  static async featureStatus(stream, feature) {
    requiresParam(stream, 'stream', 'string');
    if (feature !== undefined) {
      requiresParam(feature, 'feature', 'string');
    }
    return BiosSession._default.featureStatus({ stream, feature });
  }

  /**
   * Request refreshing a feature - to bring it up-to-date.
   *
   * @param {string} stream - the stream the feature belongs to.
   * @param {string} feature - (optional) the feature to be checked.
   * @returns Nothing.
   */
  static async featureRefresh(stream, feature) {
    requiresParam(stream, 'stream', 'string');
    if (feature !== undefined) {
      requiresParam(feature, 'feature', 'string');
    }
    return BiosSession._default.featureRefresh({ stream, feature });
  }

  static async teachBios(rows) {
    return BiosSession._default.teachBios(rows);
  }

  // Use this only for server test
  static async getContextEntries(contextName, primaryKeys) {
    return BiosSession._default.getContextEntries(contextName, primaryKeys);
  }

  // Use this only for server test
  static async putContextEntries(contextName, entries) {
    return BiosSession._default.putContextEntries(contextName, entries);
  }

  // Use this only for server test
  static async updateContextEntry(contextName, primaryKey, attributes) {
    return BiosSession._default.updateContextEntry(contextName, primaryKey, attributes);
  }

  // Use this only for server test
  static async deleteContextEntries(contextName, primaryKeys) {
    return BiosSession._default.deleteContextEntries(contextName, primaryKeys);
  }

  // Use this only for server test
  static async replaceContextAttributes(contextName, attributeName, oldValue, newValue) {
    return BiosSession._default.replaceContextAttributes(
      contextName, attributeName, oldValue, newValue);
  }

  // Utilities /////////////
  /**
   * Helper function to generate a metric object.
   *
   * @param {string} signal - signal name
   * @param {string} metric - metric description
   * @param {string} [as] -
   */
  static metric(signal, metric, as) {
    requiresParam(signal, 'signal', 'string');
    requiresParam(metric, 'metric', 'string');
    const out = {
      measurement: `${signal}.${metric}`,
    };
    validateMeasurement(out.measurement);

    if (as !== undefined) {
      requiresParam(as, 'as', 'string');
      out.as = as;
    }
    return out;
  }

  static _require(target, name, type) {
    if (typeof (target) !== type) {
      throw new Error(`Parameter ${name} of type ${type} must be specified`);
    }
  }

  static _requireProperty(target, targetName, name, type) {
    if (typeof (target[name]) !== type) {
      throw new Error(`Required property ${name} of type ${type} is missing in targetName`);
    }
  }
}

class GlobalBiosClient {
  static async login(config) {
    _globalSession = await BiosSession.login(config);
    return _globalSession;
  }

  static async bindAxios(axios) {
    _globalSession = await HttpImpl.bind(axios);
    return _globalSession;
  }
}
