import axios from 'axios';

import { BIOS_VERSION } from '../../version';
import codec from '../codec';
import {
  BiosClientError,
  BiosErrorType,
  handleErrorResponse,
  InvalidArgumentError
} from '../common/errors';
import {
  debugLog,
  isDebugLogEnabled,
  requiresParam,
  requiresProperty,
  timeUtils,
  validateMetric,
  validateNonEmptyString
} from '../common/utils';
import {
  InsertISqlResponse,
  ISqlStatementBuilder,
  SelectContextISqlResponse,
  StatementType,
  VoidISqlResponse
} from '../isql';

const INGEST_BULK_BATCH_SIZE = 1000;

export default class BiosSessionHttp {
  /**
   * Creates an instance of BiosSession. But we should not call this directly.
   * Use method login to initiate a session.
   */
  constructor(httpClient, setInterceptor) {
    this._httpClient = httpClient;
    if (setInterceptor) {
      httpClient.interceptors.request.use(
        (config) => {
          const { tenant, email } = this._userInfo || {};
          if (email) {
            config.headers['x-user-name'] = Buffer.from(email, 'utf8').toString('base64');
          }
          if (tenant) {
            config.headers['x-tenant-name'] = Buffer.from(tenant, 'utf8').toString('base64');
          }
          return config;
        },
        (error) => Promise.reject(error)
      );
    }
    // Used for tracking debug prints of execute operation
    this._execSeqNumber = 0;
  }

  get httpClient() {
    if (!this._httpClient) {
      throw new BiosClientError(BiosErrorType.NOT_LOGGED_IN, 'Login to start the session');
    }
    return this._httpClient;
  }

  static initialize(params) {
    const config = {
      baseURL: params.endpoint,
      timeout: 480000,
      headers: {
        'Content-Type': 'application/json',
        xsrfToken: 'BIClient',
        Accept: 'application/json',
        'X-Bios-Version': BIOS_VERSION,
      },
      withCredentials: true,
    };
    const httpClient = axios.create(config);
    if (!!params.unauthorizedErrorInterceptor) {
      httpClient.interceptors.response.use(
        (response) => response,
        (error) => {
          if (error && error.response && error.response.status === 401) {
            params.unauthorizedErrorInterceptor(error);
          }
          return Promise.reject(error);
        },
      );
    }
    return new BiosSessionHttp(httpClient, true);
  }

  /**
   * Returns a temporary session that delegates an app tenant.
   */
  forTenant(tenantOrDomain) {
    const delegateSession = new BiosSessionHttp(this._httpClient, false);
    const userInfo = { ...this._userInfo, tenant: tenantOrDomain };
    delegateSession._setUserInfo(userInfo);
    return delegateSession;
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
  async signIn(config) {
    requiresParam(config, 'config', 'object');
    requiresProperty(config, 'config', 'email', 'string');
    requiresProperty(config, 'config', 'password', 'string');
    if (!!config.appName) {
      requiresProperty(config, 'config', 'appName', 'string');
    }
    if (!!config.appType) {
      requiresProperty(config, 'config', 'appType', 'string');
    }

    try {
      const payload = {
        email: config.email,
        password: config.password,
      };
      if (!!config.appName) {
        payload.appName = config.appName;
      }
      if (!!config.appType) {
        payload.appType = config.appType;
      }
      const sessionInfo =
        await this._httpClient.post('/bios/v1/auth/login', payload, { isSignIn: true });
      if (!!sessionInfo && !!sessionInfo.data && !!sessionInfo.data.token) {
        this._token = sessionInfo.data.token;
        delete sessionInfo.data.token;
      }
      this._setUserInfo(sessionInfo.data);
      return this;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Returns true if the user is already signed in.
   */
  isSignedIn() {
    return !!this._userInfo;
  }

  async signOut() {
    try {
      await this._httpClient.post('/bios/v1/auth/logout');
    } catch (e) {
      throw handleErrorResponse(e);
    } finally {
      this._userInfo = null;
    }
  }

  /**
   * @deprecated Use <code>signOut</code>
   */
  async logout() {
    return this.signOut();
  }

  get tenantName() {
    return this._userInfo.tenant;
  }

  /**
   * Provides the user's information.
   *
   * @returns {object} User info
   */
  async getUserInfo() {
    if (!this.isSignedIn()) {
      // Likely to be the first call after reloading, try to fetch user info from the server
      try {
        const loginResponse = await this.httpClient.get('/bios/v1/auth/info?detail=true');
        this._setUserInfo(loginResponse.data);
      } catch (e) {
        throw handleErrorResponse(e);
      }
    }

    return this._userInfo;
  }

  _setUserInfo(sessionInfo) {
    const roles = (sessionInfo.roles !== undefined && sessionInfo.roles !== null)
      ? [...sessionInfo.roles]
      : null;
    this._userInfo = {
      name: sessionInfo.userName || null,
      email: sessionInfo.email || null,
      tenant: sessionInfo.tenant,
      devInstance: { ...sessionInfo.devInstance },
      roles,
      appName: sessionInfo.appName || null,
      appType: sessionInfo.appType || null,
    };
  }

  async inviteUser(inviteRequest) {
    requiresParam(inviteRequest, 'inviteRequest', 'object');
    requiresProperty(inviteRequest, 'inviteRequest', 'email', 'string');
    requiresProperty(inviteRequest, 'inviteRequest', 'roles', 'array');

    try {
      const path = `/bios/v1/signup/invite`;
      await this.httpClient.post(path, inviteRequest);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async createTenant(tenantConfig, registerApps) {
    requiresParam(tenantConfig, 'tenant', 'object');
    requiresProperty(tenantConfig, 'tenant', 'tenantName', 'string');
    const headers = {};
    if (registerApps !== undefined && registerApps !== null) {
      requiresParam(registerApps, 'registerApps', 'boolean');
      headers.registerApps = true;
    }

    try {
      const path = `/bios/v1/tenants`;
      await this.httpClient.post(path, tenantConfig, {
        timeout: 0,  //  no timeout, the operation can take very long
        headers: headers,
      });
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Lists tenant names.
   *
   * @param {object} options - Options (optional)
   * @property {array<string>} options.names - Tenant names to list
   * @returns {Promise<array<object>>} Promise to the tenant objects with properties
   *     tenantName and version.
   */
  // @api
  async listTenantNames(options) {
    let names = '';
    if (!!options && !!options.names) {
      requiresProperty(options, 'options', 'names', 'array');
      names = '?names=' + options.names.join(',');
    }

    try {
      const path = `/bios/v1/tenants${names}`;
      const response = await this.httpClient.get(path);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async deleteTenant(tenantName) {
    requiresParam(tenantName, 'tenant', 'string');

    try {
      const path = `/bios/v1/tenants/${tenantName}`;
      await this.httpClient.delete(path);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async getTenant(options) {
    const queryParams = {
      includeInternal: true,
    };
    let tenantName = this.tenantName;
    if (!!options) {
      if (options.detail !== undefined && options.detail !== null) {
        requiresProperty(options, 'options', 'detail', 'boolean');
        queryParams.detail = options.detail;
      }
      if (options.inferredTags !== undefined && options.inferredTags !== null) {
        requiresProperty(options, 'options', 'inferredTags', 'boolean');
        queryParams.includeInferredTags = options.inferredTags;
      }
      if (options.includeInternal !== undefined && options.includeInternal !== null) {
        requiresProperty(options, 'options', 'includeInternal', 'boolean');
        queryParams.includeInternal = options.includeInternal;
      }
      if (!!options.tenantName) {
        requiresProperty(options, 'options', 'tenantName', 'string');
        tenantName = options.tenantName;
      }
    }
    const queryParamsString = this._stringifyQueryParams(queryParams);

    try {
      const path = `/bios/v1/tenants/${tenantName}?${queryParamsString}`;
      const response = await this.httpClient.get(path);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async createSignal(signalConfig) {

    requiresParam(signalConfig, 'signalConfig', 'object');

    const tenantName = this.tenantName;

    try {
      const path = `/bios/v1/tenants/${tenantName}/signals`;
      const response = await this.httpClient.post(path, signalConfig, {
        timeout: 0,  //  no timeout, the operation can take very long
      });
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
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
  async getSignals(options) {
    const queryParams = {};
    if (!!options) {
      if (options.detail !== undefined && options.detail !== null) {
        requiresProperty(options, 'options', 'detail', 'boolean');
        queryParams.detail = options.detail;
      }
      if (options.inferredTags !== undefined && options.inferredTags !== null) {
        requiresProperty(options, 'options', 'inferredTags', 'boolean');
        queryParams.includeInferredTags = options.inferredTags;
      }
      if (options.signals) {
        requiresProperty(options, 'options', 'signals', 'array');
        options.signals.forEach((signal, i) => requiresParam(signal, `options.signals[${i}]`, 'string'));
        queryParams.names = options.signals.join();
      }
      if (options.includeInternal !== undefined && options.includeInternal !== null) {
        requiresProperty(options, 'options', 'includeInternal', 'boolean');
        queryParams.includeInternal = options.includeInternal;
      }
    }
    const queryParamsString = this._stringifyQueryParams(queryParams);
    const tenantName = this.tenantName;

    try {
      const path = `/bios/v1/tenants/${tenantName}/signals?${queryParamsString}`;
      const response = await this.httpClient.get(path);
      debugLog('getSignals', response.data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async updateSignal(signalName, signalConfig) {
    requiresParam(signalConfig, 'signalConfig', 'object');

    const tenantName = this.tenantName;
    const signal = validateNonEmptyString(signalName);
    const path = `/bios/v1/tenants/${tenantName}/signals/${signal}`;
    try {
      const response = await this.httpClient.post(path, signalConfig);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async deleteSignal(signalName) {
    const tenantName = this.tenantName;
    const signal = validateNonEmptyString(signalName);
    const path = `/bios/v1/tenants/${tenantName}/signals/${signal}`;
    try {
      await this.httpClient.delete(path);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async createContext(contextConfig) {
    requiresParam(contextConfig, 'contextConfig', 'object');

    const tenantName = this.tenantName;

    try {
      const path = `/bios/v1/tenants/${tenantName}/contexts`;
      const response = await this.httpClient.post(path, contextConfig, {
        timeout: 0,  //  no timeout, the operation can take very long
      });
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async getContexts(options) {
    const queryParams = {};
    if (!!options) {
      if (options.detail !== undefined && options.detail !== null) {
        requiresProperty(options, 'options', 'detail', 'boolean');
        queryParams.detail = options.detail;
      }
      if (options.inferredTags !== undefined && options.inferredTags !== null) {
        requiresProperty(options, 'options', 'inferredTags', 'boolean');
        queryParams.includeInferredTags = options.inferredTags;
      }
      if (options.contexts) {
        requiresProperty(options, 'options', 'contexts', 'array');
        options.contexts.forEach((context, i) => requiresParam(context, `options.contexts[${i}]`, 'string'));
        queryParams.names = options.contexts.join();
      }
      if (options.includeInternal !== undefined && options.includeInternal !== null) {
        requiresProperty(options, 'options', 'includeInternal', 'boolean');
        queryParams.includeInternal = options.includeInternal;
      }
    }
    const queryParamsString = this._stringifyQueryParams(queryParams);
    const tenantName = this.tenantName;

    try {
      const path = `/bios/v1/tenants/${tenantName}/contexts?${queryParamsString}`;
      const response = await this.httpClient.get(path);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async updateContext(contextName, contextConfig) {
    requiresParam(contextConfig, 'contextConfig', 'object');

    const tenantName = this.tenantName;
    const context = validateNonEmptyString(contextName);
    const path = `/bios/v1/tenants/${tenantName}/contexts/${context}`;
    try {
      const response = await this.httpClient.post(path, contextConfig);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async deleteContext(contextName) {
    const context = validateNonEmptyString(contextName);
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/contexts/${context}`;
    try {
      await this.httpClient.delete(path);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async execute(statement) {
    const seq = ++this._execSeqNumber;
    debugLog('request', statement, seq);
    let response;
    const type = statement.type;
    switch (type) {
      case StatementType.SELECT:
        // For Select statements called using execute() we only return the first result set.
        // To get all result sets, caller needs to use multiExecute().
        response = (await this.multiExecute(statement))[0];
        break;

      case StatementType.INSERT:
        response = await this._insert(statement);
        break;
      case StatementType.UPSERT:
        response = await this._putContextEntries(statement);
        break;
      case StatementType.SELECT_CONTEXT:
        response = await this._getContextEntries(statement);
        break;
      case StatementType.SELECT_CONTEXT_EX:
        response = await this._selectContextEntries(statement);
        break;
      case StatementType.UPDATE:
        response = await this._updateContextEntry(statement);
        break;
      case StatementType.DELETE:
        response = await this._deleteContextEntries(statement);
        break;
      default:
        throw new Error('unsupported statement type'); // TODO throw better error
    }
    debugLog('response', response, seq);
    return response;
  }

  _stringifyStatements(statements) {
    if (statements) {
      const message = statements
        .map((statement) => statement.toString())
        .join('",\n "');
      return `["${message}"]`;
    }
    return '';
  }

  async multiExecute(...statements) {
    if (statements.length === 0) {
      throw new InvalidArgumentError('At least one SELECT statement is required for multiExecute.');
    }
    const seq = ++this._execSeqNumber;
    debugLog('request', () => this._stringifyStatements(statements), seq);
    debugLog('select', () => this._stringifyStatements(statements), seq);

    const selectRequest = {
      queries: []
    };
    statements.forEach((statement, statementIndex) => {
      debugLog('multiExecute1.' + statementIndex, statement, seq);
      if (statement.type !== StatementType.SELECT) {
        throw new InvalidArgumentError('Only SELECT statements are supported by multiExecute.');
      }
      selectRequest.queries.push(statement.query);
    });

    const responses = await this._select(selectRequest);
    debugLog('select', responses, seq);
    debugLog('response', responses, seq);
    if (isDebugLogEnabled('multiExecute.')) {
      responses.forEach((response, i) => {
        debugLog('multiExecute.' + i, response, seq);
      });
    }
    return responses;
  }

  async _insert(insertRequest) {
    const numEntries = insertRequest.length;
    let offset = 0;
    let batchSize = Math.min(numEntries, INGEST_BULK_BATCH_SIZE);
    const results = new Array(numEntries);
    while (offset < numEntries) {
      await this._insertCore(insertRequest, offset, batchSize, results);
      offset += batchSize;
      batchSize = Math.min(numEntries - offset, INGEST_BULK_BATCH_SIZE);
    }
    return new InsertISqlResponse(results);
  }

  async _insertCore(insertRequest, offset, length, results) {
    const buffer = codec.InsertBulkRequestEncoder.encode(insertRequest, offset, length);
    // place the request
    try {
      const path = `/bios/v1/tenants/${this.tenantName}/events/bulk`;
      const response = await this.httpClient.post(path, buffer, {
        headers: {
          'Content-Type': 'application/x-protobuf',
          Accept: 'application/x-protobuf, application/json',
        },
        responseType: 'arraybuffer',
      });
      return codec.InsertBulkResponseDecoder.decode(this._makeBuffer(response), offset, results);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async _select(selectRequest) {
    const buffer = codec.SelectRequest.encode(selectRequest);

    // place the request
    try {
      const path = `/bios/v1/tenants/${this.tenantName}/events/select`;
      const response = await this.httpClient.post(path, buffer, {
        headers: {
          'Content-Type': 'application/x-protobuf',
          Accept: 'application/x-protobuf,application/json',
        },
        responseType: 'arraybuffer',
      });
      return codec.SelectResponse.decode(response);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   *
   * @param {object} options           - Object to carry options
   * @property {string} options.detail - Returns all report properties if true, the least set otherwise.
   *                                     The least set properties are reportId and reportName.
   * @property {string|list<string>}
   *                  options.reportId - Specifies explicit report ID(s) to fetch (default: all)
   *
   * FOLLOWS ARE SUPPORTED IN FUTURE
   * @property {number} options.limit  - Limits number of entries to return (default: no limitation)
   * @property {string} options.nextOf - Used for paging, the method returns entries from the next of the
   *                                     specified ID.
   *
   * @returns {object} Metadata and reports
   *   e.g.
   *     {
   *       more: true, // FUTURE: used for paging
   *       warning: "Name 'revenueInUSD' is used by multiple reports. Please consider renaming",
   *       reportConfigs: [
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
  async getReportConfigs(options = {}) {
    let queryParams;
    if (!!options) {
      queryParams = {};
      if (options.detail) {
        requiresProperty(options, 'options', 'detail', 'boolean');
        queryParams.detail = options.detail;
      }
      if (options.reportId) {
        requiresProperty(options, 'options', 'reportId', 'string', 'array');
        switch (typeof (options.reportId)) {
          case 'string':
            queryParams.ids = options.reportId;
            break;
          case 'object':
            queryParams.ids = options.reportId.join();
            break;
        }
      }
    }
    const headers = {
      ...this._httpClient.defaults.headers,
      ...(options.invalidateCache) && { "x-invalidate-cache": true },
    };

    const queryParamsString = this._stringifyQueryParams(queryParams);
    const tenantName = this.tenantName;

    const path = `/bios/v1/tenants/${tenantName}/reports?${queryParamsString}`;
    try {
      const response = await this.httpClient.get(path, { headers });
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
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
  async putReportConfig(config) {
    const validated = validateReportConfig(config);

    const tenantName = this.tenantName;
    const reportId = validated.reportId;
    const path = `/bios/v1/tenants/${tenantName}/reports/${reportId}`;
    const body = JSON.stringify(validated);
    try {
      await this.httpClient.put(path, body);
      await this.getReportConfigs({ detail: true, invalidateCache: true });
    } catch (e) {
      const err = handleErrorResponse(e);
      throw err;
    }
  }

  /**
   * Deletes a report configuration.
   *
   * @param {string} reportId - Report ID of the configuration to delete.
   */
  async deleteReportConfig(reportId) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/reports/${reportId}`;
    try {
      await this.httpClient.delete(path);
      await this.getReportConfigs({ detail: true, invalidateCache: true });
    } catch (e) {
      const err = handleErrorResponse(e);
      throw err;
    }
  }

  /**
   * Returns the user's insight configuration objects
   *
   * @param {string} insightName - Insight name
   * @param {object} options - Options
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
  async getInsightConfigs(insightName, options = {}) {
    const tenantName = this.tenantName;
    const headers = {
      ...this._httpClient.defaults.headers,
      ...(options.invalidateCache) && { "x-invalidate-cache": true },
    };
    const path = `/bios/v1/tenants/${tenantName}/insights/${insightName}`;
    try {
      const response = await this.httpClient.get(path, { headers });
      const configs = response.data || {};
      configs.sections = configs.sections || [];
      return configs;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * The method puts an insight config to the backbone storage. If the backbone storage has an entry with
   * the same ID already, the method overwrites it.
   *
   * Example usage:
   *   bios.putInsightConfigs({
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
   * @param {string} insightName
   * @param {object} insightConfigs
   * @property {list<section>} sections
   * @property {string} section.sectionId - Unique ID within the user
   * @property {number} section.timeRange - Time range of the insights used in this section
   * @property {list<insight>} section.insightConfigs - Insight configs
   * @property {string} insight.insightId - Insight ID
   * @property {string} insight.reportId - Report ID
   */
  async putInsightConfigs(insightName, insightConfigs) {
    const validated = validateInsightConfigs(insightConfigs);

    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/insights/${insightName}`;
    try {
      await this.httpClient.post(path, validated);
      await this.getInsightConfigs(insightName, {
        invalidateCache: true
      });
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Deletes entire insight configurations for the user.
   */
  async deleteInsightConfigs(insightName) {

    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/insights/${insightName}`;
    try {
      await this.httpClient.delete(path);
      await this.getInsightConfigs({
        invalidateCache: true
      });
    } catch (e) {
      throw handleErrorResponse(e);
    }
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
  async getInsights(insightRequests) {
    const requests = validateGetInsightsRequest(insightRequests);
    const statements = buildGetInsightsStatements(requests);
    const responses = await this.multiExecute(...statements);
    if (insightRequests.length !== responses.length) {
      throw Error('Request and response sizes are different unexpectedly;'
        + ` requests=${JSON.stringify(insightRequests)}, response=${JSON.stringify(responses)}`);
    }
    return insightRequests.map((request, i) => {
      const insight = {
        insightId: request.insightId,
      };
      const windowSize = statements[i].query.windows[0].tumbling.windowSizeMs;
      const response = responses[i];
      let data;
      insight.value = null;
      insight.timestamp = null;
      if (!!(data = response.data) && data.length > 0) {
        data.forEach((chunk) => {
          if (chunk.records.length > 0 && chunk.records[0].length > 0) {
            insight.timestamp = chunk.windowBeginTime + windowSize;
            if (insight.value === null) {
              insight.value = 0;
            }
            insight.value += chunk.records[0][0];
          }
        });
      } else {
        insight.value = null;
        insight.timestamp = null;
      }
      return insight;
    });
  }

  async getSignalSynopsis(signalName, currentTime, recallTimeMs, windowSizeMs) {
    currentTime = (!currentTime) ? timeUtils.now() : currentTime;
    recallTimeMs = (!recallTimeMs) ? timeUtils.days(1) : recallTimeMs;
    windowSizeMs = (!windowSizeMs) ? timeUtils.hours(1) : windowSizeMs;
    const signalSynopsis = {};
    signalSynopsis.signalName = signalName;
    const statement = new ISqlStatementBuilder()
      .select('synopsis()')
      .from(signalName)
      .tumblingWindow(windowSizeMs)
      .snappedTimeRange(currentTime, -1 * recallTimeMs, windowSizeMs)
      .build()
    let synopsis;
    try {
      synopsis = await this.multiExecute(statement);
    } catch (e) {
      debugLog('exceptions', e);
      debugLog('getSignalSynopsis', signalSynopsis);
      return signalSynopsis;
    }
    debugLog('getSignalSynopsis1', synopsis);

    if (!synopsis || (synopsis.length < 3)
      || !synopsis[0].dataWindows
      || !synopsis[1].dataWindows
      || !synopsis[2].dataWindows) {
      debugLog('getSignalSynopsis', signalSynopsis);
      return signalSynopsis;
    }

    const lastUpdated = new Date(0);
    lastUpdated.setUTCMilliseconds(currentTime);
    signalSynopsis.lastUpdated = lastUpdated;
    signalSynopsis.durationSecs = recallTimeMs / 1000;
    signalSynopsis.timeWindowSizeSecs = windowSizeMs / 1000;

    // Add Counts for requested time windows.
    const startTime = [];
    const count = [];
    const countsResponse = synopsis[0];
    if (!!countsResponse.dataWindows.length) {
      countsResponse.dataWindows.map((window) => {
        if ((!window) || (!window.windowBeginTime) || (!window.records) || (!window.records.length)
          || (!window.records[0]) || (!window.records[0].length)) {
          return;
        }
        const startTimeFormatted = new Date(0);
        startTimeFormatted.setUTCMilliseconds(window.windowBeginTime);
        startTime.push(startTimeFormatted);
        count.push(window.records[0][0]);
      });
    }
    signalSynopsis.startTime = startTime;
    signalSynopsis.count = count;
    debugLog('getSignalSynopsis2', signalSynopsis);

    // Add total count for previous (cyclical) time window.
    const countCurrent = count.reduce((a, b) => parseInt(a) + parseInt(b), 0);
    let countPrevious = 0;
    let countTrendPercent = 0;
    let countTrendDesirability = 0;
    const prevCountResponse = synopsis[1];
    if ((!!prevCountResponse.dataWindows.length)
      && (!!prevCountResponse.dataWindows[0]) && (!!prevCountResponse.dataWindows[0].records)
      && (!!prevCountResponse.dataWindows[0].records.length)
      && (!!prevCountResponse.dataWindows[0].records[0].length)) {
      countPrevious = parseInt(prevCountResponse.dataWindows[0].records[0][0]);
      if (countPrevious !== 0) {
        countTrendPercent = (countCurrent - countPrevious) * 100 / countPrevious;
        countTrendDesirability = Math.sign(countTrendPercent);
      }
    }
    signalSynopsis.countCurrent = countCurrent;
    signalSynopsis.countPrevious = countPrevious;
    signalSynopsis.countTrendPercent = countTrendPercent;
    signalSynopsis.countTrendDesirability = countTrendDesirability;
    debugLog('getSignalSynopsis3', signalSynopsis);

    // Add the list of attributes in this signal.
    const attributesResponse = synopsis[2];
    const attributes = [];
    if ((!!attributesResponse.dataWindows.length)
      && (!!attributesResponse.dataWindows[0]) && (!!attributesResponse.dataWindows[0].records)
      && (!!attributesResponse.dataWindows[0].records.length)) {
      attributesResponse.dataWindows[0].records.map((singleAttribute) => {
        const attribute = {};
        attribute.attributeName = singleAttribute[0];
        attribute.attributeType = singleAttribute[1];
        attribute.attributeOrigin = singleAttribute[2];
        attributes.push(attribute);
      });
    }
    signalSynopsis.attributes = attributes;

    debugLog('getSignalSynopsis', signalSynopsis);
    debugLog('getSignalSynopsis.' + signalName, signalSynopsis);
    return signalSynopsis;
  }

  async getAttributeSynopsis(signalName, attributeName, currentTime, recallTimeMs, windowSizeMs) {
    currentTime = (!currentTime) ? timeUtils.now() : currentTime;
    recallTimeMs = (!recallTimeMs) ? timeUtils.days(1) : recallTimeMs;
    windowSizeMs = (!windowSizeMs) ? timeUtils.hours(1) : windowSizeMs;
    const attributeSynopsis = {};
    attributeSynopsis.signalName = signalName;
    attributeSynopsis.attributeName = attributeName;
    const statement = new ISqlStatementBuilder()
      .select('synopsis(' + attributeName + ')')
      .from(signalName)
      .tumblingWindow(windowSizeMs)
      .snappedTimeRange(currentTime, -1 * recallTimeMs, windowSizeMs)
      .build()
    let synopsis;
    try {
      synopsis = await this.multiExecute(statement);
    } catch (e) {
      debugLog('exceptions', e);
      debugLog('getAttributeSynopsis', attributeSynopsis);
      return attributeSynopsis;
    }
    debugLog('getAttributeSynopsis1', synopsis);

    if (!synopsis || (synopsis.length < 5)
      || !synopsis[0].dataWindows
      || !synopsis[1].dataWindows
      || !synopsis[2].dataWindows
      || !synopsis[3].dataWindows
      || !synopsis[4].dataWindows) {
      debugLog('getAttributeSynopsis', attributeSynopsis);
      return attributeSynopsis;
    }

    const lastUpdated = new Date(0);
    lastUpdated.setUTCMilliseconds(currentTime);
    attributeSynopsis.lastUpdated = lastUpdated;
    attributeSynopsis.durationSecs = recallTimeMs / 1000;
    attributeSynopsis.timeWindowSizeSecs = windowSizeMs / 1000;

    // Get data type of the attribute and other metadata.
    const metadata = synopsis[0].dataWindows;
    if ((!metadata.length) || (!metadata[0])
      || (!metadata[0].records) || (!metadata[0].records.length)
      || (!metadata[0].records[0]) || (!metadata[0].records[0].length)) {
      debugLog('getAttributeSynopsis', attributeSynopsis);
      return attributeSynopsis;
    }
    const attributeType = metadata[0].records[0][0];
    attributeSynopsis.attributeType = attributeType;
    attributeSynopsis.unitDisplayName = metadata[0].records[0][1];
    attributeSynopsis.unitDisplayPosition = metadata[0].records[0][2];
    attributeSynopsis.positiveIndicator = metadata[0].records[0][3];
    const firstSummary = metadata[0].records[0][4];
    const secondSummary = metadata[0].records[0][5];
    const firstSummaryIndex = parseInt(metadata[0].records[0][6]);
    const secondSummaryIndex = parseInt(metadata[0].records[0][7]);
    attributeSynopsis.firstSummary = firstSummary;
    attributeSynopsis.secondSummary = secondSummary;
    let positiiveIndicatorMultiplier;
    if (attributeSynopsis.positiveIndicator === 'High') {
      positiiveIndicatorMultiplier = 1;
    } else if (attributeSynopsis.positiveIndicator === 'Low') {
      positiiveIndicatorMultiplier = -1;
    } else {
      positiiveIndicatorMultiplier = 0;
    }

    // Add metrics for requested time windows.
    const metrics = synopsis[1].dataWindows;
    if (!metrics.length) {
      debugLog('getAttributeSynopsis', attributeSynopsis);
      return attributeSynopsis;
    }
    const startTime = [];
    metrics.map((window) => {
      if ((!window) || (!window.windowBeginTime) || (!window.records) || (!window.records.length)
        || (!window.records[0]) || (!window.records[0].length)) {
        return;
      }
      const startTimeFormatted = new Date(0);
      startTimeFormatted.setUTCMilliseconds(window.windowBeginTime);
      startTime.push(startTimeFormatted);
    });
    attributeSynopsis.startTime = startTime;
    attributeSynopsis.count = metrics.map((window) => window.records[0][0]);
    attributeSynopsis.distinctCount = metrics.map((window) => window.records[0][1]);
    if ((attributeType === 'Integer') || (attributeType === 'Decimal')) {
      attributeSynopsis.sum = metrics.map((window) => window.records[0][2]);
      attributeSynopsis.avg = metrics.map((window) => window.records[0][3]);
      attributeSynopsis.min = metrics.map((window) => window.records[0][4]);
      attributeSynopsis.max = metrics.map((window) => window.records[0][5]);
      attributeSynopsis.stddev = metrics.map((window) => window.records[0][6]);
      attributeSynopsis.skewness = metrics.map((window) => window.records[0][7]);
      attributeSynopsis.kurtosis = metrics.map((window) => window.records[0][8]);
      attributeSynopsis.p1 = metrics.map((window) => window.records[0][9]);
      attributeSynopsis.p25 = metrics.map((window) => window.records[0][10]);
      attributeSynopsis.median = metrics.map((window) => window.records[0][11]);
      attributeSynopsis.p75 = metrics.map((window) => window.records[0][12]);
      attributeSynopsis.p99 = metrics.map((window) => window.records[0][13]);
      if (firstSummaryIndex > 13) {
        // We have a special summary function requested that is not part of the default metrics above. Add it.
        attributeSynopsis[firstSummary.toLowerCase()] = metrics.map((window) => window.records[0][firstSummaryIndex]);
      }
      if (secondSummaryIndex > 13) {
        // We have a special summary function requested that is not part of the default metrics above. Add it.
        attributeSynopsis[secondSummary.toLowerCase()] = metrics.map((window) => window.records[0][secondSummaryIndex]);
      }
    }
    debugLog('getAttributeSynopsis2', attributeSynopsis);


    // Add summary metrics for this and previous (cyclical) time window if present.
    const currentMetrics = synopsis[2].dataWindows;
    const prevMetrics = synopsis[3].dataWindows;
    if ((!currentMetrics.length) || (!currentMetrics[0].records) || (!currentMetrics[0].records.length)) {
      debugLog('getAttributeSynopsis', attributeSynopsis);
      return attributeSynopsis;
    }
    attributeSynopsis.distinctCountCurrent = currentMetrics[0].records[0][0];
    if ((!prevMetrics.length) || (!prevMetrics[0].records) || (!prevMetrics[0].records.length)) {
      attributeSynopsis.distinctCountPrevious = 0;
      attributeSynopsis.distinctCountTrendPercent = 0;
    } else {
      attributeSynopsis.distinctCountPrevious = prevMetrics[0].records[0][0];
      attributeSynopsis.distinctCountTrendPercent =
        (attributeSynopsis.distinctCountCurrent - attributeSynopsis.distinctCountPrevious) * 100 /
        attributeSynopsis.distinctCountPrevious;
    }
    let runningIndex = 1;
    if ((firstSummaryIndex >= 0) || (firstSummary === 'TIMESTAMP_LAG')) {
      let firstSummaryCurrent;
      let firstSummaryPrevious;
      if (firstSummaryIndex >= 0) {
        let firstSummaryCurrentIndex;
        if (firstSummary === 'DISTINCTCOUNT') {
          firstSummaryCurrentIndex = 0;
        } else {
          firstSummaryCurrentIndex = runningIndex;
          runningIndex++;
        }
        firstSummaryCurrent = parseFloat(currentMetrics[0].records[0][firstSummaryCurrentIndex]);
        if ((!prevMetrics.length) || (!prevMetrics[0].records) || (!prevMetrics[0].records.length)) {
          firstSummaryPrevious = NaN;
        } else {
          firstSummaryPrevious = parseFloat(prevMetrics[0].records[0][firstSummaryCurrentIndex]);
        }
      } else {
        firstSummaryCurrent = parseFloat(metadata[0].records[0][8] / (1000 * 60));
        firstSummaryPrevious = parseFloat(metadata[0].records[0][10] / (1000 * 60));
      }
      let firstSummaryTrendPercent = 0;
      let firstSummaryTrendDesirability = 0;
      if (firstSummaryPrevious !== 0) {
        firstSummaryTrendPercent = (firstSummaryCurrent - firstSummaryPrevious) * 100 / firstSummaryPrevious;
        firstSummaryTrendDesirability = Math.sign(firstSummaryTrendPercent) * positiiveIndicatorMultiplier;
      }
      attributeSynopsis.firstSummaryCurrent = firstSummaryCurrent;
      attributeSynopsis.firstSummaryPrevious = firstSummaryPrevious;
      attributeSynopsis.firstSummaryTrendPercent = firstSummaryTrendPercent;
      attributeSynopsis.firstSummaryTrendDesirability = firstSummaryTrendDesirability;
    }
    debugLog('getAttributeSynopsis3', attributeSynopsis);

    if ((secondSummaryIndex >= 0) || (secondSummary === 'TIMESTAMP_LAG')) {
      let secondSummaryCurrent;
      let secondSummaryPrevious;
      if (secondSummaryIndex >= 0) {
        let secondSummaryCurrentIndex;
        if (secondSummary === 'DISTINCTCOUNT') {
          secondSummaryCurrentIndex = 0;
        } else {
          secondSummaryCurrentIndex = runningIndex;
          runningIndex++;
        }
        secondSummaryCurrent = parseFloat(currentMetrics[0].records[0][secondSummaryCurrentIndex]);
        if ((!prevMetrics.length) || (!prevMetrics[0].records) || (!prevMetrics[0].records.length)) {
          secondSummaryPrevious = NaN;
        } else {
          secondSummaryPrevious = parseFloat(prevMetrics[0].records[0][secondSummaryCurrentIndex]);
        }
      } else {
        secondSummaryCurrent = parseFloat(metadata[0].records[0][8] / (1000 * 60));
        secondSummaryPrevious = parseFloat(metadata[0].records[0][10] / (1000 * 60));
      }
      let secondSummaryTrendPercent = 0;
      let secondSummaryTrendDesirability = 0;
      if (secondSummaryPrevious !== 0) {
        secondSummaryTrendPercent = (secondSummaryCurrent - secondSummaryPrevious) * 100 / secondSummaryPrevious;
        secondSummaryTrendDesirability = Math.sign(secondSummaryTrendPercent) * positiiveIndicatorMultiplier;
      }
      attributeSynopsis.secondSummaryCurrent = secondSummaryCurrent;
      attributeSynopsis.secondSummaryPrevious = secondSummaryPrevious;
      attributeSynopsis.secondSummaryTrendPercent = secondSummaryTrendPercent;
      attributeSynopsis.secondSummaryTrendDesirability = secondSummaryTrendDesirability;
    }
    debugLog('getAttributeSynopsis4', attributeSynopsis);

    // Add samples and their counts.
    const sampleCountResponse = synopsis[4].dataWindows;
    if ((!sampleCountResponse.length) || (!sampleCountResponse[0])
      || (!sampleCountResponse[0].records) || (!sampleCountResponse[0].records.length)) {
      debugLog('getAttributeSynopsis', attributeSynopsis);
      return attributeSynopsis;
    }
    const sample = [];
    const sampleCount = [];
    const sampleLength = [];
    sampleCountResponse[0].records.map((record) => {
      if (!record.length) {
        debugLog('getAttributeSynopsis', attributeSynopsis);
        return attributeSynopsis;
      }
      sample.push(record[0]);
      sampleCount.push(record[1]);
      if (record.length > 2) {
        sampleLength.push(record[2]);
      }
    });
    attributeSynopsis.sample = sample;
    attributeSynopsis.sampleCount = sampleCount;
    if (sampleLength.length > 0) {
      attributeSynopsis.sampleLength = sampleLength;
    }
    debugLog('getAttributeSynopsis5', attributeSynopsis);

    // Add timestamp lag if present.
    if ((firstSummary === 'TIMESTAMP_LAG') || (secondSummary === 'TIMESTAMP_LAG')) {
      const timestampLag = synopsis[5].dataWindows;
      attributeSynopsis.timestampLagMinutes = timestampLag.map((window) => window.records[0][0] / (1000 * 60));
    }
    debugLog('getAttributeSynopsis6', attributeSynopsis);

    debugLog('getAttributeSynopsis', attributeSynopsis);
    debugLog('getAttributeSynopsis.' + attributeName, attributeSynopsis);
    return attributeSynopsis;
  }

  async getAllContextSynopses(currentTime) {
    const tenantName = this.tenantName;
    if (!currentTime) {
      currentTime = timeUtils.now();
    }
    const request = { currentTime };
    try {
      const path = `/bios/v1/tenants/${tenantName}/synopses/contexts`;
      const response = await this.httpClient.post(path, request);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async getContextSynopsis(contextName, currentTime) {
    const tenantName = this.tenantName;
    if (!currentTime) {
      currentTime = timeUtils.now();
    }
    const request = { currentTime };
    try {
      const path = `/bios/v1/tenants/${tenantName}/synopses/contexts/${contextName}`;
      const response = await this.httpClient.post(path, request);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async getAvailableMetrics2() {
    const tenantName = this.tenantName;
    try {
      const path = `/bios/v1/tenants/${tenantName}/availableMetrics`;
      const response = await this.httpClient.get(path);
      debugLog('getAvailableMetrics2', response.data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async getSupportedTags() {
    try {
      const path = `/bios/v1/admin/supportedTags`;
      const response = await this.httpClient.get(path);
      debugLog('getSupportedTags', response.data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async _getContextEntries(statement) {
    // TODO(Naoki): sanity check
    const tenantName = this.tenantName;
    const contextName = statement.getTarget();
    const path = `/bios/v1/tenants/${tenantName}/contexts/${contextName}/entries/fetch`;
    try {
      const response = await this.httpClient.post(path, statement.getRequestMessage());
      return new SelectContextISqlResponse(response.data);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async _selectContextEntries(statement) {
    // TODO(Naoki): sanity check
    const tenantName = this.tenantName;
    const contextName = statement.getTarget();
    const path = `/bios/v1/tenants/${tenantName}/contexts/${contextName}/entries/select`;
    try {
      const response = await this.httpClient.post(path, statement.getRequestMessage());
      return new SelectContextISqlResponse(response.data);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async _putContextEntries(statement) {
    // TODO(Naoki): sanity check
    const tenantName = this.tenantName;
    const contextName = statement.getTarget();
    const path = `/bios/v1/tenants/${tenantName}/contexts/${contextName}/entries`;
    try {
      await this.httpClient.post(path, statement.getRequestMessage());
      return VoidISqlResponse.INSTANCE;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async _updateContextEntry(statement) {
    // TODO(Naoki): sanity check
    const tenantName = this.tenantName;
    const contextName = statement.getTarget();
    const path = `/bios/v1/tenants/${tenantName}/contexts/${contextName}/entries`;
    try {
      await this.httpClient.patch(path, statement.getRequestMessage());
      return VoidISqlResponse.INSTANCE;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async _deleteContextEntries(statement) {
    // TODO(Naoki): sanity check
    const tenantName = this.tenantName;
    const contextName = statement.getTarget();
    const path = `/bios/v1/tenants/${tenantName}/contexts/${contextName}/entries/delete`;
    try {
      await this.httpClient.post(path, statement.getRequestMessage());
      return VoidISqlResponse.INSTANCE;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async createTenantAppendix(category, content, entryId) {
    requiresParam(category, 'category', 'string');
    requiresParam(content, 'content', 'object');
    if (!!entryId) {
      requiresParam(entryId, 'entryId', 'string');
    }
    const tenantAppendixSpec = {
      '@type': category,
      content,
    };
    if (!!entryId) {
      tenantAppendixSpec.entryId = entryId;
    }
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/appendixes/${category}`;
    try {
      return (await this.httpClient.post(path, tenantAppendixSpec)).data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async getTenantAppendix(category, entryId) {
    requiresParam(category, 'category', 'string');
    requiresParam(entryId, 'entryId', 'string');
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/appendixes/${category}/entries/${entryId}`;
    try {
      return (await this.httpClient.get(path)).data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async updateTenantAppendix(category, content, entryId) {
    requiresParam(category, 'category', 'string');
    requiresParam(content, 'content', 'object');
    requiresParam(entryId, 'entryId', 'string');

    const tenantAppendixSpec = {
      '@type': category,
      entryId,
      content,
    };
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/appendixes/${category}/entries/${entryId}`;
    try {
      return (await this.httpClient.post(path, tenantAppendixSpec)).data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async deleteTenantAppendix(category, entryId) {
    requiresParam(entryId, 'entryId', 'string');
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/appendixes/${category}/entries/${entryId}`;
    try {
      return await this.httpClient.delete(path);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async createExportDestination(exportDestinationConfig) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/exports`;
    try {
      const response = await this.httpClient.post(path, exportDestinationConfig);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async updateExportDestination(name, exportDestinationConfig) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/exports/${name}`;
    try {
      const response = await this.httpClient.put(path, exportDestinationConfig);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async getExportDestination(name) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/exports/${name}`;
    try {
      const response = await this.httpClient.get(path);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async deleteExportDestination(name) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/exports/${name}`;
    try {
      await this.httpClient.delete(path);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async discoverImportSubjects(importSourceId, timeout) {
    if (timeout === undefined || timeout === null) {
      timeout = 60;
    }
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/importSources/${importSourceId}/subjects`;
    try {
      return (await this.httpClient.get(path, {
        params: { timeout },
        timeout: timeout * 1500, // 1.5 times longer than server side timeout
      })).data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
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
  async changePassword(request) {
    requiresParam(request, 'request', 'object');
    requiresProperty(request, 'request', 'newPassword', 'string');
    if (!!request.currentPassword) {
      requiresProperty(request, 'request', 'currentPassword', 'string');
    }
    if (!!request.email) {
      requiresProperty(request, 'request', 'email', 'string');
    }
    const path = '/bios/v1/auth/change-password';
    try {
      await this.httpClient.post(path, request);
    } catch (e) {
      throw handleErrorResponse(e);
    }
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
  async createUser(user) {
    requiresParam(user, 'user', 'object');
    requiresProperty(user, 'user', 'email', 'string');
    requiresProperty(user, 'user', 'password', 'string');
    requiresProperty(user, 'user', 'tenantName', 'string');
    requiresProperty(user, 'user', 'fullName', 'string');
    requiresProperty(user, 'user', 'roles', 'object');
    const path = '/bios/v1/users';
    const userInfo = {
      status: 'Active',
      ...user
    };
    try {
      const response = await this.httpClient.post(path, userInfo);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Gets all users in the tenant.
   *
   * @return {list<object>} List of user configurations
   */
  async getUsers() {
    const path = '/bios/v1/users';
    try {
      const response = await this.httpClient.get(path);
      return response.data.users;
    } catch (e) {
      throw handleErrorResponse(e);
    }
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
  async modifyUser(user) {
    requiresParam(user, 'user', 'object');
    requiresProperty(user, 'user', 'userId', 'string');
    const path = `/bios/v1/users/${user.userId}`;
    delete user.userId;
    try {
      const response = await this.httpClient.patch(path, user);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Delete a user.
   *
   * @param {object} user - User configuration. Either of properties email or id is required
   * @property {string} user.email - Email address
   * @property {string} user.userId - User ID
   */
  async deleteUser(user) {
    requiresParam(user, 'user', 'object');
    if (!!user.email) {
      requiresProperty(user, 'user', 'email', 'string');
    }
    if (!!user.userId) {  // assuming user.userId never is zero
      requiresProperty(user, 'user', 'userId', 'string', 'number');
    }
    if (!user.email && !user.userId) {
      throw new InvalidArgumentError("Either of properties 'user.email' or 'user.id' must be set");
    }

    const param = this._stringifyQueryParams(user);
    const path = `/bios/v1/users?${param}`;
    try {
      await this.httpClient.delete(path);
    } catch (e) {
      throw handleErrorResponse(e);
    }
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
  async registerAppsService(appsInfo) {
    requiresParam(appsInfo, 'appsInfo', 'object');
    requiresProperty(appsInfo, 'appsInfo', 'tenantName', 'string');
    if (appsInfo.hosts !== undefined) {
      requiresProperty(appsInfo, 'appsInfo', 'hosts', 'object');
    }
    if (appsInfo.controlPort !== undefined) {
      requiresProperty(appsInfo, 'appsInfo', 'controlPort', 'number');
    }
    if (appsInfo.webhookPort !== undefined) {
      requiresProperty(appsInfo, 'appsInfo', 'webhookPort', 'number');
    }

    const path = '/bios/v1/admin/apps';
    try {
      const response = await this.httpClient.post(path, appsInfo);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Gets biOS Apps service information for a tenant.
   *
   * @param {string} tenantName - Tenant name
   * @returns The biOS Apps information for the specified tenant.
   */
  async getAppsInfo(tenantName) {
    requiresParam(tenantName, 'tenantName', 'string');

    const path = `/bios/v1/admin/apps/${tenantName}`;
    try {
      const response = await this.httpClient.get(path);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Deregisters biOS Apps service for a tenant.
   *
   * @param {string} tenantName - Tenant name
   */
  async deregisterAppsService(tenantName) {
    requiresParam(tenantName, 'tenantName', 'string');

    const path = `/bios/v1/admin/apps/${tenantName}`;
    try {
      await this.httpClient.delete(path);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Sets a shared property value.
   *
   * @param {string} key - Property key
   * @param {string} value - Property value
   */
  async setProperty(key, value) {
    requiresParam(key, 'key', 'string');
    requiresParam(value, 'value', 'string');

    const path = `/bios/v1/admin/properties/${key}`;
    try {
      const encoded = Buffer.from(value, 'utf8').toString('base64');
      await this.httpClient.put(path, JSON.stringify(encoded));
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Gets a shared property value.
   *
   * @param {string} key - Property key
   * @returns The property value. If the specified property is not set, an empty string is returned.
   */
  async getProperty(key) {
    requiresParam(key, 'key', 'string');

    const path = `/bios/v1/admin/properties/${key}`;
    try {
      const response = await this.httpClient.get(path);
      return Buffer.from(response.data, 'base64').toString('utf8');
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async initiateSignup(email, sid, gcpMarketplaceToken) {
    const path = '/bios/v1/signup/initiate';
    const data = { email, sid, gcpMarketplaceToken };
    try {
      await this.httpClient.post(path, data);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async initiateServiceRegistration(primaryEmail, service, source, domain, additionalEmails) {
    const path = '/bios/v1/signup/initiate';
    const data = {
      email: primaryEmail,
      source,
      service,
      domain,
      additionalEmails,
    };
    try {
      const response = await this.httpClient.post(path, data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async verifySignupToken(token) {
    const path = '/bios/v1/signup/verify';
    const data = { token };
    try {
      const response = await this.httpClient.post(path, data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async completeSignup(token, password, username) {
    const path = '/bios/v1/signup/complete';
    const data = { token, password, username };
    try {
      const response = await this.httpClient.post(path, data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async initiateResetPassword(email) {
    const path = '/bios/v1/auth/forgotpassword/initiate';
    const data = { email };
    try {
      await this.httpClient.post(path, data);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async completeResetPassword(token, password) {
    const path = '/bios/v1/auth/forgotpassword/reset';
    const data = { token, password };
    try {
      const response = await this.httpClient.post(path, data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async featureStatus(featureStatusRequest) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/feature/status`;
    try {
      const response = await this.httpClient.post(path, featureStatusRequest);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async featureRefresh(featureRefreshRequest) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/feature/refresh`;
    try {
      await this.httpClient.post(path, featureRefreshRequest);
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  async teachBios(rows) {
    const tenantName = this.tenantName;
    const path = `/bios/v1/tenants/${tenantName}/teachbios`;
    const data = {
      rows,
    }
    try {
      const response = await this.httpClient.post(path, data);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Registers a domain and a user for a service.
   *
   * @param {object} options Request options
   * @property {string} options.domain Domain name of the subscriber (required)
   * @property {string} options.email Email address of the subscriber (required)
   * @property {string} options.serviceType Service type (required)
   */
  async registerForService(options) {
    requiresParam(options, 'options', 'object');
    requiresProperty(options, 'options', 'domain', 'string');
    requiresProperty(options, 'options', 'email', 'string');
    requiresProperty(options, 'options', 'serviceType', 'string');

    const path = '/bios/v1/services/register';
    try {
      const response = await this._httpClient.post(path, options);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Query for store enhancement info.
   *
   * Example query parameters are:
   * ```
   * {
   *   domain: 'my-shopify-app.shopify.com',
   *   sessionId: '55435098',
   *   clientId: '763574bc-e23a-40fd-bff2-407634d25962',
   *   productId: '8192902',
   *   queries: [
   *     {
   *       queryType: 'RecentViews',
   *       maxItems: 7,
   *     },
   *     {
   *       queryType: 'NumViewers',
   *     }
   *   ]
   * }
   * ```
   *
   * @param {object} params Request parameters
   * @property {string} params.domain Domain name of the subscriber
   * @property {string} params.tenant Tenant name (optional)
   * @property {string} params.sessionId Session ID
   * @property {string} params.clientId Client ID
   * @property {string} params.productId Product ID
   * @property {list<object>} params.queries Query type, currently
   * @property {string} query.queryType Type of query,
   *   current allowed values are 'RecentViews' and 'NumViewers'
   * @property {number} query.maxItems Maximum items to return (optional, default=5)
   */
  async storeEnhancementQuery(params) {
    requiresParam(params, 'params', 'object');
    requiresProperty(params, 'params', 'domain', 'string');
    if (!!params.tenantName) {
      requiresProperty(params, 'params', 'tenantName', 'string');
    }
    if (!!params.sessionId) {
      requiresProperty(params, 'params', 'sessionId', 'string');
    }
    if (!!params.clientId) {
      requiresProperty(params, 'params', 'clientId', 'string');
    }
    if (!!params.productId) {
      requiresProperty(params, 'params', 'productId', 'string');
    }
    requiresProperty(params, 'params', 'queries', 'array');
    params.queries.forEach((query, i) => {
      requiresParam(query, `params.queries[${i}]`, 'object');
      requiresParam(query.queryType, `params.queries[${i}].queryType`, 'string');
      if (query.maxItems !== undefined) {
        requiresParam(query.maxItems, `params.queries[${i}].maxItems`, 'number');
      }
    });

    const path = '/bios/v1/stores/query';
    try {
      const response = await this._httpClient.post(path, params);
      return response.data;
    } catch (e) {
      throw handleErrorResponse(e);
    }
  }

  /**
   * Make a buffer as Uint8Array from the web response.
   * @param {object} response - Web response
   */
  _makeBuffer(response) {
    return new Uint8Array(response.data, 0, response.headers['content-length']);
  }

  _stringifyQueryParams(queryParams) {
    return new URLSearchParams(queryParams).toString();
  }
}

export function validateReportConfig(reportConfig) {
  requiresParam(reportConfig, 'reportConfig', 'object');
  requiresProperty(reportConfig, 'reportConfig', 'reportId', 'string');
  requiresProperty(reportConfig, 'reportConfig', 'reportName', 'string');
  requiresProperty(reportConfig, 'reportConfig', 'metrics', 'array');
  let metrics = reportConfig.metrics.map((metric) => validateMetric(metric));
  requiresProperty(reportConfig, 'reportConfig', 'dimensions', 'array'); // TODO check the list
  if (!!reportConfig.filters) {
    requiresProperty(reportConfig, 'reportConfig', 'filters', 'object'); // TODO verify details
  }
  if (reportConfig.defaultStartTime !== undefined) {
    requiresProperty(reportConfig, 'reportConfig', 'defaultStartTime', 'number');
  }
  requiresProperty(reportConfig, 'reportConfig', 'defaultTimeRange', 'number');
  requiresProperty(reportConfig, 'reportConfig', 'defaultWindowLength', 'number');

  return {
    ...reportConfig,
    metrics,
  };
}

export function validateInsightConfigs(insightConfigs) {
  requiresParam(insightConfigs, 'insightConfigs', 'object');
  requiresProperty(insightConfigs, 'insightConfigs', 'sections', 'array');
  insightConfigs.sections.map((section, i) => {
    const sectionName = `sections[${i}]`;
    requiresProperty(section, sectionName, 'sectionId', 'string');
    if (section.timeRange !== undefined) {
      requiresProperty(section, sectionName, 'timeRange', 'number');
    }
    requiresProperty(section, sectionName, 'insightConfigs', 'array');
    section.insightConfigs.map((insightConfig, j) => {
      const configName = `${sectionName}.insightConfigs[${j}]`;
      requiresProperty(insightConfig, configName, 'insightId', 'string');
      requiresProperty(insightConfig, configName, 'reportId', 'string');
    });
  });
  return {
    ...insightConfigs
  };
}

export function validateGetInsightsRequest(insights) {
  requiresParam(insights, 'insights', 'array');
  return insights.map((insight, i) => {
    let { insightId, metric, originTime, delta, snapStepSize } = insight;
    requiresProperty(insight, `insights[${i}]`, 'insightId', 'string');
    requiresProperty(insight, `insights[${i}]`, 'metric', 'string', 'object');
    requiresProperty(insight, `insights[${i}]`, 'originTime', 'number');
    if (insight.originTime <= 0) {
      throw new InvalidArgumentError(`insights[${i}].originTime must be a positive number`);
    }
    requiresProperty(insight, `insights[${i}]`, 'delta', 'number');
    requiresProperty(insight, `insights[${i}]`, 'snapStepSize', 'number');
    if (insight.snapStepSize <= 0) {
      throw new InvalidArgumentError(`insights[${i}].snapStepSize must be a positive number`);
    }
    metric = validateMetric(metric);
    const validated = { insightId, metric, originTime, delta, snapStepSize };
    // For testing purpose. Not strictly validated...
    if (insight.originTime) {
      validated.originTime = insight.originTime;
    }
    return validated;
  });
}

export function buildGetInsightsStatements(requests) {
  return requests.map((request, i) => {
    // break measurement to signal and function
    const tokens = request.metric.measurement.split('.', 2);
    if (tokens.length < 2) {
      throw new Error(`requests[${i}]: Unexpected measurement value: ${JSON.stringify(request)}`);
    }

    const windowSize = Math.abs(request.delta);

    return new ISqlStatementBuilder()
      .select(tokens[1])
      .from(tokens[0])
      .tumblingWindow(windowSize)
      .snappedTimeRange(request.originTime, request.delta, request.snapStepSize)
      .build();
  });
}
