import cookie from 'cookie';

import { BiosErrorType } from '../common/errors';
import { BiosSession, BiosSessionHttp } from './biosSession';

let https;
try {
  https = require('node:https');
} catch (err) {
  // unsupported, ignore
  // console.error('https support is disabled!');
}
// const https = require('node:https');

/**
 * Class that holds a bios session.
 *
 * An instance of this class holds user credentials, then the object executes authentication
 * whenever necessary.
 *
 * In order to call a SDK method, fetch a session using {@see getSession} method.
 */
export class BiosClient {

  /**
   * Constructs a biOS client.
   *
   * @param {object} config Session configuration
   * @property {string} config.endpoint Endpoint URL
   * @property {string} config.email User email
   * @property {string} config.password User password
   */
  constructor(config) {
    this.RECOVERABLE_ERRORS = new Set([
      BiosErrorType.CLIENT_CHANNEL_ERROR.errorNumber,
      BiosErrorType.SERVER_CONNECTION_FAILURE.errorNumber,
      BiosErrorType.SERVICE_UNAVAILABLE.errorNumber,
      BiosErrorType.BAD_GATEWAY.errorNumber,
      BiosErrorType.OPERATION_CANCELLED.errorNumber,
    ]);

    this._sessionConfig = config;
    this._session = BiosSession.initialize({
      endpoint: config.endpoint,
      nonGlobal: true,
    });

    // Configure axios to be used on Node.js
    const httpClient = this._session.httpClient;
    httpClient.defaults.httpsAgent = new https.Agent({ keepAlive: true });
    httpClient.defaults.headers['connection'] = 'keep-alive';
    httpClient.interceptors.response.use(
      (response) => {
        // Set up session token
        if (!!response.headers && response.headers['set-cookie']) {
          response.headers['set-cookie'].forEach((value) => {
            const parsed = cookie.parse(value);
            if (!!parsed.token) {
              httpClient.defaults.headers['authorization'] = `Bearer ${parsed.token}`;
            }
          });
        }
        return response;
      },
      (error) => {
        // Run login is the session is not authenticated
        const { config, response } = error;
        if (!response || response.status !== 401 || config.retry || config.isSignIn) {
          return Promise.reject(error);
        }
        delete httpClient.defaults.headers['authorization'];
        const retryConfig = {
          ...config,
          retry: true,
        }
        delete retryConfig.headers['authorization'];
        return this._session.signIn(this._sessionConfig)
          .then(() => httpClient.request(retryConfig));
      }
    );

    this.status = 'created';
  }

  /**
   * Creates and initializes a client.
   *
   * @static
   * @param {object} config Session configuration
   * @property {string} config.endpoint Endpoint URL
   * @property {string} config.email User email
   * @property {string} config.password User password
   * @returns {BiosClient} Created client
   */
  static start(config) {
    const client = new BiosClient(config);
    client.initialize();
    return client;
  }

  /**
   * Initializes the client.
   *
   * The object signs in to the biOS server using this method.
   *
   * @async
   * @returns {Promise<BiosSessionHttp>} biOS session
   */
  async initialize() {
    this.status = 'initializing';
    this.initError = null;
    try {
      const session = await this._session.signIn(this._sessionConfig);
      this.status = 'initialized';
      return session;
    } catch (e) {
      this.initError = e;
      this.status = 'failed';
    }
  }

  /**
   * Provides a biOS session.
   *
   * If the session is still in initialization, the method holds on until the initialization
   *  completes.
   *
   * @async
   * @returns {Promise<BiosSessionHttp>} biOS session
   */
  async getSession() {
    let numTrials = 20;
    let sleepTime = 10;
    let last = Date.now();
    while (true) {
      const now = Date.now();
      last = now;
      if (this.status === 'initialized') {
        return this._session;
      } else if (this.status === 'created') {
        this.initialize();
      } else if (numTrials === 0) {
        throw this.initError;
      } else if (this.status === 'failed') {
        if (this.RECOVERABLE_ERRORS.has(this.initError.errorNumber)) {
          this.initialize();
        } else {
          throw this.initError;
        }
      }
      await new Promise((r) => setTimeout(r, sleepTime));
      sleepTime *= 2;
      --numTrials;
    }
  }
}
