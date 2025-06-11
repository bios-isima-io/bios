/*
 * Copyright (C) 2025 Isima, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.isima.bios.configuration;

import io.isima.bios.common.BiosConfigBase;
import io.isima.bios.service.CorsOriginWhiteList;
import java.util.Properties;

/**
 * BiosServer V2 server static configurations.
 *
 * <p>The configuration is specified as properties. The process looks for configuration parameters
 * from following places on startup, listed in priority order:
 *
 * <ul>
 *   <li>System property
 *   <li>${BIOS_HOME}/configuration/server.options
 * </ul>
 */
public class Bios2Config {
  // io.isima.bios.server /////////////////////////////////////////////////////////////
  public static final String SERVER_ADDRESS = "io.isima.bios.server.address";
  public static final String SERVER_PORT = "io.isima.bios.server.port";
  public static final String SSL_KEYSTORE_FILE = "io.isima.bios.server.ssl.keystore.file";
  public static final String SSL_KEYSTORE_PASSWORD = "io.isima.bios.server.ssl.keystore.password";
  public static final String SSL_KEYSTORE_TYPE = "io.isima.bios.server.ssl.keystore.type";
  public static final String NIO_LOGGING_ENABLED = "io.isima.bios.server.nioLogging.enabled";
  public static final String IO_NUM_THREADS = "io.isima.bios.server.io.threads";
  public static final String IO_THREADS_AFFINITY = "io.isima.bios.server.io.threads.affinity";
  public static final String ADMIN_NUM_THREADS = "io.isima.bios.server.admin.threads";
  public static final String DIGESTOR_NUM_THREADS = "io.isima.bios.server.digestor.threads";
  public static final String DIGESTOR_THREADS_AFFINITY =
      "io.isima.bios.server.digestor.threads.affinity";

  /**
   * CORS Access-Control-Allow-Origin origin white list (comma separated). Set asterisk ("*") to
   * allow-all.
   */
  public static final String CORS_ALLOW_ORIGIN_WHITELIST =
      "io.isima.bios.server.cors.origin.whitelist";

  public static final String CORS_ALLOW_HEADERS = "io.isima.bios.server.cors.allowHeaders";
  public static final String CORS_MAX_AGE = "io.isima.bios.server.cors.maxAge";

  // io.isima.bios.server.2nd //////////////////////////////////////////////////////////
  public static final String SERVER_PORT_2ND = "io.isima.bios.server.2nd.port";
  public static final String SSL_KEYSTORE_FILE_2ND = "io.isima.bios.server.2nd.ssl.keystore.file";
  public static final String SSL_KEYSTORE_PASSWORD_2ND =
      "io.isima.bios.server.2nd.ssl.keystore.password";
  public static final String SSL_KEYSTORE_TYPE_2ND = "io.isima.bios.server.2nd.ssl.keystore.type";

  // io.isima.bios.server.http3 ////////////////////////////////////////////////////////
  public static final String HTTP3_ENABLED = "io.isima.bios.server.http3.enabled";
  public static final String HTTP3_PORT = "io.isima.bios.server.http3.port";
  public static final String QUIC_KEYSTORE_FILE = "io.isima.bios.server.quic.keystore.file";
  public static final String HTTP3_MAX_IDLE_TIMEOUT =
      "io.isima.bios.server.quic.maxIdleTimeoutMillis";
  public static final String HTTP3_INITIAL_MAX_DATA = "io.isima.bios.server.quic.initialMaxData";
  public static final String HTTP3_INITIAL_MAX_STREAM_DATA_LOCAL =
      "io.isima.bios.server.quic.initialMaxStreamDataBidirectionalLocal";
  public static final String HTTP3_INITIAL_MAX_STREAM_DATA_REMOTE =
      "io.isima.bios.server.quic.initialMaxStreamDataBidirectionalRemote";
  public static final String HTTP3_INITIAL_MAX_STREAMS =
      "io.isima.bios.server.quic.initialMaxStreamsBidirectional";

  // io.isima.bios.data /////////////////////////////////////////////////////////////
  public static final String REDUCER_ENABLED = "io.isima.bios.data.reducer.enabled";
  public static final String REDUCER_INTERVAL_SECONDS = "io.isima.bios.data.reducer.interval";
  public static final String REDUCER_MAX_CONCURRENCY = "io.isima.bios.data.reducer.maxConcurrency";

  // User access control ////////////////////////////////////////////////////////////
  public static final String AUTH_EXPIRATION_TIME = "io.isima.bios.auth.timeout";
  public static final int MINIMUM_AUTH_EXPIRATION = 15000; // 15 seconds
  // Initial users file location.
  public static final String INITIAL_USERS_FILE = "io.isima.bios.initialUsersFile";

  // TODO(Naoki): Store this to shared config
  public static final String EXECUTOR_CONFIG_FILE = "io.isima.bios.executor.configFile";

  public static final String INSERT_BULK_CONCURRENCY =
      "io.isima.bios.data.insertBulkMaxConcurrency";
  public static final String INDEXING_DEFAULT_INTERVAL_SECONDS =
      "io.isima.bios.data.indexing.defaultInterval";

  // Misc ///////////////////////////////////////////////////////////////////////////
  public static final String HEADER_DUMP_ENABLED = "io.isima.bios.headerDump.enabled";
  public static final String SERVICE_EXTENSIONS_LOADER_CLASS =
      "io.isima.bios.service.extensions.class";

  public static final String COUNTERS_MICROSERVICE_ENABLED =
      "io.isima.bios.microservice.counters.enabled";
  public static final String PRODUCTS_MICROSERVICE_ENABLED =
      "io.isima.bios.microservice.products.enabled";
  public static final String RECOMMENDATION_MICROSERVICE_ENABLED =
      "io.isima.bios.microservice.recommendation.enabled";

  public static final String APPS_SUPERVISOR_XMLRPC_USER = "io.isima.bios.apps.xmlrpc.user";
  public static final String APPS_SUPERVISOR_XMLRPC_PASSWORD = "io.isima.bios.apps.xmlrpc.password";

  /**
   * Turns on test mode -- the key enables special features for test such as test service, online
   * session expiry configuration, etc.
   */
  public static final String TEST_MODE = "io.isima.bios.test.enabled";

  private static Bios2Config instance;

  private static Bios2Config getInstance() {
    if (instance == null) {
      synchronized (Bios2Config.class) {
        instance = new Bios2Config();
      }
    }
    return instance;
  }

  public static void setProperties(Properties properties) {
    BiosConfigBase.setProperties(properties);
  }

  private final BiosConfigBase base;

  // cached values
  private final CorsOriginWhiteList corsOriginWhiteList;
  Boolean headerDumpEnabled;
  Boolean testMode;

  private Bios2Config() {
    base = BiosConfigBase.getInstance();
    corsOriginWhiteList =
        new CorsOriginWhiteList(base.getStringArray(CORS_ALLOW_ORIGIN_WHITELIST, ",", "*"));
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////

  /** Server listener address, default=0.0.0.0. */
  public static String serverAddress() {
    return getInstance().base.getString(SERVER_ADDRESS, "0.0.0.0");
  }

  /** Server port number, default=8443. */
  public static int serverPort() {
    return getInstance().base.getInt(SERVER_PORT, 8443);
  }

  /** Server SSL keystore file, default=$BIOS_HOME/configuration/server.p12. */
  public static String sslKeyStoreFile() {
    return getInstance().base.getString(SSL_KEYSTORE_FILE, "configuration/server.p12");
  }

  /** Server SSL keystore password, default=secret. */
  public static String sslKeyStorePassword() {
    return getInstance().base.getString(SSL_KEYSTORE_PASSWORD, "secret");
  }

  /** Server SSL keystore type, default=PKCS12. */
  public static String sslKeyStoreType() {
    return getInstance().base.getString(SSL_KEYSTORE_TYPE, "PKCS12");
  }

  /** Server 2nd port number, default=-1. */
  public static int serverPort2nd() {
    return getInstance().base.getInt(SERVER_PORT_2ND, -1);
  }

  /** Server 2nd SSL keystore file, default=$BIOS_HOME/configuration/server.p12. */
  public static String sslKeyStoreFile2nd() {
    return getInstance().base.getString(SSL_KEYSTORE_FILE_2ND, "configuration/server.p12");
  }

  /** Server 2nd SSL keystore password, default=secret. */
  public static String sslKeyStorePassword2nd() {
    return getInstance().base.getString(SSL_KEYSTORE_PASSWORD_2ND, "secret");
  }

  /** Server SSL keystore type, default=PKCS12. */
  public static String sslKeyStoreType2nd() {
    return getInstance().base.getString(SSL_KEYSTORE_TYPE_2ND, "PKCS12");
  }

  /** Whether to enable Netty logging handler, default=false. */
  public static boolean nioLoggingEnabled() {
    return getInstance().base.getBoolean(NIO_LOGGING_ENABLED, false);
  }

  /** Executor configuration file name, default=null that reads internal config. */
  public static String executorConfigFile() {
    return getInstance().base.getString(EXECUTOR_CONFIG_FILE, null);
  }

  /** Number of I/O threads, default=0. */
  public static int ioNumThreads() {
    return getInstance().base.getInt(IO_NUM_THREADS, 0);
  }

  /** CPU IDs for I/O threads affinity, e.g. 5-7,13-15, default is none. */
  public static String ioThreadsAffinity() {
    return getInstance().base.getString(IO_THREADS_AFFINITY, "");
  }

  /** Number of admin threads, default=1. */
  public static int adminNumThreads() {
    return getInstance().base.getInt(ADMIN_NUM_THREADS, 1);
  }

  /** Number of digestor threads, default=0. */
  public static int digestorNumThreads() {
    return getInstance().base.getInt(DIGESTOR_NUM_THREADS, 0);
  }

  /** CPU IDs for I/O threads affinity, e.g. 5-7,13-15, default is none. */
  public static String digestorThreadsAffinity() {
    return getInstance().base.getString(DIGESTOR_THREADS_AFFINITY, "");
  }

  /**
   * CORS Access-Control-Allow-Headers values (default="accept, authorization, content-type,
   * x-requested-with, xsrfToken, x-bios-version, x-tenant-name, x-user-name, x-invalidate-cache").
   */
  public static String corsAllowHeaders() {
    return getInstance()
        .base
        .getString(
            CORS_ALLOW_HEADERS,
            "accept, authorization, content-type, x-requested-with, xsrfToken,"
                + " x-bios-version, x-tenant-name, x-user-name, x-invalidate-cache");
  }

  /** CORS Access-Control-Max-Age value (default=1). */
  public static String corsMaxAge() {
    return getInstance().base.getString(CORS_MAX_AGE, "1");
  }

  /** Whether HTTP/3 listener is enabled (default=false). */
  public static boolean isHttp3Enabled() {
    return getInstance().base.getBoolean(HTTP3_ENABLED, false);
  }

  /** Whether HTTP/3 listener port (default=4433). */
  public static int http3Port() {
    return getInstance().base.getInt(HTTP3_PORT, 4433, 1, 65535);
  }

  /**
   * QUIC key store file for HTTP/3 listener (default: value of
   * io.isima.bios.server.ssl.keystore.file).
   */
  public static String quicKeyStoreFile() {
    return getInstance().base.getString(QUIC_KEYSTORE_FILE, sslKeyStoreFile());
  }

  /** QUIC max idle timeout in milliseconds (default=600000). */
  public static long quicMaxIdleTimeoutMillis() {
    return getInstance().base.getLong(HTTP3_MAX_IDLE_TIMEOUT, 600000L);
  }

  /** QUIC initial maximum data for flow control (default=10000000). */
  public static long quicInitialMaxData() {
    return getInstance().base.getLong(HTTP3_INITIAL_MAX_DATA, 10000000L);
  }

  /** QUIC initial maximum stream data from local for flow control (default=10000000). */
  public static long quicInitialMaxStreamDataLocal() {
    return getInstance().base.getLong(HTTP3_INITIAL_MAX_STREAM_DATA_LOCAL, 10000000L);
  }

  /** QUIC initial maximum stream data from remote for flow control (default=10000000). */
  public static long quicInitialMaxStreamDataRemote() {
    return getInstance().base.getLong(HTTP3_INITIAL_MAX_STREAM_DATA_REMOTE, 10000000L);
  }

  /** QUIC initial maximum bi-directional streams for flow control (default=8192). */
  public static long quicInitialMaxStreamsBidirectional() {
    return getInstance().base.getLong(HTTP3_INITIAL_MAX_STREAMS, 8192L);
  }

  /** Whether the reducer is enabled, default=true. */
  public static boolean reducerEnabled() {
    return getInstance().base.getBoolean(REDUCER_ENABLED, true);
  }

  /** Reducer interval in seconds (default=10). */
  public static int reducerIntervalSeconds() {
    return getInstance().base.getInt(REDUCER_INTERVAL_SECONDS, 10);
  }

  /** Maximum reducer task concurrency (default=2). */
  public static int reducerMaxConcurrency() {
    return getInstance().base.getInt(REDUCER_MAX_CONCURRENCY, 8);
  }

  /** CORS Access-Control-Allow-Origin origin white list. */
  public static CorsOriginWhiteList getCorsOriginWhiteList() {
    return getInstance().corsOriginWhiteList;
  }

  public static int authExpirationTimeMillis() {
    return Math.max(
        getInstance().base.getInt(AUTH_EXPIRATION_TIME, 10 * 60 * 1000), MINIMUM_AUTH_EXPIRATION);
  }

  /** Initial users file name, default=null. */
  public static String initialUsersFile() {
    return getInstance().base.getString(INITIAL_USERS_FILE, null);
  }

  /** Maximum allowed concurrency in a insert bulk operation (default=128). */
  public static int insertBulkMaxConcurrency() {
    return getInstance().base.getInt(INSERT_BULK_CONCURRENCY, 128);
  }

  /** Default indexing interval when not specified, default=300. */
  public static int indexingDefaultIntervalMillis() {
    return getInstance().base.getInt(INDEXING_DEFAULT_INTERVAL_SECONDS, 300) * 1000;
  }

  public static boolean isHeaderDumpEnabled() {
    final var instance = getInstance();
    if (instance.headerDumpEnabled == null) {
      final var src = instance.base.getStringReplacedByEnv(HEADER_DUMP_ENABLED, "false");
      instance.headerDumpEnabled = Boolean.parseBoolean(src);
    }
    return instance.headerDumpEnabled;
  }

  public static String getServiceExtensionsLoaderClassName() {
    return getInstance().base.getString(SERVICE_EXTENSIONS_LOADER_CLASS, "");
  }

  public static boolean isCountersMicroserviceEnabled() {
    final var src =
        getInstance().base.getStringReplacedByEnv(COUNTERS_MICROSERVICE_ENABLED, "false");
    return Boolean.parseBoolean(src);
  }

  public static boolean isProductsMicroserviceEnabled() {
    final var src =
        getInstance().base.getStringReplacedByEnv(PRODUCTS_MICROSERVICE_ENABLED, "false");
    return Boolean.parseBoolean(src);
  }

  public static boolean isRecommendationMicroserviceEnabled() {
    final var src =
        getInstance().base.getStringReplacedByEnv(RECOMMENDATION_MICROSERVICE_ENABLED, "false");
    return Boolean.parseBoolean(src);
  }

  public static String getAppsXmlrpcUser() {
    return getInstance().base.getString(APPS_SUPERVISOR_XMLRPC_USER, "isima");
  }

  public static String getAppsXmlrpcPassword() {
    return getInstance().base.getString(APPS_SUPERVISOR_XMLRPC_PASSWORD, "");
  }

  /** Flag to indicate that the execution is in test mode (default=false). */
  public static boolean isTestMode() {
    final var instance = getInstance();
    if (instance.testMode == null) {
      instance.testMode = instance.base.getBoolean(TEST_MODE, false);
    }
    return instance.testMode;
  }

  /** Gets an integer value. */
  public static int getInteger(String key, int defaultValue) {
    return getInstance().base.getInt(key, defaultValue);
  }
}
