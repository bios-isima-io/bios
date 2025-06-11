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
package io.isima.bios.storage.cassandra;

import com.datastax.driver.core.ConsistencyLevel;
import io.isima.bios.common.BiosConfigBase;
import io.isima.bios.exceptions.ApplicationException;

public class CassandraConfig {
  public static final String CASSANDRA_CONTACT_POINTS = "io.isima.bios.db.contactPoints";
  public static final String CASSANDRA_USE_NETWORK_TOPOLOGY_STRATEGY =
      "io.isima.bios.db.useNetworkTopologyStrategy";
  public static final String CASSANDRA_DATA_CENTER = "io.isima.bios.db.datacenter";
  public static final String CASSANDRA_DATA_CENTERS = "io.isima.bios.db.datacenters";
  public static final String CASSANDRA_USED_HOSTS_PER_REMOTE_DC =
      "io.isima.bios.db.usedHostsPerRemoteDc";
  public static final String CASSANDRA_REPLICATION_FACTOR = "io.isima.bios.db.replicationFactor";
  public static final String CASSANDRA_CONSISTENCY_LEVEL = "io.isima.bios.db.consistencyLevel";
  public static final String CASSANDRA_SSL_ENABLED = "io.isima.bios.db.ssl.enabled";
  public static final String CASSANDRA_SSL_KEYSTORE_FILE = "io.isima.bios.db.ssl.keystore.file";
  public static final String CASSANDRA_SSL_KEYSTORE_PASSWORD =
      "io.isima.bios.db.ssl.keystore.password";
  public static final String CASSANDRA_SSL_TRUSTSTORE_FILE = "io.isima.bios.db.ssl.truststore.file";
  public static final String CASSANDRA_SSL_TRUSTSTORE_PASSWORD =
      "io.isima.bios.db.ssl.truststore.password";
  public static final String CASSANDRA_POOLING_LOGGING_INTERVAL =
      "io.isima.bios.db.pooling.loggingInterval";
  public static final String CASSANDRA_POOLING_REQUESTS_PER_CONNECTION_LOCAL =
      "io.isima.bios.db.pooling.requestsPerConnectionLocal";
  public static final String CASSANDRA_POOLING_REQUESTS_PER_CONNECTION_REMOTE =
      "io.isima.bios.db.pooling.requestsPerConnectionRemote";
  public static final String DB_PORT = "io.isima.bios.db.port";

  /** Cassandra client read timeout milliseconds. */
  public static final int CASSANDRA_READ_TIMEOUT_MILLIS = 180000;

  /** Maximum number of retries for initial Cassandra connection. */
  public static final int CASSANDRA_INITIAL_CONNECT_RETRY = 15;

  /** Cassandra connection retry initial backoff sleep milliseconds. */
  public static final long CASSANDRA_CONNECT_RETRY_BACKOFF_INITIAL_SLEEP_MILLIS = 1000;

  /** Cassandra connection retry backoff mechanism extends sleep time until this value. */
  public static final long CASSANDRA_CONNECT_RETRY_BACKOFF_LIMIT_MILLIS = 32000;

  protected static BiosConfigBase getInstance() {
    return BiosConfigBase.getInstance();
  }

  /** Cassandra contact points. */
  public static String[] cassandraContactPoints() {
    return getInstance().getStringArray(CASSANDRA_CONTACT_POINTS, ",", "127.0.0.1");
  }

  /** Cassandra: Use NetworkTopologyStrategy for replication strategy. */
  public static boolean cassandraUseNetworkTopologyStrategy() {
    return getInstance().getBoolean(CASSANDRA_USE_NETWORK_TOPOLOGY_STRATEGY, false);
  }

  /**
   * Cassandra: Datacenter name for a keyspace.
   *
   * <p>The value is ignored when replication strategy is SimpleStrategy or CASSANDRA_DATA_CENTERS
   * are set.
   */
  public static String cassandraDataCenter() {
    return getInstance().getString(CASSANDRA_DATA_CENTER, null);
  }

  /**
   * Cassandra: Datacenter names and replications for a keyspace.
   *
   * <p>The format is list of &lt;datacenter_name&gt;:&lt;replication_factor&gt; separated by comma.
   *
   * <p>The value is ignored when replication strategy is SimpleStrategy.
   */
  public static String cassandraDataCenters() {
    return getInstance().getString(CASSANDRA_DATA_CENTERS, null);
  }

  /** Maximum number of remote hosts used for DCAwareRoundRobinPolicy (default=2). */
  public static int usedHostsPerRemoteDc() {
    return getInstance().getInt(CASSANDRA_USED_HOSTS_PER_REMOTE_DC, 2, 0, 16);
  }

  /** Cassandra replication factor. */
  public static int cassandraReplicationFactor() {
    return getInstance().getInt(CASSANDRA_REPLICATION_FACTOR, 1);
  }

  /** Cassandra Coneection SSL enabled. */
  public static Boolean sslEnabled() {
    return getInstance().getBoolean(CASSANDRA_SSL_ENABLED, false);
  }

  public static String sslKeystoreFile() {
    return getInstance()
        .getString(CASSANDRA_SSL_KEYSTORE_FILE, "/var/ext_resoruces/db.pks12.keystore");
  }

  public static String sslKeystorePassword() {
    return getInstance().getString(CASSANDRA_SSL_KEYSTORE_PASSWORD, "secret");
  }

  public static String sslTruststoreFile() {
    return getInstance()
        .getString(CASSANDRA_SSL_TRUSTSTORE_FILE, "/var/ext_resoruces/db.truststore");
  }

  public static String sslTruststorePassword() {
    return getInstance().getString(CASSANDRA_SSL_TRUSTSTORE_PASSWORD, "secret");
  }

  /**
   * Interval seconds for Cassandra pooling logging. The logging is disabled if the value is equal
   * or less than zero. (default: 0)
   */
  public static int cassandraPoolingLoggingInterval() {
    return getInstance().getInt(CASSANDRA_POOLING_LOGGING_INTERVAL, 0);
  }

  /**
   * Number of maximum in-flight queries per connection for a local-distance host. (default: 16384)
   */
  public static int cassandraPoolingRequestsPerConnectionLocal() {
    return getInstance().getInt(CASSANDRA_POOLING_REQUESTS_PER_CONNECTION_LOCAL, 16384);
  }

  /**
   * Number of maximum in-flight queries per connection for a remote-distance host. (default: 4096)
   */
  public static int cassandraPoolingRequestsPerConnectionRemote() {
    return getInstance().getInt(CASSANDRA_POOLING_REQUESTS_PER_CONNECTION_REMOTE, 4096);
  }

  /**
   * Cassandra default consistency level.
   *
   * @throws ApplicationException when unknown consistency level is specified.
   */
  public static ConsistencyLevel cassandraConsistencyLevel() throws ApplicationException {
    String src = getInstance().getString(CASSANDRA_CONSISTENCY_LEVEL, "QUORUM");
    try {
      return ConsistencyLevel.valueOf(src.toUpperCase());
    } catch (IllegalArgumentException e) {
      String message =
          String.format(
              "Invalid property: %s=%s: Unknown consistency level",
              CASSANDRA_CONSISTENCY_LEVEL, src);
      throw new ApplicationException(message, e);
    }
  }

  /** DB Port. */
  public static int getDbPort() {
    return getInstance().getInt(DB_PORT, 10109);
  }
}
