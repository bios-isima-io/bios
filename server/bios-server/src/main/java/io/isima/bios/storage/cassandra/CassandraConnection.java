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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.QueryConsistencyException;
import com.datastax.driver.core.exceptions.ServerError;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.extras.codecs.date.SimpleTimestampCodec;
import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.Utils;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Establish connection to Cassandra instances. */
public class CassandraConnection {
  private static final Logger logger = LoggerFactory.getLogger(CassandraConnection.class);

  private Cluster cluster;
  private Session session;

  boolean isShutdown;
  private AtomicInteger usersCount = new AtomicInteger();
  private final Map<String, String> users; // for debugging
  private final Lock lock;
  private final Condition condition;

  private final ScheduledExecutorService scheduled;
  private ScheduledFuture<?> poolStatusReportFuture;

  /** Initialization method called by DI framework. */
  public CassandraConnection() {
    logger.debug("CassandraConnection initializing ###");
    isShutdown = false;
    usersCount.set(0);
    users = new ConcurrentHashMap<>();
    lock = new ReentrantLock();
    condition = lock.newCondition();
    boolean initialConnect = false;
    try {
      int retry = CassandraConfig.CASSANDRA_INITIAL_CONNECT_RETRY;
      long sleep = CassandraConfig.CASSANDRA_CONNECT_RETRY_BACKOFF_INITIAL_SLEEP_MILLIS;
      while (true) {
        try {
          cluster = buildCluster(TfosConfig.getDbAuthUser(), TfosConfig.getDbAuthPassword());
          cluster.getConfiguration().getCodecRegistry().register(SimpleTimestampCodec.instance);
          cluster.getConfiguration().getCodecRegistry().register(LocalDateCodec.instance);
          cluster
              .getConfiguration()
              .getSocketOptions()
              .setReadTimeoutMillis(CassandraConfig.CASSANDRA_READ_TIMEOUT_MILLIS);
          session = cluster.connect();
          logger.info("Established connection to Cassandra");
          break;
        } catch (NoHostAvailableException ex) {
          if (retry-- > 0) {
            logger.warn("Cannot connect to Cassandra. Scheduling retry in {} ms", sleep);
            try {
              Thread.sleep(sleep);
              // extend back off interval up to 16 seconds.
              if (sleep < CassandraConfig.CASSANDRA_CONNECT_RETRY_BACKOFF_LIMIT_MILLIS) {
                sleep *= 2;
              }
            } catch (InterruptedException e) {
              logger.error("Interrupted CassandraConnection startup sequence. Aborting");
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
          } else {
            String message = "Cassandra does not come up. Initialization Failed";
            logger.error(message);
            throw new RuntimeException(message);
          }
        }
      }
    } catch (AuthenticationException e) {
      initialConnect = true;
    }
    if (initialConnect) {
      // Create the TFOS super user from the default user, reconnect, and drop the default user.
      createSuperUser();
      cluster = buildCluster(TfosConfig.getDbAuthUser(), TfosConfig.getDbAuthPassword());
      session = cluster.connect();
      session.execute(CassandraConstants.DROP_ROLE + CassandraConstants.DEFAULT_USERNAME);
    }
    try {
      createKeyspace(CassandraConstants.KEYSPACE_ADMIN);
    } catch (ApplicationException e) {
      logger.error("Creating tfos admin keyspace failed", e);
      throw new RuntimeException(e);
    }
    try {
      createKeyspace(CassandraConstants.KEYSPACE_BI);
    } catch (ApplicationException e) {
      logger.error("Creating bi meta keyspace failed", e);
      throw new RuntimeException(e);
    }
    // TODO(BB-400): Include these to DB metrics and eliminate the logging
    final int loggingInterval = CassandraConfig.cassandraPoolingLoggingInterval();
    if (loggingInterval > 0) {
      scheduled = Executors.newScheduledThreadPool(1);
      poolStatusReportFuture =
          scheduled.scheduleAtFixedRate(
              new Runnable() {
                @Override
                public void run() {
                  final Session.State state = session.getState();
                  final LoadBalancingPolicy loadBalancingPolicy =
                      cluster.getConfiguration().getPolicies().getLoadBalancingPolicy();
                  final PoolingOptions poolingOptions =
                      cluster.getConfiguration().getPoolingOptions();
                  for (Host host : state.getConnectedHosts()) {
                    final HostDistance distance = loadBalancingPolicy.distance(host);
                    int connections = state.getOpenConnections(host);
                    int inFlightQueries = state.getInFlightQueries(host);
                    logger.info(
                        "Pool Usage: host={} distance={} connections={}, inFlightQueries={}, capacity={}",
                        host,
                        distance.name(),
                        connections,
                        inFlightQueries,
                        connections * poolingOptions.getMaxRequestsPerConnection(distance));
                  }
                }
              },
              loggingInterval,
              loggingInterval,
              TimeUnit.SECONDS);
    } else {
      scheduled = null;
    }
    logger.debug("### CassandraConnection initialized");
  }

  /**
   * Builds a cluster.
   *
   * @param username Cassandra user name
   * @param password Cassandra password
   * @return Built cluster
   */
  private Cluster buildCluster(String username, String password) {
    Cluster.Builder builder = Cluster.builder().withPort(CassandraConfig.getDbPort());
    for (String contactPoint : CassandraConfig.cassandraContactPoints()) {
      builder.addContactPoint(contactPoint);
    }

    final ConsistencyLevel cl;
    try {
      cl = CassandraConfig.cassandraConsistencyLevel();
    } catch (ApplicationException e) {
      logger.error("Cassandra client initialization failure", e);
      throw new RuntimeException(e);
    }
    logger.debug("Consistency level is set to {}", cl);

    final PoolingOptions poolingOptions =
        new PoolingOptions()
            .setConnectionsPerHost(HostDistance.LOCAL, 1, 4)
            .setConnectionsPerHost(HostDistance.REMOTE, 1, 4)
            .setMaxRequestsPerConnection(
                HostDistance.LOCAL, CassandraConfig.cassandraPoolingRequestsPerConnectionLocal())
            .setMaxRequestsPerConnection(
                HostDistance.REMOTE, CassandraConfig.cassandraPoolingRequestsPerConnectionRemote())
            .setMaxQueueSize(512)
            .setPoolTimeoutMillis(10000);

    builder =
        builder
            .withLoadBalancingPolicy(
                DCAwareRoundRobinPolicy.builder()
                    .withUsedHostsPerRemoteDc(CassandraConfig.usedHostsPerRemoteDc())
                    .build())
            .withQueryOptions(
                new QueryOptions().setConsistencyLevel(cl).setDefaultIdempotence(true))
            .withAuthProvider(new PlainTextAuthProvider(username, password))
            .withPoolingOptions(poolingOptions)
            .withRetryPolicy(new TfosLoggingRetryPolicy(new TfosRetryPolicy()));

    if (CassandraConfig.sslEnabled()) {
      try {
        KeyManager[] kms =
            getKeyManagers(
                CassandraConfig.sslKeystoreFile(), CassandraConfig.sslKeystorePassword());
        TrustManager[] tms =
            getTrustManagers(
                CassandraConfig.sslTruststoreFile(), CassandraConfig.sslTruststorePassword());
        final var sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(kms, tms, null);
        final var sslOptions =
            RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();
        builder = builder.withSSL(sslOptions);
      } catch (IOException | GeneralSecurityException e) {
        throw new RuntimeException("Failed to start CassandraConnection", e);
      }
    }

    return builder.build();
  }

  private static TrustManager[] getTrustManagers(String location, String password)
      throws IOException, GeneralSecurityException {
    // First, get the default TrustManagerFactory.
    String alg = TrustManagerFactory.getDefaultAlgorithm();
    TrustManagerFactory tmFact = TrustManagerFactory.getInstance(alg);

    try (final var fis = new FileInputStream(location)) {
      KeyStore ks = KeyStore.getInstance("jks");
      ks.load(fis, password.toCharArray());
      fis.close();

      tmFact.init(ks);

      // And now get the TrustManagers
      TrustManager[] tms = tmFact.getTrustManagers();
      return tms;
    }
  }

  private static KeyManager[] getKeyManagers(String location, String password)
      throws IOException, GeneralSecurityException {
    // First, get the default KeyManagerFactory.
    String alg = KeyManagerFactory.getDefaultAlgorithm();
    KeyManagerFactory kmFact = KeyManagerFactory.getInstance(alg);

    try (final var fis = new FileInputStream(location)) {
      KeyStore ks = KeyStore.getInstance("jks");
      ks.load(fis, password.toCharArray());
      fis.close();

      // Now we initialise the KeyManagerFactory with this KeyStore
      kmFact.init(ks, password.toCharArray());

      // And now get the KeyManagers
      KeyManager[] kms = kmFact.getKeyManagers();
      return kms;
    }
  }

  /** Create new superuser for tfos. */
  private void createSuperUser() {
    logger.info("Creating a role for the service");
    long start = System.currentTimeMillis();
    Session tempSession = null;
    while (tempSession == null) {
      try {
        tempSession =
            buildCluster(CassandraConstants.DEFAULT_USERNAME, CassandraConstants.DEFAULT_PASSWORD)
                .connect();
      } catch (AuthenticationException e) {
        // retry up to 60 seconds
        if (System.currentTimeMillis() - 60 * 1000 < start) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            // Do nothing
            Thread.currentThread().interrupt();
          }
        } else {
          throw e;
        }
      }
    }
    tempSession.execute(
        String.format(
            CassandraConstants.FORMAT_CREATE_ROLE,
            TfosConfig.getDbAuthUser(),
            TfosConfig.getDbAuthPassword()));
  }

  /**
   * Get cluster object.
   *
   * @return the cluster
   */
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * Get session object.
   *
   * @return the session
   */
  public Session getSession() {
    return session;
  }

  /**
   * Create keyspace.
   *
   * @param name Keyspace name
   */
  public void createKeyspace(String name) throws ApplicationException {
    createKeyspace(name, CassandraConfig.cassandraReplicationFactor());
  }

  /**
   * Generic method to create a keyspace.
   *
   * @param name Keyspace name
   * @param replicationFactor Replication factor.
   */
  public void createKeyspace(String name, int replicationFactor) throws ApplicationException {
    final String query = makeCreateKeyspaceQuery(name, replicationFactor);
    final Statement statement =
        new SimpleStatement(query)
            .setReadTimeoutMillis(CassandraConfig.CASSANDRA_READ_TIMEOUT_MILLIS);
    try {
      try {
        session.execute(statement);
      } catch (OperationTimedOutException | InvalidConfigurationInQueryException e) {
        logger.warn("creating keyspace failed, retrying after 5 seconds; error={}", e.getMessage());
        Thread.sleep(TfosConfig.CASSANDRA_CREATE_RETRY_SLEEP_MILLIS);
        if (!verifyKeyspace(name)) {
          session.execute(statement);
        }
      }
    } catch (InterruptedException e) {
      String message =
          String.format("creating keyspace %s interrupted RF=%d", name, replicationFactor);
      Thread.currentThread().interrupt();
      throw new ApplicationException(message, e);
    } catch (Throwable t) {
      String message = String.format("creating keyspace %s failed RF=%d", name, replicationFactor);
      throw new ApplicationException(message, t);
    }
  }

  public Map<String, String> getTableColumns(
      String keyspaceName, String tableName, Logger logger, Predicate<String> columnFilter)
      throws ApplicationException {
    final var statement =
        "SELECT column_name, type FROM system_schema.columns"
            + " WHERE keyspace_name = ? AND table_name = ?";
    final var resultSet = execute("Get table columns", logger, statement, keyspaceName, tableName);

    Map<String, String> columns = null;
    while (!resultSet.isExhausted()) {
      if (columns == null) {
        columns = new HashMap<>();
      }
      final var row = resultSet.one();
      final String name = row.getString("column_name");
      final String type = row.getString("type");
      if (columnFilter.test(name)) {
        columns.put(name, type);
      }
    }
    return columns;
  }

  protected static String makeCreateKeyspaceQuery(String name, int replicationFactor)
      throws ApplicationException {
    logger.debug("creating keyspace {} with replication factor {}", name, replicationFactor);
    final String strategy;
    String rfProperty;
    final String simpleReplFormat = "{'class': '%s', '%s': '%d'}";
    final String replication;
    if (CassandraConfig.cassandraUseNetworkTopologyStrategy()) {
      strategy = "NetworkTopologyStrategy";
      final String datacenters = CassandraConfig.cassandraDataCenters();
      if (datacenters != null) {
        StringBuilder sb = new StringBuilder("{'class': '").append(strategy).append("'");
        String[] entries = datacenters.trim().split(",");
        for (String entry : entries) {
          final String[] pair = entry.split(":");
          final String dc = pair[0].trim();
          final String rf = pair.length > 1 ? pair[1] : Integer.toString(replicationFactor);
          sb.append(", '").append(dc).append("': '").append(rf).append("'");
        }
        replication = sb.append("}").toString();
      } else {
        // use simple format
        rfProperty = CassandraConfig.cassandraDataCenter();
        if (rfProperty == null) {
          throw new ApplicationException(
              CassandraConfig.CASSANDRA_DATA_CENTER
                  + " must be set when NetworkTopologyStrategy is used");
        }
        replication = String.format(simpleReplFormat, strategy, rfProperty, replicationFactor);
      }
    } else {
      strategy = "SimpleStrategy";
      rfProperty = "replication_factor";
      replication = String.format(simpleReplFormat, strategy, rfProperty, replicationFactor);
    }

    final String query =
        String.format(CassandraConstants.FORMAT_CREATE_KEYSPACE, name, replication);
    logger.debug("createKeyspace query={}", query);
    return query;
  }

  /**
   * Method to verify if a keyspace is visible.
   *
   * @param keyspaceName Target keyspace name
   * @return true if specified keyspace is visible, false otherwise
   */
  public boolean verifyKeyspace(String keyspaceName) {
    final String statement = "SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?";
    ResultSet result = session.execute(statement, keyspaceName);
    if (result.isExhausted()) {
      logger.info("keyspace {} is missing", keyspaceName);
      return false;
    } else {
      Row row = result.one();
      logger.debug(row.toString());
      return true;
    }
  }

  /** Method to drop a keyspace. */
  public void dropKeyspace(String keyspaceName) {
    final String dropKeyspaceStatementString =
        String.format(CassandraConstants.FORMAT_DELETE_KEYSPACE, keyspaceName);
    Statement dropKeyspaceStatement = new SimpleStatement(dropKeyspaceStatementString);
    try {
      execute("Dropping keyspace " + keyspaceName, logger, dropKeyspaceStatement);
    } catch (ApplicationException e) {
      logger.warn("Got exception {} while dropping keyspace {}", e, keyspaceName);
    }
  }

  /** Method to drop a table. */
  public void dropTable(String keyspace, String tableName) {
    String qualifiedTableName = keyspace + "." + tableName;
    final String dropTableStatementString = String.format("DROP TABLE %s", qualifiedTableName);
    Statement dropTableStatement = new SimpleStatement(dropTableStatementString);
    try {
      logger.info("Dropping table {}", qualifiedTableName);
      execute("Dropping table " + qualifiedTableName, logger, dropTableStatement);
    } catch (ApplicationException e) {
      logger.warn("Got exception {} while dropping table {}", e, qualifiedTableName);
    }
  }

  /**
   * Execute a statement.
   *
   * <p>The method retries the operation when recoverable errors happen.
   *
   * @param message Message logged when the operation fails.
   * @param logger Logger to be used for logging.
   * @param statement Statement to execute.
   * @return Result of the execution.
   * @throws ApplicationException when operation fails.
   */
  public ResultSet execute(String message, Logger logger, Statement statement)
      throws ApplicationException {
    final RetryHandler retryHandler = new RetryHandler(message, logger, "");
    while (true) {
      try {
        assert !ExecutorManager.isInIoThread();
        return session.execute(statement);
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }
  }

  /**
   * Execute a statement.
   *
   * <p>The method retries the operation when recoverable errors happen.
   *
   * @param message Message logged when the operation fails.
   * @param logger Logger to be used for logging.
   * @param query Query to execute
   * @param values Query values
   * @return Result of the execution.
   * @throws ApplicationException when operation fails.
   */
  public ResultSet execute(String message, Logger logger, String query, Object... values)
      throws ApplicationException {
    final RetryHandler retryHandler = new RetryHandler(message, logger, "");
    while (true) {
      try {
        assert !ExecutorManager.isInIoThread();
        return session.execute(query, values);
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }
  }

  /**
   * Executes a statement asynchronously.
   *
   * @param session Cassandra session
   * @param statement Statement to execute
   * @param state The execution state
   * @return Completion stage for the result set
   */
  public static CompletionStage<ResultSet> executeAsync(
      Session session, Statement statement, ExecutionState state) {
    final var future = new CompletableFuture<ResultSet>();
    final var executor = state.getExecutor();
    final var resultFuture =
        Futures.transformAsync(session.executeAsync(statement), iterate(executor), executor);
    Futures.addCallback(
        resultFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(ResultSet rows) {
            state.endDbAccess();
            future.complete(rows);
          }

          @Override
          public void onFailure(Throwable throwable) {
            state.markError();
            logger.error("Cassandra query error; error={}, statement={}", throwable, statement);
            future.completeExceptionally(throwable);
          }
        },
        state.getExecutor());
    return future;
  }

  /**
   * Executes a statement asynchronously.
   *
   * @param statement Statement to execute
   * @param state Execution state
   * @return Completion stage for the result set
   */
  public CompletionStage<ResultSet> executeAsync(Statement statement, ExecutionState state) {
    return executeAsync(session, statement, state);
  }

  /**
   * Executes a statement asynchronously.
   *
   * @param statement Statement to execute
   * @param state The execution state
   * @param acceptor Result acceptor
   * @param errorHandler Error handler
   * @return Completion stage for the result set
   */
  public void executeAsync(
      Statement statement,
      ExecutionState state,
      Consumer<ResultSet> acceptor,
      Consumer<Throwable> errorHandler) {
    final var executor = state.getExecutor();
    final var resultFuture =
        Futures.transformAsync(session.executeAsync(statement), iterate(executor), executor);
    Futures.addCallback(
        resultFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(ResultSet rows) {
            state.endDbAccess();
            try {
              acceptor.accept(rows);
            } catch (Throwable t) {
              errorHandler.accept(t);
            }
          }

          @Override
          public void onFailure(Throwable throwable) {
            state.markError();
            logger.error("Cassandra query error; error={}, statement={}", throwable, statement);
            errorHandler.accept(throwable);
          }
        },
        state.getExecutor());
  }

  /**
   * Create prepared statement.
   *
   * <p>The method retries the operation when recoverable errors happen.
   *
   * @param query Statement to prepare.
   * @param name Name of the prepared statement, used for log message on failure.
   * @param logger Logger to be used for logging.
   * @return The created prepared statement.
   * @throws ApplicationException when the operation fails.
   */
  public PreparedStatement createPreparedStatement(String query, String name, Logger logger)
      throws ApplicationException {
    final RetryHandler retryHandler =
        new RetryHandler("Prepared statement for " + name, logger, "");
    while (true) {
      try {
        return session.prepare(query);
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }
  }

  public PreparedStatement createPreparedStatement(
      RegularStatement query, String name, Logger logger) throws ApplicationException {
    final RetryHandler retryHandler =
        new RetryHandler("Prepared statement for " + name, logger, "");
    while (true) {
      try {
        return session.prepare(query);
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }
  }

  public boolean verifyQualifiedTable(String tableQualfiedName) {
    String[] parts = tableQualfiedName.split("\\.");
    if (parts.length < 2) {
      return false;
    } else {
      return verifyTable(parts[0], parts[1]);
    }
  }

  /**
   * Method to verify if a table is visible.
   *
   * @param keyspaceName Target keyspace name
   * @param tableName Target table name
   * @return true if specified table is visible, false otherwise
   */
  public boolean verifyTableHasColumn(String keyspaceName, String tableName, String columnName) {
    logger.debug("Verifying table {}.{} has column {}.", keyspaceName, tableName, columnName);
    final String statement =
        "SELECT * FROM system_schema.columns WHERE keyspace_name = ? and table_name = ? and "
            + "column_name = ?";
    ResultSet result = session.execute(statement, keyspaceName, tableName, columnName);
    if (result.isExhausted()) {
      logger.debug("table {}.{} is missing", keyspaceName, tableName);
      return false;
    } else {
      Row row = result.one();
      logger.debug(row.toString());
      return true;
    }
  }

  /**
   * Method to verify if a table is visible.
   *
   * @param keyspaceName Target keyspace name
   * @param tableName Target table name
   * @return true if specified table is visible, false otherwise
   */
  public boolean verifyTable(String keyspaceName, String tableName) {
    logger.debug("verifying table {}.{}", keyspaceName, tableName);
    final String statement =
        "SELECT * FROM system_schema.tables WHERE keyspace_name = ? and table_name = ?";
    ResultSet result = session.execute(statement, keyspaceName, tableName);
    if (result.isExhausted()) {
      logger.debug("table {}.{} is missing", keyspaceName, tableName);
      return false;
    } else {
      Row row = result.one();
      logger.debug(row.toString());
      return true;
    }
  }

  /**
   * Method to verify if a table is visible with retry.
   *
   * <p>Retry interval is 1s.
   *
   * @param keyspaceName Target keyspace name
   * @param tableName Target table name
   * @param numTrials Number of verification trials
   * @return true if specified table is visible, false otherwise
   */
  public boolean verifyTable(String keyspaceName, String tableName, int numTrials) {
    for (int i = 0; i < numTrials; ++i) {
      if (verifyTable(keyspaceName, tableName)) {
        return true;
      } else {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // do nothing
          Thread.currentThread().interrupt();
        }
      }
    }
    return false;
  }

  /**
   * Create a table if not exists.
   *
   * @param keyspace Keyspace where the table is created
   * @param table Table name to create
   * @param createTableStatement Statement to create the table. The method caller is responsible to
   *     build the proper statement. The method does not check the statement validity.
   * @param yourLogger Your logger
   * @throws ApplicationException When an unexpected exception happens.
   */
  public void createTable(
      String keyspace, String table, String createTableStatement, Logger yourLogger)
      throws ApplicationException {
    createTable(keyspace, table, new SimpleStatement(createTableStatement), yourLogger);
  }

  /**
   * Create a table if not exists.
   *
   * @param keyspace Keyspace where the table is created
   * @param tableName Table name to create
   * @param createTableStatement Statement to create the table. The method caller is responsible to
   *     build the proper statement. The method does not check the statement validity.
   * @param yourLogger Your logger
   * @throws ApplicationException When an unexpected exception happens.
   */
  public void createTable(
      String keyspace, String tableName, Statement createTableStatement, Logger yourLogger)
      throws ApplicationException {
    if (!verifyTable(keyspace, tableName)) {
      final var message = "Creating table: " + keyspace + "." + tableName;
      yourLogger.info(message);
      execute(message, yourLogger, createTableStatement);
    }
  }

  /**
   * Method to check whether the thrown Cassandra Driver exception is recoverable.
   *
   * @param t The Cassandra driver exception
   * @return Boolean whehter the exception is recoverable by retrying.
   */
  public static boolean isRecoverable(Throwable t) {
    if (t instanceof InvalidQueryException
        || t instanceof OperationTimedOutException
        || t instanceof QueryConsistencyException) {
      return true;
    }
    if (t instanceof ServerError && t.getMessage().contains("Column family ID mismatch")) {
      return true;
    }
    return false;
  }

  public static String createClient(String name) {
    final String user = name + " " + StringUtils.tsToIso8601Millis(System.currentTimeMillis());
    return user;
  }

  public boolean registerUser(String user) {
    if (isShutdown) {
      return false;
    }
    lock.lock();
    usersCount.incrementAndGet();
    users.put(user, Thread.currentThread().getName());
    lock.unlock();
    return true;
  }

  public void deregisterClient(String user) {
    lock.lock();
    try {
      users.remove(user);
      usersCount.decrementAndGet();
      condition.signal();
    } finally {
      lock.unlock();
    }
  }

  public void shutdown() {
    isShutdown = true;
    for (int i = 0; i < 60; ++i) {
      lock.lock();
      try {
        if (usersCount.get() == 0) {
          break;
        }
        logger.debug("Registered users are still remaining: {}", users);
        condition.await(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        // keep waiting
        Thread.currentThread().interrupt();
      } finally {
        lock.unlock();
      }
    }
    if (usersCount.get() != 0) {
      final var sb = new StringBuilder();
      for (final var entry : users.entrySet()) {
        final var stackTrace = Utils.dumpThread(entry.getValue());
        sb.append(entry.getKey() + ": " + stackTrace);
      }
      logger.warn(
          "{} remaining Cassandra users={}; timed out and closing anyway.",
          usersCount.get(),
          sb.toString());
    }
    if (poolStatusReportFuture != null) {
      poolStatusReportFuture.cancel(true);
      final long statusReportCancelTimeout = System.currentTimeMillis() + 10000; // 10 seconds
      while (!poolStatusReportFuture.isCancelled() && !poolStatusReportFuture.isDone()) {
        if (System.currentTimeMillis() > statusReportCancelTimeout) {
          logger.warn("Pool status reporter did not stop, force terminating anyway");
          break;
        }
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    getSession().close();
    getCluster().close();
  }

  private static AsyncFunction<ResultSet, ResultSet> iterate(Executor executor) {
    return new AsyncFunction<>() {
      @Override
      public ListenableFuture<ResultSet> apply(ResultSet rows) {

        boolean wasLastPage = rows.getExecutionInfo().getPagingState() == null;
        if (wasLastPage) {
          return Futures.immediateFuture(rows);
        }
        ListenableFuture<ResultSet> future = rows.fetchMoreResults();
        return Futures.transformAsync(future, iterate(executor), executor);
      }
    };
  }
}
