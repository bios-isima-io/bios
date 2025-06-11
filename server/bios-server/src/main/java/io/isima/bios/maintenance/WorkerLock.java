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
package io.isima.bios.maintenance;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.CASWriteUnknownException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.storage.cassandra.RetryHandler;
import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerLock {

  private static final Logger logger = LoggerFactory.getLogger(WorkerLock.class);

  // table parameters
  private static final String LOCK_TABLE =
      CassandraConstants.KEYSPACE_ADMIN + "." + CassandraConstants.TABLE_ROLLUP_LOCKS;
  private static final int GC_GRACE_SECONDS = 86400;
  private static final String COMPACTION_CONFIG =
      "{"
          + "'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy',"
          + "'compaction_window_size': '6',"
          + "'compaction_window_unit': 'HOURS',"
          + "'tombstone_compaction_interval': '43200',"
          + "'tombstone_threshold': '0.8',"
          + "'unchecked_tombstone_compaction': 'true'"
          + "}";

  // TODO(TFOS-1085): Should we make this configurable?
  /** Lock TTL seconds. A lock expires after the specified seconds unless it is refreshed. */
  public static final int LOCK_TTL = 600;

  /**
   * Lock refresh seconds. A lock is refreshed after the specified seconds as long as the process is
   * running.
   */
  public static final int LOCK_REFRESH_RATE = 500;

  // TODO(TFOS-1085): Should GC grace secnods be configurable?
  private static final String QUERY_CREATE_ROLLUP_LOCK_TABLE =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s (lock_target text PRIMARY KEY)"
              + " WITH comment = 'TFOS Lock Table version 1'"
              + " AND gc_grace_seconds = %d"
              + " AND compaction = %s",
          LOCK_TABLE, GC_GRACE_SECONDS, COMPACTION_CONFIG);

  private final Session session;
  private final ScheduledExecutorService scheduler;

  // unit is seconds for these properties
  private final int ttl;
  private final int refreshInterval;

  boolean autoRefreshEnabled;

  private PreparedStatement statementAcquireLock;
  private PreparedStatement statementRefreshLock;
  private PreparedStatement statementReleaseLock;

  private String lockLoggingTarget;

  /**
   * The default constructor of this method.
   *
   * <p>This constructor constructs an object with default configuration parameters {@link
   * #LOCK_TTL} and {@link #LOCK_REFRESH_RATE}.
   */
  public WorkerLock() {
    this(LOCK_TTL, LOCK_REFRESH_RATE);
  }

  /**
   * Constructor with configuration parameters.
   *
   * <p>This method is meant be used only for testing. Production code should not use this method.
   *
   * @param ttl Lock TTL
   * @param refreshRate Lock refresh rate
   */
  public WorkerLock(int ttl, int refreshRate) {
    this.ttl = ttl;
    this.refreshInterval = refreshRate;
    autoRefreshEnabled = true;
    logger.debug("RollupOwnershipHandler initializing ###");
    try {
      CassandraConnection cassandraConnection = BiosModules.getCassandraConnection();
      session = cassandraConnection.getSession();
      scheduler = Executors.newScheduledThreadPool(1);

      logger.debug("creating rollup lock table: {}", QUERY_CREATE_ROLLUP_LOCK_TABLE);
      assert !ExecutorManager.isInIoThread();
      session.execute(QUERY_CREATE_ROLLUP_LOCK_TABLE);

      if (!cassandraConnection.verifyTable(
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ROLLUP_LOCKS, 30)) {
        throw new ApplicationException(
            String.format(
                "Failed to creating table %s.%s",
                CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ROLLUP_LOCKS));
      }

      final RetryHandler retryHandler =
          new RetryHandler("Creating prepared statements to acquire rollup lock", logger, "");
      while (true) {
        try {
          statementAcquireLock =
              session
                  .prepare(
                      String.format(
                          "INSERT INTO %s (lock_target) VALUES (?) IF NOT EXISTS USING TTL %d",
                          LOCK_TABLE, ttl))
                  .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                  .setConsistencyLevel(ConsistencyLevel.QUORUM);

          statementRefreshLock =
              session
                  .prepare(
                      String.format(
                          "INSERT INTO %s (lock_target) VALUES (?) USING TTL %d", LOCK_TABLE, ttl))
                  .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                  .setConsistencyLevel(ConsistencyLevel.QUORUM);

          statementReleaseLock =
              session
                  .prepare(
                      String.format("DELETE FROM %s WHERE lock_target = ? IF EXISTS", LOCK_TABLE))
                  .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
                  .setConsistencyLevel(ConsistencyLevel.QUORUM);
          break;
        } catch (DriverException e) {
          retryHandler.handleError(e);
        }
      }
    } catch (Throwable t) {
      logger.error("Caught an exception during initialization of RollupOwnershipHandler", t);
      throw new RuntimeException(t);
    }
    lockLoggingTarget = TfosConfig.getLockLoggingTarget();
    logger.debug("### RollupOwnershipHandler initialized");
  }

  /**
   * Acquire a lock of specified target.
   *
   * @param target Lock target. A lock target is identified by this name.
   * @return Lock object if the lock is successfully acquired. If any other locker has obtained the
   *     lock already, the method returns null.
   * @throws ApplicationException When unexpected error happens.
   */
  public LockEntity lock(String target) throws ApplicationException {
    ResultSet result;
    try {
      result = executeLwt(statementAcquireLock.bind(target), target, "acquiring");
    } catch (UnavailableException e) {
      logger.warn("Acquiring lock failed, trying to roll back; target={}, error={}", target, e);
      try {
        executeLwt(statementReleaseLock.bind(target), target, "reverting");
      } catch (DriverException ei) {
        logger.warn("Acquiring lock failed; target={}, error={}", target, e);
      }
      return null;
    } catch (DriverException e) {
      throw new ApplicationException("Acquiring lock failed; target=" + target, e);
    }
    if (result.isExhausted()) {
      throw new ApplicationException(
          "Lock acquirement query did not return a result; target=" + target);
    }
    final boolean wasApplied = result.wasApplied();
    if (loggingEnabled(target)) {
      logger.info(
          "Acquiring lock; target={}, result={}", target, wasApplied ? "success" : "missed");
    }
    if (wasApplied) {
      final LockEntity lock = new LockEntity(this, target);
      if (autoRefreshEnabled) {
        lock.future = scheduler.schedule(() -> lock.refresh(), refreshInterval, TimeUnit.SECONDS);
      }
      return lock;
    } else {
      return null;
    }
  }

  /**
   * Acquire a lock of specified target asynchronously.
   *
   * @param target Lock target. A lock target is identified by this name.
   * @return Future for lock object if the lock is successfully acquired. If any other locker has
   *     obtained the lock already, the value returned via the future is null.
   * @throws ApplicationException via CompletionException When unexpected error happens.
   */
  public CompletionStage<LockEntity> lockAsync(String target, Executor executor) {
    if (executor == null) {
      executor = ExecutorManager.getSidelineExecutor();
    }

    return CompletableFuture.supplyAsync(
        () -> ExecutionHelper.supply(() -> lock(target)), executor);
  }

  /**
   * Returns lock expiry time.
   *
   * @return Expiry time in seconds after a lock is acquired
   */
  public int getLockExpiryTime() {
    return ttl;
  }

  /**
   * Turn on/off auto refresh.
   *
   * <p>This method is only for testing
   *
   * @param autoRefreshEnabled Flag to indicate if auto refresh is enabled
   */
  public void setAutoRefresh(boolean autoRefreshEnabled) {
    this.autoRefreshEnabled = autoRefreshEnabled;
  }

  /**
   * Method to conduct refresh.
   *
   * @param lock Lock object to refresh
   */
  private void refresh(LockEntity lock) {
    if (loggingEnabled(lock.getTarget())) {
      logger.info("Refreshing lock; target={}", lock.getTarget());
    }
    assert !ExecutorManager.isInIoThread();
    session.execute(statementRefreshLock.bind(lock.getTarget()));
    if (autoRefreshEnabled) {
      lock.future = scheduler.schedule(() -> lock.refresh(), refreshInterval, TimeUnit.SECONDS);
    }
  }

  /**
   * Generic method to execute a lightweight transaction.
   *
   * <p>A LWT may fail in paxos phase, in which case the execute method throws WriteTimeoutException
   * with WriteType CAS. This method retries the operation in such a case up to {@link
   * TfosConfig#CASSANDRA_OP_RETRY_COUNT} times. See <a
   * href="https://www.datastax.com/blog/2014/10/cassandra-error-handling-done-right>Cassandra error
   * handling</a>
   *
   * @param statement Cassandra query statement
   * @param target Lock entity logging; Used for logging
   * @param operation Operation name; Used for logging
   * @return Query results
   * @throws DriverException when any Cassandra operation error happens.
   */
  private ResultSet executeLwt(Statement statement, String target, String operation)
      throws DriverException {
    long sleep = TfosConfig.CASSANDRA_OP_RETRY_SLEEP_MILLIS;
    final int maxRetry = TfosConfig.CASSANDRA_OP_RETRY_COUNT;
    int trial = 0;
    while (true) {
      try {
        if (loggingEnabled(target)) {
          logger.info(
              "{} lock; target={}",
              operation.substring(0, 1).toUpperCase() + operation.substring(1),
              target);
        }
        assert !ExecutorManager.isInIoThread();
        final ResultSet result = session.execute(statement);
        return result;
      } catch (WriteTimeoutException | CASWriteUnknownException e) {
        if ((e instanceof CASWriteUnknownException
                || ((WriteTimeoutException) e).getWriteType() == WriteType.CAS)
            && trial++ < maxRetry) {
          logger.debug(
              "Retrying CAS for {} lock; target={}, trial={}, error={}",
              operation,
              target,
              trial,
              e.getMessage());
          try {
            Thread.sleep(sleep);
          } catch (InterruptedException ei) {
            Thread.currentThread().interrupt();
          }
          sleep = (long) (sleep * 1.5);
        } else {
          logger.warn(
              "Timed out during {} lock; target={}, error={}", operation, target, e.getMessage());
          return null;
        }
      }
    }
  }

  public void shutdown() {
    scheduler.shutdownNow();
  }

  /**
   * Lock object for a target.
   *
   * <p>The object must be refreshed before being expired using {@link #refresh()} method.
   */
  public class LockEntity implements Closeable {
    final WorkerLock workerLock;
    final String target;
    boolean isValid;
    ScheduledFuture<?> future;

    protected LockEntity(WorkerLock lock, String target) {
      this.workerLock = lock;
      this.target = target;
      isValid = true;
      future = null;
    }

    /**
     * Method to refresh the lock.
     *
     * <p>The method returns silently when lock refresh has been successfully done, otherwise throws
     * {@link ApplicationException}. User of the lock must assume that the lock is not effective
     * anymore when an exception is thrown. Abort the ongoing critical section if any, and handle
     * the error.
     */
    public synchronized void refresh() {
      if (!isValid) {
        return;
      }
      try {
        workerLock.refresh(this);
      } catch (DriverException e) {
        logger.error("Failed to refresho lock; target={}", target, e);
        isValid = false;
      }
    }

    @Override
    public synchronized void close() {
      isValid = false;
      if (future != null) {
        future.cancel(true);
      }
      try {
        workerLock.executeLwt(workerLock.statementReleaseLock.bind(target), target, "releasing");
      } catch (UnavailableException e) {
        logger.warn("Releasing lock failed, retrying; target={}, error={}", target, e);
        try {
          executeLwt(statementReleaseLock.bind(target), target, "retrying");
        } catch (DriverException ei) {
          logger.warn("Releasing lock failed; target={}, error={}", target, e);
        }
      } catch (DriverException e) {
        // log but continue
        logger.error("Releasing lock failed; target={}, error={}", target, e.toString());
      }
    }

    public void release() {
      close();
    }

    public CompletableFuture<Void> releaseAsync(Executor executor) {
      return CompletableFuture.runAsync(() -> release(), executor);
    }

    public String getTarget() {
      return target;
    }

    public boolean isValid() {
      return this.isValid;
    }
  }

  /**
   * Method to check whether logging is enabled for a lock target.
   *
   * <p>The lock is enabled when the target's prefix matches property
   * com.tieredfractals.tfos.maintenance.lock.logging.target.
   *
   * @param target Lock target to be checked
   * @return True if the target should be logged, false otherwise.
   */
  protected boolean loggingEnabled(String target) {
    return lockLoggingTarget != null && target.startsWith(lockLoggingTarget);
  }
}
