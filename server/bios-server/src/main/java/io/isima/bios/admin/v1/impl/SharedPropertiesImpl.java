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
package io.isima.bios.admin.v1.impl;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.QueryConsistencyException;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.storage.cassandra.RetryHandler;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedPropertiesImpl extends SharedProperties {
  private static final Logger logger = LoggerFactory.getLogger(SharedPropertiesImpl.class);

  // column names
  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final String APPLIED = "[applied]";
  public static final String OVERRIDE_PREFIX = "OVERRIDE-";

  // statement templates
  private static final String FORMAT_CREATE_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.%s ( %s text PRIMARY KEY, %s text )";
  private static final String FORMAT_GET = "SELECT %s FROM %s.%s WHERE %s = ?";
  private static final String FORMAT_SET = "INSERT INTO %s.%s (%s, %s) VALUES (?, ?)";
  private static final String FORMAT_DEL = "DELETE FROM %s.%s WHERE %s = ?";
  private static final String FORMAT_SAFE_INSERT =
      "INSERT INTO %s.%s (%s, %s) VALUES (?, ?) IF NOT EXISTS";
  private static final String FORMAT_SAFE_UPDATE = "UPDATE %s.%s SET %s = ? WHERE %s = ? IF %s = ?";

  private final CassandraConnection cassandraConnection;
  private final Session session;
  private PreparedStatement statementGet;
  private PreparedStatement statementSet;
  private PreparedStatement statementDel;
  private PreparedStatement statementSafeInsert;
  private PreparedStatement statementSafeUpdate;

  /** The constructor. */
  public SharedPropertiesImpl(CassandraConnection cassandraConnection) {
    logger.debug("SharedPropertiesImpl initializing ###");
    this.cassandraConnection = cassandraConnection;
    session = cassandraConnection.getSession();

    // make prepared statements
    try {
      String keyspace = CassandraConstants.KEYSPACE_ADMIN;
      String table = CassandraConstants.TABLE_PROPERTIES;
      cassandraConnection.execute(
          "Create SharedProperty table",
          logger,
          String.format(FORMAT_CREATE_TABLE, keyspace, table, KEY, VALUE));

      initialize(keyspace, table);
    } catch (ApplicationException e) {
      throw new RuntimeException(e);
    }

    logger.debug("### SharedPropertiesImpl initialized");
  }

  private void initialize(String keyspace, String table) throws ApplicationException {
    RetryHandler retryHandler =
        new RetryHandler(
            "Preparing statement for SharedProperties.get",
            logger,
            String.format(" keyspace=%s table=%s", keyspace, table));
    while (true) {
      try {
        statementGet = session.prepare(String.format(FORMAT_GET, VALUE, keyspace, table, KEY));
        retryHandler.done();
        break;
      } catch (InvalidQueryException | OperationTimedOutException | QueryConsistencyException e) {
        retryHandler.handleError(e);
      }
    }
    retryHandler.reset("Preparing statement for SharedProperties.set");
    while (true) {
      try {
        statementSet = session.prepare(String.format(FORMAT_SET, keyspace, table, KEY, VALUE));
        retryHandler.done();
        break;
      } catch (InvalidQueryException | OperationTimedOutException | QueryConsistencyException e) {
        retryHandler.handleError(e);
      }
    }
    retryHandler.reset("Preparing statement for SharedProperties.del");
    while (true) {
      try {
        statementDel = session.prepare(String.format(FORMAT_DEL, keyspace, table, KEY));
        retryHandler.done();
        break;
      } catch (InvalidQueryException | OperationTimedOutException | QueryConsistencyException e) {
        retryHandler.handleError(e);
      }
    }
    retryHandler.reset("Preparing statement for SharedProperties.safeInsert");
    while (true) {
      try {
        statementSafeInsert =
            session.prepare(String.format(FORMAT_SAFE_INSERT, keyspace, table, KEY, VALUE));
        retryHandler.done();
        break;
      } catch (InvalidQueryException | OperationTimedOutException | QueryConsistencyException e) {
        retryHandler.handleError(e);
      }
    }
    retryHandler.reset("Preparing statement for SharedProperties.safeUpdate");
    while (true) {
      try {
        statementSafeUpdate =
            session.prepare(String.format(FORMAT_SAFE_UPDATE, keyspace, table, VALUE, KEY, VALUE));
        retryHandler.done();
        break;
      } catch (InvalidQueryException | OperationTimedOutException | QueryConsistencyException e) {
        retryHandler.handleError(e);
      }
    }
  }

  /** Return the value of a shared property from the database or system/environment. */
  @Override
  public String getProperty(String key) {
    final Statement statement = statementGet.bind(key);
    try {
      assert !ExecutorManager.isInIoThread();
      ResultSet results = session.execute(statement);
      if (results.isExhausted()) {
        return null;
      }
      return results.one().getString(0);
    } catch (NoHostAvailableException e) {
      logger.error(
          "SharedPropertiesImpl: NO HOST AVAILABLE: statement={}: cl={}: session={}",
          statement,
          statement.getConsistencyLevel(),
          session);
      return null;
      // We should not throw here, it causes flooding of log messages in server.log.
      // We should put all required information in the log itself.
    }
  }

  @Override
  public void getPropertyAsync(
      String key,
      ExecutionState state,
      Consumer<String> acceptor,
      Consumer<Throwable> errorHandler) {
    final Statement statement = statementGet.bind(key);

    cassandraConnection.executeAsync(
        statement,
        state,
        (results) -> {
          if (results.isExhausted()) {
            acceptor.accept(null);
          } else {
            acceptor.accept(results.one().getString(0));
          }
        },
        errorHandler::accept);
  }

  /**
   * Same as {@link #getProperty(String)}, except that it does not hide NoHostAvailableException
   * exceptions - it throws that exception to the caller.
   */
  @Override
  public String getPropertyFailOnError(String key) {
    final Statement statement = statementGet.bind(key);
    assert !ExecutorManager.isInIoThread();
    ResultSet results = session.execute(statement);
    if (results.isExhausted()) {
      return null;
    }
    return results.one().getString(0);
  }

  @Override
  public void setProperty(String key, String value) throws ApplicationException {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }
    cassandraConnection.execute(
        "SharedProperty.setProperty", logger, statementSet.bind(key, value));
  }

  @Override
  public String safeAddProperty(String key, String value) throws ApplicationException {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }
    ResultSet results =
        cassandraConnection.execute(
            "SharedProperty.safeAddProperty", logger, statementSafeInsert.bind(key, value));
    if (!results.isExhausted()) {
      Row row = results.one();
      boolean applied = row.getBool(APPLIED);
      if (!applied) {
        return row.getString(VALUE);
      }
    }
    return null;
  }

  @Override
  public String safeUpdateProperty(String key, String oldValue, String newValue)
      throws ApplicationException {
    if (key == null) {
      throw new IllegalArgumentException("property key may not be null");
    }
    ResultSet results =
        cassandraConnection.execute(
            "SharedProperty.safeUpdateProperty",
            logger,
            statementSafeUpdate.bind(newValue, key, oldValue));
    if (!results.isExhausted()) {
      Row row = results.one();
      boolean applied = row.getBool(APPLIED);
      if (!applied) {
        return row.getString(VALUE);
      }
    }
    return null;
  }

  // used only for test
  @Override
  public void deleteProperty(String key) {
    assert !ExecutorManager.isInIoThread();
    session.execute(statementDel.bind(key));
  }
}
