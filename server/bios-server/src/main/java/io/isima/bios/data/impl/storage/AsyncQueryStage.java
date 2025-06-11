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
package io.isima.bios.data.impl.storage;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;

/**
 * Interface to provide functionality of an asynchronous query stage execution.
 *
 * <p>The class is meant to be used by class {@link AsyncQueryExecutor} that executes asynchrnous
 * queries concurrently.
 */
public interface AsyncQueryStage {
  /**
   * Returns a Cassandra query statement for the stage.
   *
   * <p>This method always must return a statement. Returning null is not allowed.
   *
   * @return the statement
   */
  Statement getStatement();

  /** Callback invoked right before starting the database query. */
  default void beforeQuery() {}

  /**
   * The method invoked when the query for the stage was successfully done.
   *
   * @param result Query result passed by Cassandra server.
   */
  void handleResult(ResultSet result);

  /**
   * The method invoked when the query for the stage has an error.
   *
   * @param t The exception.
   */
  void handleError(Throwable t);

  /**
   * Method invoked when all the queries for the stage have completed successfully.
   *
   * <p>When all queries are done successfully, the {@link AsyncQueryExecutor} calls this method to
   * complete the queries.
   *
   * <p>When error happens, {@link AsyncQueryExecutor} cancels the queries. This method is not
   * called in the case.
   */
  default void handleCompletion() {}
}
