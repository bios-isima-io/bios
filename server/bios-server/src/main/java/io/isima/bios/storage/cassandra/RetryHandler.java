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

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.exceptions.ApplicationException;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;

/** Class to handle retrying Cassandra recoverable error. */
public class RetryHandler {
  @Setter @Getter private int retry;
  @Setter @Getter private long sleep;
  private boolean retrying;

  /** The name of operation for this handler. */
  private String operation;

  private final String logContext;
  private final Logger logger;

  /**
   * The constructor.
   *
   * @param operation Operation name
   * @param logger Logger to be used for report retry and error
   * @param logContext Log context to be put to log messages
   */
  public RetryHandler(String operation, Logger logger, String logContext) {
    if (operation == null) {
      throw new IllegalArgumentException("operation may not be null");
    }
    if (logger == null) {
      throw new IllegalArgumentException("logger may not be null");
    }
    if (logContext == null) {
      throw new IllegalArgumentException("logContext may not be null");
    }
    this.logger = logger;
    this.logContext = logContext;
    reset(operation);
  }

  /**
   * Refill retry credit for reusing the instance.
   *
   * @param operation New operation name
   */
  public void reset(String operation) {
    this.operation = operation;
    retry = TfosConfig.CASSANDRA_OP_RETRY_COUNT;
    sleep = TfosConfig.CASSANDRA_OP_RETRY_SLEEP_MILLIS;
    retrying = false;
  }

  /**
   * Utility method that handles a recoverable Cassandra error.
   *
   * <p>The method assumes the input exception being recoverable. The method checks number of
   * remaining number of retries, decrement the credit and sleep if it remains. Otherwise, the
   * method throws ApplicationException.
   *
   * @param e The exception
   * @throws ApplicationException When the error is not recoverable or no retry credit is left
   */
  public void handleError(DriverException e) throws ApplicationException {
    if (CassandraConnection.isRecoverable(e) && retry-- > 0) {
      retrying = true;
      if (e instanceof OperationTimedOutException) {
        // Retry for timeout shouldn't be repeated.
        sleep = TfosConfig.CASSANDRA_TIMEOUT_BACKOFF_MILLIS;
      }
      logger.warn(
          "{} failed, retrying after {} milliseconds;{} error={}",
          operation,
          sleep,
          logContext,
          e.toString());
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e1) {
        // ignore
        Thread.currentThread().interrupt();
      }
      sleep *= 2;
    } else {
      logger.error("{} failed;{}, error={}", operation, logContext, e);
      throw new ApplicationException(String.format("%s failed;%s", operation, logContext), e);
    }
  }

  /**
   * Notify recovery by retry.
   *
   * <p>The method does nothing when retry has not happen.
   */
  public void done() {
    if (retrying) {
      logger.info("{} is done successfully;{}", operation, logContext);
    }
  }
}
