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
package io.isima.bios.data.impl;

import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.SyntaxError;
import io.isima.bios.errors.EventExtractError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.Event;
import io.isima.bios.models.Record;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class DataUtils {

  /**
   * Method to handle an extraction error from {@link SignalExtractor} and {@link SignalSummarizer}.
   *
   * <p>The main purpose of this method is to understand unchecked exceptions from Cassandra and
   * convert them to {@link TfosException} or {@link ApplicationException}.
   *
   * @param error Received error
   * @param statement Query statement
   * @return Exception to report
   */
  public static Throwable handleExtractError(Throwable error, Statement statement) {
    final Throwable toReturn;
    // TODO(TFOS-906): This is a temporary fix to handle invalid query exception
    // due to an incorrect filter specification from the user.
    if (error instanceof InvalidQueryException) {
      String message =
          error
              .getMessage()
              .replaceAll("com.datastax.driver.core.exceptions.InvalidQueryException: ", "")
              .replaceAll("\"evt_", "\"");
      toReturn = new TfosException(EventExtractError.INVALID_QUERY, message);
    } else if (error instanceof SyntaxError) {
      SignalExtractor.logger.debug("Syntax error. Statement={}", statement);
      toReturn = new ApplicationException("Query syntax error: " + error.getMessage());
    } else {
      toReturn = error;
    }
    return toReturn;
  }

  /**
   * Make a composite key from a list of attribute names and an attribute map.
   *
   * @param attributeNames Attribute names
   * @param attributes Attribute map
   */
  public static List<Object> makeCompositeKey(
      List<String> attributeNames, Map<String, Object> attributes) {
    final var values = new Object[attributeNames.size()];
    for (int i = 0; i < values.length; ++i) {
      values[i] = attributes.get(attributeNames.get(i));
    }
    return Arrays.asList(values);
  }

  /**
   * Make a composite key from a list of attribute names and an event.
   *
   * @param attributeNames Attribute names
   * @param event Event
   */
  public static List<Object> makeCompositeKey(List<String> attributeNames, Event event) {
    final var values = new Object[attributeNames.size()];
    for (int i = 0; i < values.length; ++i) {
      values[i] = event.get(attributeNames.get(i));
    }
    return Arrays.asList(values);
  }

  public static List<Object> makeCompositeKey(List<String> attributeNames, Record record) {
    final var values = new Object[attributeNames.size()];
    for (int i = 0; i < values.length; ++i) {
      values[i] = record.getAttribute(attributeNames.get(i)).asObject();
    }
    return Arrays.asList(values);
  }

  /** Waits on a completable future synchronously and returns the result. */
  public static <T> T wait(CompletableFuture<T> future, String operation)
      throws TfosException, ApplicationException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TfosException(GenericError.OPERATION_CANCELED, operation + " interrupted");
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof TfosException) {
        final var orig = (TfosException) cause;
        throw orig;
      } else if (cause instanceof ApplicationException) {
        final var orig = (ApplicationException) cause;
        throw orig;
      } else {
        throw new RuntimeException(operation + " failed", cause);
      }
    }
  }
}
