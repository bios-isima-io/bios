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
package io.isima.bios.data.impl.feature;

import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Pre-compiled image that is used for modifying a context retrieval result to adjust to up-to-date
 * state.
 */
public interface ContextAdjuster {
  /** Runs adjustment. */
  CompletableFuture<List<Event>> adjust(
      List<List<Object>> primaryKeys,
      List<String> targetAttributes,
      List<Event> initialEntries,
      ExecutionState state);

  /**
   * Checks if the adjuster is valid.
   *
   * @return null if the adjuster is valid. An error message is returned otherwise.
   */
  default String isValid() {
    return null;
  }

  /**
   * Generic implementation that indicates that a context is not feasible to do adjustment query,
   * likely caused by misconfiguration.
   */
  public static ContextAdjuster invalidAdjuster(String message) {
    return new InvalidContextAdjuster(message);
  }

  public class InvalidContextAdjuster implements ContextAdjuster {
    final String message;

    private InvalidContextAdjuster(String message) {
      this.message = message;
    }

    @Override
    public CompletableFuture<List<Event>> adjust(
        List<List<Object>> primaryKeys,
        List<String> targetAttributes,
        List<Event> initialEntries,
        ExecutionState state) {
      throw new UnsupportedOperationException(message);
    }

    @Override
    public String isValid() {
      return message;
    }
  }
}
