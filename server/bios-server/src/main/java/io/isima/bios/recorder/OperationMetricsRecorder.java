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
package io.isima.bios.recorder;

import io.isima.bios.stats.Counter;
import io.isima.bios.stats.Moments;

/**
 * Interface used to cache metrics per request so that it can be eventually committed atomically to
 * the global recorder.
 *
 * <p>Implementations should override only what is necessary.
 */
public interface OperationMetricsRecorder {
  /**
   * Sub operation latency for auth and validation of any request.
   *
   * @return Latency accumulator for auth and validation of request
   */
  default Moments getDecodeMetrics() {
    return null;
  }

  /**
   * Sub operation latency for auth and validation of any request.
   *
   * @return Latency accumulator for auth and validation of request
   */
  default Moments getValidateMetrics() {
    return null;
  }

  /**
   * Pre processing latency of any request for an operation, if any.
   *
   * @return Latency accumulator for pre process
   */
  default Moments getPreProcessMetrics() {
    return null;
  }

  /**
   * DB prepare latency of any request for an operation, if any.
   *
   * @return Latency accumulator for pre process
   */
  default Moments getDbPrepareMetrics() {
    return null;
  }

  /**
   * DB execute latency of any request for an operation, if any.
   *
   * @return Latency accumulator for executing queries
   */
  default Moments getStorageAccessMetrics() {
    return null;
  }

  /**
   * DB execute error latency of any request for an operation, if any.
   *
   * @return Latency accumulator for queries that had an error
   */
  default Moments getDbErrorMetrics() {
    return null;
  }

  /**
   * Post process latency for this operation.
   *
   * @return post process latency
   */
  default Moments getPostProcessMetrics() {
    return null;
  }

  Moments getEncodeMetrics();

  /** Num writes counter */
  default Counter getNumWrites() {
    return null;
  }

  /** Num reads counter */
  default Counter getNumReads() {
    return null;
  }

  default void addError(boolean isDataValidationError) {
    if (isDataValidationError) {
      getNumValidationErrors().increment();
    } else {
      getNumTransientErrors().increment();
    }
  }

  Counter getNumValidationErrors();

  Counter getNumTransientErrors();
}
