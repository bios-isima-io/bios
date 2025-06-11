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
package io.isima.bios.sdk.impl;

import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.isql.QueryValidator;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.sdk.metrics.MetricsController;

/**
 * Encapsulates per signal control.
 *
 * <p>Also, reconsider doing this check in the language layer is right. C-SDK is the better place if
 * feasible. Mechanism to retry on version mismatch error is in C-SDK already.
 */
public class SignalAttachment {

  private final QueryValidator queryValidator;
  private final MetricsController metricsController;
  private final long signalVersion;

  public SignalAttachment(
      String tenantName, String signalName, long signalVersion, QueryValidator queryValidator) {
    this.queryValidator = queryValidator;
    this.metricsController =
        new MetricsController(
            tenantName,
            signalName,
            MetricsController.STREAM_TYPE_SIGNAL,
            MetricsController.getOperationsForSignal());
    this.signalVersion = signalVersion;
  }

  public void validateQuery(SelectStatement query, int queryIdx) throws BiosClientException {
    if (queryValidator != null) {
      try {
        queryValidator.validate(query, queryIdx, null);
      } catch (ValidatorException e) {
        throw new BiosClientException(BiosClientError.BAD_INPUT, e.getMessage());
      }
    }
  }

  public MetricsController getMetricsController() {
    return metricsController;
  }

  public long getSchemaVersion() {
    return signalVersion;
  }
}
