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
package io.isima.bios.dto;

import java.util.List;

/**
 * Data transfer object class to carry a failure report.
 *
 * <p>Class memebers are immutable.
 */
public class FailureReport {
  /**
   * Timestamp when the error occurred. The value should be the one returned by initial phase of the
   * admin/context operation.
   */
  private Long timestamp;

  /** Conducted operation. */
  private String operation;

  /** Parameters as JSON. */
  private String payload;

  /** List of endpoints where the failures happened. */
  private List<String> endpoints;

  /** Failure reasons. */
  private List<String> reasons;

  /** Reporter ID. */
  private String reporter;

  /** Default constructor used by object mapper. */
  protected FailureReport() {}

  /**
   * The constructor.
   *
   * @param timestamp Timestamp when the error occurred. The value should be the one returned by
   *     initial phase of the admin/context operation.
   * @param operation Conducted operation.
   * @param payload Operation parameters as JSON.
   * @param endpoints List of endpoints where the failures happened.
   * @param reasons Failure reasons.
   * @param reporter Reporter ID.
   */
  public FailureReport(
      Long timestamp,
      String operation,
      String payload,
      List<String> endpoints,
      List<String> reasons,
      String reporter) {
    this.timestamp = timestamp;
    this.operation = operation;
    this.payload = payload;
    this.endpoints = endpoints;
    this.reasons = reasons;
    this.reporter = reporter;
  }

  /**
   * Method to get timestamp when the error happens.
   *
   * @return Timestamp when the error occurs. In case of reporting an error in final phase of an
   *     admin or context update, the value should be the timestamp that was returned for the
   *     initial operation.
   */
  public Long getTimestamp() {
    return timestamp;
  }

  /**
   * Method to get the operation name where the error happened.
   *
   * @return Operation
   */
  public String getOperation() {
    return operation;
  }

  /**
   * Method to get the payload that conveys parameters for the operation.
   *
   * @return Operation parameters as a JSON string.
   */
  public String getPayload() {
    return payload;
  }

  /**
   * Method to get endpoints of failed nodes.
   *
   * @return List of endpoints.
   */
  public List<String> getEndpoints() {
    return endpoints;
  }

  /**
   * Method to get failure reasons.
   *
   * @return List of failure reasons.
   */
  public List<String> getReasons() {
    return reasons;
  }

  /**
   * Method to get reporter ID.
   *
   * @return Reporter ID.
   */
  public String getReporter() {
    return reporter;
  }
}
