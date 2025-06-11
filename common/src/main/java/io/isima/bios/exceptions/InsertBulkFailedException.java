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
package io.isima.bios.exceptions;

import io.isima.bios.models.Record;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.Collections;
import java.util.Map;

/**
 * An exception thrown when InsertBulk operation fails.
 *
 * <p>The class includes error code, error message, and insert responses.
 */
public class InsertBulkFailedException extends BiosClientException {
  private static final long serialVersionUID = 2538234680437988663L;

  private final Map<Integer, Record> responses;
  private final Map<Integer, BiosClientException> reasons;

  /**
   * Generic constructor.
   *
   * @param code Error code
   * @param message Failure reason
   * @param responses Map of responses when events have been ingested partially.
   * @param reasons Map of reasons for the failure.
   */
  public InsertBulkFailedException(
      BiosClientError code,
      String message,
      Map<Integer, Record> responses,
      Map<Integer, BiosClientException> reasons) {
    super(code, message);
    this.responses = responses;
    this.reasons = reasons;
  }

  /**
   * Constructor used to wrap-and-rethrow an exception from the async caller completion callback.
   */
  public InsertBulkFailedException(InsertBulkFailedException cause) {
    super(cause.getCode(), cause.getMessage(), cause.getEndpoint(), cause);
    this.responses = cause.responses;
    this.reasons = cause.reasons;
  }

  public InsertBulkFailedException(
      BiosClientError code,
      String message,
      Map<Integer, Record> responses,
      Map<Integer, BiosClientException> reasons,
      Throwable cause) {
    super(code, message, cause);
    this.responses = responses;
    this.reasons = reasons;
  }

  public InsertBulkFailedException(BiosClientError code, String message) {
    super(code, message);
    this.responses = Collections.emptyMap();
    this.reasons = Collections.emptyMap();
  }

  /**
   * Get responses of ingested events.
   *
   * <p>This method returns responses when events have been ingested partially. The returned map
   * includes responses for events that are ingested successfully. The map is empty when no
   * ingestion succeeds.
   *
   * @return Responses of ingested events. The returned map is empty if no events are ingested. The
   *     key of the map denotes the index in the requested event list for the ingested event.
   */
  public Map<Integer, Record> getResponses() {
    return responses;
  }

  /**
   * Get reasons of failed ingestions.
   *
   * <p>This method provides erros for individual events. When the ingestions have succeeded
   * partially, the returned map includes errors for only failed ones.
   *
   * @return Error details of failed ingestions. The key of the map denotes the index in the
   *     requested event list for the invalid event.
   */
  public Map<Integer, BiosClientException> getReasons() {
    return reasons;
  }
}
