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
package io.isima.bios.sdk.exceptions;

import io.isima.bios.dto.IngestResultOrError;
import io.isima.bios.sdk.errors.BiosClientError;
import java.util.List;

/**
 * An exception thrown when a batch of IngestBulk operation fails.
 *
 * <p>The class includes error code, error message, and ingest responses.
 */
public class IngestBulkBatchFailedException extends BiosClientException {
  private static final long serialVersionUID = -1L;

  private final List<? extends IngestResultOrError> batchResponses;

  /**
   * Generic constructor.
   *
   * @param code Error code
   * @param message Failure reason
   * @param batchResponses List of responses when events have been ingested partially.
   */
  public IngestBulkBatchFailedException(
      BiosClientError code, String message, List<? extends IngestResultOrError> batchResponses) {
    super(code, message);
    this.batchResponses = batchResponses;
  }

  public List<? extends IngestResultOrError> getBatchResponses() {
    return batchResponses;
  }
}
