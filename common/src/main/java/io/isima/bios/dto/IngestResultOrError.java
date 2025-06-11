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

import io.isima.bios.dto.bulk.InsertSuccessOrError;
import java.util.UUID;

public interface IngestResultOrError extends InsertSuccessOrError {
  UUID getEventId();

  Long getTimestamp();

  Integer getStatusCode();

  String getErrorMessage();

  String getServerErrorCode();

  IngestResultOrError setEventId(UUID eventId);

  void setTimestamp(Long timestamp);

  void setStatusCode(Integer statusCode);

  void setErrorMessage(String errorMessage);

  void setServerErrorCode(String serverErrorCode);
}
