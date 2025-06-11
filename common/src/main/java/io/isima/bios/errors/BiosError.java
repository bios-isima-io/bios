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
package io.isima.bios.errors;

import javax.ws.rs.core.Response.Status;

/** This interface provides a template of declaring an error event. */
public interface BiosError {

  /**
   * Get internal unique error code.
   *
   * @return Error code
   */
  String getErrorCode();

  /**
   * Get HTTP status code to use.
   *
   * @return Status code.
   */
  Status getStatus();

  /**
   * Get error message.
   *
   * @return Error message.
   */
  String getErrorMessage();
}
