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
package io.isima.bios.errors.exception;

import io.isima.bios.errors.AdminError;

/**
 * Exception thrown when Admin component receives an update request but the given config is
 * identical to existing one.
 */
public class AdminChangeRequestToSameException extends TfosException {
  private static final long serialVersionUID = 5224823459220354782L;

  public AdminChangeRequestToSameException() {
    super(AdminError.ADMIN_CHANGE_REQUEST_TO_SAME);
  }

  public AdminChangeRequestToSameException(String message) {
    super(AdminError.ADMIN_CHANGE_REQUEST_TO_SAME, message);
  }
}
