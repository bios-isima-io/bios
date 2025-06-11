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

import io.isima.bios.errors.BiosError;

public class TenantException extends TfosException {
  /** Generated serial version. */
  private static final long serialVersionUID = 1088673299246178070L;

  public TenantException(String message) {
    super(message);
  }

  public TenantException(BiosError info) {
    super(info.getErrorMessage());
  }

  public TenantException(BiosError info, String additionalMessage) {
    super(info, additionalMessage);
  }
}
