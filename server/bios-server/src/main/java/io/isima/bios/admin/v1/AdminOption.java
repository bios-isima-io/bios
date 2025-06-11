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
package io.isima.bios.admin.v1;

/** Options in executing stream modification. */
public enum AdminOption {
  IS_MODIFICATION,
  /**
   * Skip audit signal and context consistency check during the operation.
   *
   * <p>In order to skip audit validation fully for a context, setting DRY_RUN is also necessary.
   */
  SKIP_AUDIT_VALIDATION,
  /** Executes stream modification validation, but does not actually change state or store data. */
  DRY_RUN,
  /** Fetch details. */
  DETAIL,
}
