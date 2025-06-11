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

import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Transfers a ResetPassword operation request. */
@Getter
@Setter
@NoArgsConstructor
public class ChangePasswordRequest {
  /** Copy constructor. */
  public ChangePasswordRequest(ChangePasswordRequest src) {
    email = src.email;
    currentPassword = src.currentPassword;
    newPassword = src.newPassword;
  }

  /** Email address of the user. Required for a cross-user request. */
  String email;

  /** The current password. Required for an individual user. */
  String currentPassword;

  /** The new password. Always required. */
  @NotNull String newPassword;

  @Override
  public String toString() {
    final var sb = new StringBuilder("{");
    String delimiter = "";
    if (email != null) {
      sb.append("email=").append(email);
      delimiter = ", ";
    }
    if (currentPassword != null) {
      sb.append(delimiter).append("currentPassword=***");
      delimiter = ", ";
    }
    if (newPassword != null) {
      sb.append(delimiter).append("newPassword=***");
    }
    return sb.append("}").toString();
  }
}
