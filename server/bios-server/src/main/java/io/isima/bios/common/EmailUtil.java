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
package io.isima.bios.common;

public class EmailUtil {
  public static boolean validateEmail(String email) {
    if (email == null || email.isEmpty()) {
      return false;
    } else {
      String[] tokens = email.split("@");
      return tokens.length == 2;
    }
  }

  public static String getNameFromMailId(String mailId) {
    String[] tokens = mailId.split("@");
    if (tokens.length >= 2) {
      String name = tokens[0];
      return name.substring(0, 1).toUpperCase() + name.substring(1);
    }
    return mailId;
  }
}
