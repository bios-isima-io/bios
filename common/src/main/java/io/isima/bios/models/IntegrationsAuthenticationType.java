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
package io.isima.bios.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;

public enum IntegrationsAuthenticationType {
  LOGIN,
  SASL_PLAINTEXT,
  IN_MESSAGE,
  ACCESS_KEY,
  HTTP_AUTHORIZATION_HEADER,
  HTTP_HEADERS_PLAIN,
  API_ACCESS_TOKEN,
  APP_SECRET,
  GOOGLE_OAUTH,
  MONGODB_X509,
  UNKNOWN;

  private static final EnumStringifier<IntegrationsAuthenticationType> stringifier =
      new EnumStringifier<>(values());

  @JsonCreator
  static IntegrationsAuthenticationType forValue(String value) {
    try {
      return stringifier.destringify(value);
    } catch (IllegalArgumentException e) {
      return UNKNOWN;
    }
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }
}
