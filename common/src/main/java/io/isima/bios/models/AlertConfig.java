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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Alert schema. */
@JsonInclude(Include.NON_NULL)
@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class AlertConfig extends Duplicatable {
  /** Alert name. */
  @JsonProperty("alertName")
  private String name;

  @JsonProperty("alertType")
  private AlertType type;

  /** Condition to trigger alerts. */
  @JsonProperty("condition")
  private String condition;

  /** URL of the webhook to call when an alert is triggered. */
  @JsonProperty("webhookUrl")
  private String webhookUrl;

  @JsonProperty("userName")
  private String userName;

  @JsonProperty("password")
  private String password;

  // TODO: We'll introduce destination config in future. There, we'll be able to specify destination
  // type as well as authentication methodology. For now, we only support webhook. If the user and
  // the password are specified, we assume the InMessage authentication methodology.

  @Override
  public AlertConfig duplicate() {
    return new AlertConfig().duplicate(this);
  }

  protected AlertConfig duplicate(AlertConfig src) {
    name = src.name;
    type = src.type;
    condition = src.condition;
    webhookUrl = src.webhookUrl;
    userName = src.userName;
    password = src.password;
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof AlertConfig)) {
      return false;
    }

    final AlertConfig test = (AlertConfig) o;
    if (!(name == null ? test.name == null : name.equalsIgnoreCase(test.name))) {
      return false;
    }
    if (!(type == null ? test.type == null : type == test.type)) {
      return false;
    }
    if (!Objects.equals(condition, test.condition)) {
      return false;
    }
    if (!(webhookUrl == null
        ? test.webhookUrl == null
        : webhookUrl.equalsIgnoreCase(test.webhookUrl))) {
      return false;
    }
    if (!(userName == null ? test.userName == null : userName.equalsIgnoreCase(test.userName))) {
      return false;
    }
    return Objects.equals(password, test.password);
  }

  @Override
  public int hashCode() {
    final var nameToHash = name != null ? name.toLowerCase() : null;
    final var typeToHash = type != null ? type : null;
    final var urlToHash = webhookUrl != null ? webhookUrl.toLowerCase() : null;
    final var userToHash = userName != null ? userName.toLowerCase() : null;
    return Objects.hash(nameToHash, typeToHash, condition, urlToHash, userToHash, password);
  }
}
