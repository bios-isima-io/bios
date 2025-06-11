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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.Map;
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true) // not ideal but to avoid surprises on SDK side
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class IntegrationsAuthentication {
  @NotNull private IntegrationsAuthenticationType type;

  private String user;

  private String password;

  private String inMessageUserAttribute;

  private String inMessagePasswordAttribute;

  private String userHeader;

  private String passwordHeader;

  private String accessKey;

  private String secretKey;

  private String clientId;

  private String clientSecret;

  // require for facebook, along with clientId and clientSecret in case of APP_SECRET auth
  private String accessToken;

  private String developerToken;

  private String refreshToken;

  private Map<String, String> options;

  @Override
  public String toString() {
    final var sb = new StringBuilder("{type=").append(type);
    if (user != null) {
      sb.append(", user=").append(user);
    }
    if (password != null) {
      sb.append(", password=***");
    }
    if (inMessageUserAttribute != null) {
      sb.append(", inMessageUserAttribute=").append(inMessageUserAttribute);
    }
    if (inMessagePasswordAttribute != null) {
      sb.append(", inMessagePasswordAttribute=").append(inMessagePasswordAttribute);
    }
    if (userHeader != null) {
      sb.append(", userHeader=").append(userHeader);
    }
    if (passwordHeader != null) {
      sb.append(", passwordHeader=").append(passwordHeader);
    }
    if (accessKey != null) {
      sb.append(", accessKey=").append(accessKey);
    }
    if (secretKey != null) {
      sb.append(", secretKey=").append(secretKey);
    }
    if (clientId != null) {
      sb.append(", clientId=").append(clientId);
    }
    if (clientSecret != null) {
      sb.append(", clientSecret=").append(clientSecret);
    }
    if (accessToken != null) {
      sb.append(", accessToken=").append(accessToken);
    }
    if (developerToken != null) {
      sb.append(", developerToken=").append(developerToken);
    }
    if (refreshToken != null) {
      sb.append(", refreshToken=").append(refreshToken);
    }
    return sb.append("}").toString();
  }
}
