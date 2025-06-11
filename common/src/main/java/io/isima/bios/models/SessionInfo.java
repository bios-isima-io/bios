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
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Object to carray session information. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@Getter
@Setter(AccessLevel.PACKAGE)
@NoArgsConstructor
@ToString
public class SessionInfo {
  @JsonProperty("token")
  private String token;

  @JsonProperty("email")
  private String email;

  @JsonProperty("userName")
  private String userName;

  @JsonProperty("tenant")
  private String tenantName;

  @JsonProperty("appName")
  private String appName;

  @JsonProperty("appType")
  private AppType appType;

  @JsonProperty("expiry")
  private Long expiry;

  @JsonProperty("devInstance")
  private DevInstanceConfig devInstance;

  @JsonProperty("upstreams")
  private UpstreamConfig upstreams;

  @JsonProperty("homePageConfig")
  private String homePageConfig;

  @JsonProperty("roles")
  private List<Role> roles;

  @JsonProperty("sessionAttributes")
  private List<SessionAttribute> sessionAttributes;
}
