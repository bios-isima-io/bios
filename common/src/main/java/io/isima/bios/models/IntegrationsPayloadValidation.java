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
import javax.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class IntegrationsPayloadValidation {
  @NotNull private IntegrationsPayloadValidationType type;

  private String hmacHeader;

  private String sharedSecret;

  private Boolean digestBase64Encoded = Boolean.TRUE;

  @Override
  public String toString() {
    final var sb = new StringBuilder("{type=").append(type);
    if (hmacHeader != null) {
      sb.append(", hmacHeader=").append(hmacHeader);
    }
    if (sharedSecret != null) {
      sb.append(", sharedSecret=***");
    }
    if (digestBase64Encoded != null) {
      sb.append(", digestBase64Encoded=").append(digestBase64Encoded.toString());
    }
    return sb.append("}").toString();
  }
}
