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
public class IntegrationsSslConfig {

  @NotNull private SslMode mode;

  /**
   * Content of server root CA certificate PEM. In default, the content should be encoded by base64.
   */
  private String rootCaContent;

  /**
   * Content of client certificate in PKCS12 format. In default, the content should be encoded by
   * base64.
   */
  private String clientCertificateContent;

  private String clientCertificatePassword;

  // TODO(Naoki): Support encrypted encoding
  // private ContentEncoding contentEncoding;
}
