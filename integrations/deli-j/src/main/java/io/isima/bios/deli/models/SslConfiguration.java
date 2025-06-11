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
package io.isima.bios.deli.models;

import io.isima.bios.deli.utils.ClientCertificate;
import io.isima.bios.deli.utils.RootCA;
import io.isima.bios.models.SslMode;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * TLS configuration object.
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SslConfiguration {
  // ImportSourceConfig property names
  public static final String MODE = "ssl.mode";
  public static final String ROOT_CA_CONTENT = "ssl.rootCaContent";
  public static final String TRUSTSTORE_PASSWORD = "ssl.truststorePassword";
  public static final String CLIENT_CERTIFICATE_CONTENT = "ssl.clientCertificateContent";
  public static final String CLIENT_CERTIFICATE_PASSWORD = "ssl.clientCertificatePassword";

  private SslMode mode;
  private RootCA rootCa;
  private ClientCertificate clientCertificate;
}
