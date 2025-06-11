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
package io.isima.bios.utils;

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

public class TrustStoreConverter {

  /**
   * Converts a PEM certificate data to a truststore file.
   *
   * @param pemData the PEM certificate data as a byte array
   * @param trustStoreFilePath the path to the truststore file
   * @param trustStorePassword the password for the truststore
   * @param alias the alias for the certificate entry in the truststore
   * @throws GeneralSecurityException if a security-related exception occurs during the conversion
   * @throws IOException if an I/O exception occurs during the conversion
   */
  public static void convertPemToTrustStore(
      byte[] pemData, String trustStoreFilePath, String trustStorePassword, String alias)
      throws GeneralSecurityException, IOException {

    String truststoreType = "JKS";

    // Load PEM certificate
    CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
    Certificate certificate;
    try (ByteArrayInputStream pemInputStream = new ByteArrayInputStream(pemData)) {
      certificate = certificateFactory.generateCertificate(pemInputStream);
    }

    // Create a new trustStore
    KeyStore trustStore = KeyStore.getInstance(truststoreType);
    trustStore.load(null, trustStorePassword.toCharArray());

    // Add the certificate to the trustStore with the provided alias
    trustStore.setCertificateEntry(alias, certificate);

    // Save the trustStore to a file
    try (FileOutputStream trustStoreOutputStream = new FileOutputStream(trustStoreFilePath)) {
      trustStore.store(trustStoreOutputStream, trustStorePassword.toCharArray());
    }
  }
}
