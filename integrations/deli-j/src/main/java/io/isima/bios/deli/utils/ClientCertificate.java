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
package io.isima.bios.deli.utils;

import io.isima.bios.deli.models.InvalidConfigurationException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;
import lombok.Getter;
import org.slf4j.Logger;

@Getter
public class ClientCertificate {
  private final String clientCertificateFileName;
  private final byte[] clientCertificateData;
  private final String clientCertificatePassword;

  public ClientCertificate(String clientCertificateFileName, byte[] clientCertificateData,
      String clientCertificatePassword) {
    this.clientCertificateFileName = Objects.requireNonNull(clientCertificateFileName);
    this.clientCertificateData = Objects.requireNonNull(clientCertificateData);
    this.clientCertificatePassword = Objects.requireNonNull(clientCertificatePassword);
  }

  /**
   * Save the client certificate data to the configured file.
   */
  public boolean trySaving(String fileName, Logger logger) throws IOException {
    if (Utils.trySaveToFile(clientCertificateData, fileName)) {
      logger.info("Saved client certificate file {}", fileName);
      return true;
    }
    logger.info("Kept existing client certificate file {} for no change", fileName);
    return false;
  }

  public void saveClientCertificate(Logger logger) throws IOException {
    trySaving(clientCertificateFileName, logger);
  }

  public void convertToPemFiles(String certFileName, String keyFileName, Logger logger)
      throws InvalidConfigurationException {
    try {
      P12ToPemConverter.convert(clientCertificateData, clientCertificatePassword, certFileName,
          keyFileName);
      logger.info("Converted client certificate p12 to {} (cert) and {} (key)", certFileName,
          keyFileName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } catch (GeneralSecurityException e) {
      throw new InvalidConfigurationException(
          "Failed to convert client certifiicate P12 to cert and key pem files: %s",
          e.getMessage());
    }
  }
}
