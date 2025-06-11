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

import java.io.ByteArrayInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.openssl.jcajce.JcaMiscPEMGenerator;
import org.bouncycastle.util.io.pem.PemWriter;

public class P12ToPemConverter {

  public static void convert(byte[] pkcs12Data, String password, String certFileName,
      String keyFileName) throws IOException, GeneralSecurityException {
    // Load PKCS12 data
    KeyStore keystore = KeyStore.getInstance("PKCS12");
    try (ByteArrayInputStream inputStream = new ByteArrayInputStream(pkcs12Data)) {
      keystore.load(inputStream, password.toCharArray());
    }

    final var aliases = keystore.aliases();
    while (aliases.hasMoreElements()) {
      final var alias = aliases.nextElement();

      final PrivateKey privateKey = (PrivateKey) keystore.getKey(alias, password.toCharArray());
      final Certificate[] chain = keystore.getCertificateChain(alias);

      exportPrivateKeyToPEM(privateKey, keyFileName);
      exportCertificateChainToPEM(chain, certFileName);

      // assume the source file has only one set of key - certs
      return;
    }

    throw new IOException("Given certificate is empty");
  }

  private static void exportPrivateKeyToPEM(PrivateKey privateKey, String keyFileName)
      throws IOException {
    PrivateKeyInfo privateKeyInfo = PrivateKeyInfo.getInstance(privateKey.getEncoded());
    try (final var outputStream = new FileOutputStream(keyFileName);
         final var pemWriter = new PemWriter(new OutputStreamWriter(outputStream))) {
      final var object = new JcaMiscPEMGenerator(privateKeyInfo);
      pemWriter.writeObject(object);
      pemWriter.flush();
    }
  }

  private static void exportCertificateChainToPEM(Certificate[] chain, String certFileName)
      throws IOException {
    try (final var outputStream = new FileOutputStream(certFileName);
         final var pemWriter = new PemWriter(new OutputStreamWriter(outputStream))) {
      for (Certificate certificate : chain) {
        final var object = new JcaMiscPEMGenerator(certificate);
        pemWriter.writeObject(object);
      }
      pemWriter.flush();
    }
  }
}
