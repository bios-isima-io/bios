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
package io.isima.bios.server;

import io.isima.bios.configuration.Bios2Config;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Http3Utils {
  private static final Logger logger = LoggerFactory.getLogger(Http3Utils.class);

  public static QuicSslContext createSslContext()
      throws CertificateException,
          IOException,
          KeyStoreException,
          NoSuchAlgorithmException,
          UnrecoverableKeyException {

    final String keyStorePath = Bios2Config.quicKeyStoreFile();
    final String password = Bios2Config.sslKeyStorePassword();
    final String keystoreType = Bios2Config.sslKeyStoreType();

    final String algorithm = "SunX509";

    final File keyStoreFile =
        keyStorePath.startsWith("/")
            ? new File(keyStorePath)
            : new File(System.getProperty("user.dir"), keyStorePath);

    try (InputStream inputStream = new FileInputStream(keyStoreFile)) {
      final var keystore = KeyStore.getInstance(keystoreType);
      keystore.load(inputStream, password.toCharArray());
      final var kmf = KeyManagerFactory.getInstance(algorithm);
      kmf.init(keystore, password.toCharArray());
      return QuicSslContextBuilder.forServer(kmf, password)
          .applicationProtocols(Http3.supportedApplicationProtocols())
          .build();
    }
  }
}
