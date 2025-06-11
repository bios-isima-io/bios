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
package io.isima.bios.service;

import io.isima.bios.common.TfosConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Class to manage HTTP clients (creation, pooling, etc). */
public class HttpClientManager {
  private static final Logger logger = LoggerFactory.getLogger(HttpClientManager.class);
  private static final String X509_CERT_FACTORY = "X.509";

  private final Map<String, OkHttpClient> httpClients;
  private final Map<String, OkHttpClient> httpClientsInsecure;
  private final String cafile;

  public HttpClientManager() {
    httpClients = new ConcurrentHashMap<>();
    httpClientsInsecure = new ConcurrentHashMap<>();
    cafile = TfosConfig.cafile();
  }

  /**
   * Returns HTTP client for specified endpoint.
   *
   * @param endpoint Target endpoint
   * @return OkHttpClient instance for the specified endpoint
   */
  public OkHttpClient getClient(String endpoint) {
    if (cafile == null) {
      throw new RuntimeException(
          "CA file should be configured when ssl is enabled. Check property "
              + TfosConfig.SSL_CERT_FILE);
    }
    final boolean isSecure = endpoint.startsWith("https:");
    var clients = isSecure ? httpClients : httpClientsInsecure;
    var client = clients.get(endpoint);
    if (client == null) {
      client = isSecure ? buildSslHttpClient() : buildHttpClient();
      httpClients.put(endpoint, client);
    }
    return client;
  }

  private OkHttpClient buildHttpClient() {
    return generateClientBuilder().build();
  }

  private OkHttpClient buildSslHttpClient() {
    final SSLContext sslContext = getSslContextForCertificateFile(cafile);
    final var builder =
        generateClientBuilder()
            .sslSocketFactory(sslContext.getSocketFactory(), new X509TrustManagerImpl())
            .pingInterval(10, TimeUnit.SECONDS);
    if (TfosConfig.useSelfSignedCertForFanRouting()) {
      builder.hostnameVerifier(new SelfSignedHostNameVerifier());
    }
    return builder.build();
  }

  private SSLContext getSslContextForCertificateFile(String fileName) {
    try {
      KeyStore keyStore = getKeyStore(fileName);
      SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(keyStore);
      sslContext.init(null, trustManagerFactory.getTrustManagers(), new SecureRandom());
      return sslContext;
    } catch (Exception e) {
      throw new RuntimeException("Failed to load certificate file", e);
    }
  }

  private KeyStore getKeyStore(String fileName)
      throws CertificateException, IOException, KeyStoreException, NoSuchAlgorithmException {
    KeyStore keyStore;
    CertificateFactory cf = CertificateFactory.getInstance(X509_CERT_FACTORY);
    final InputStream inputStream = new FileInputStream(fileName);
    Collection<? extends Certificate> certificates;
    try {
      certificates = cf.generateCertificates(inputStream);
    } finally {
      inputStream.close();
    }
    String keyStoreType = KeyStore.getDefaultType();
    keyStore = KeyStore.getInstance(keyStoreType);
    keyStore.load(null, null);
    int index = 0;
    for (Certificate certificate : certificates) {
      String certificateAlias = Integer.toString(index++);
      keyStore.setCertificateEntry(certificateAlias, certificate);
    }
    return keyStore;
  }

  private OkHttpClient.Builder generateClientBuilder() {
    // TODO(Naoki): Parameterize the constants
    return new OkHttpClient()
        .newBuilder()
        .readTimeout(60, TimeUnit.SECONDS)
        .writeTimeout(60, TimeUnit.SECONDS)
        .connectionPool(new ConnectionPool(5, 300, TimeUnit.SECONDS));
  }
}
