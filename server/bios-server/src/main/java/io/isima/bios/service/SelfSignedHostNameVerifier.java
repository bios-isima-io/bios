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

import java.security.cert.X509Certificate;
import java.util.Arrays;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;

public class SelfSignedHostNameVerifier implements HostnameVerifier {
  @Override
  public boolean verify(String hostname, SSLSession session) {
    try {
      final var peerCerts = session.getPeerCertificates();
      if (peerCerts.length < 1 || !(peerCerts[0] instanceof X509Certificate)) {
        return false;
      }
      final var cert = (X509Certificate) peerCerts[0];
      final var dn = cert.getSubjectDN().getName();
      final var elements = Arrays.asList(dn.split(","));
      return elements.contains("CN=" + hostname);
    } catch (SSLPeerUnverifiedException e) {
      return false;
    }
  }
}
