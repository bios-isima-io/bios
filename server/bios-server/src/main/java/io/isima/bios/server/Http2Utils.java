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
import io.isima.bios.maintenance.ServiceStatus;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2SecurityUtil;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.ApplicationProtocolConfig.Protocol;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectedListenerFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolConfig.SelectorFailureBehavior;
import io.netty.handler.ssl.ApplicationProtocolNames;
import io.netty.handler.ssl.ApplicationProtocolNegotiationHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import javax.net.ssl.KeyManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Http2Utils {
  private static final Logger logger = LoggerFactory.getLogger(Http2Utils.class);

  private static final String PATTERN_RFC1123 = "EEE, dd MMM yyyy HH:mm:ss z";
  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern(PATTERN_RFC1123).withZone(ZoneId.of("GMT"));

  public static SslContext createSslContext()
      throws CertificateException,
          IOException,
          KeyStoreException,
          NoSuchAlgorithmException,
          UnrecoverableKeyException {

    final String keyStorePath = Bios2Config.sslKeyStoreFile();
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
      return SslContextBuilder.forServer(kmf)
          .sslProvider(SslProvider.JDK)
          .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
          .applicationProtocolConfig(
              new ApplicationProtocolConfig(
                  Protocol.ALPN,
                  SelectorFailureBehavior.NO_ADVERTISE,
                  SelectedListenerFailureBehavior.ACCEPT,
                  ApplicationProtocolNames.HTTP_2))
          .build();
    }
  }

  public static SslContext createSslContext2nd()
      throws CertificateException,
          IOException,
          KeyStoreException,
          NoSuchAlgorithmException,
          UnrecoverableKeyException {

    final String keyStorePath = Bios2Config.sslKeyStoreFile2nd();
    final String password = Bios2Config.sslKeyStorePassword2nd();
    final String keystoreType = Bios2Config.sslKeyStoreType2nd();

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
      return SslContextBuilder.forServer(kmf)
          .sslProvider(SslProvider.JDK)
          .ciphers(Http2SecurityUtil.CIPHERS, SupportedCipherSuiteFilter.INSTANCE)
          .applicationProtocolConfig(
              new ApplicationProtocolConfig(
                  Protocol.ALPN,
                  SelectorFailureBehavior.NO_ADVERTISE,
                  SelectedListenerFailureBehavior.ACCEPT,
                  ApplicationProtocolNames.HTTP_2))
          .build();
    }
  }

  public static ApplicationProtocolNegotiationHandler getHandler(
      ServiceRouter resolver, ServiceStatus serviceStatus) {
    return new ApplicationProtocolNegotiationHandler(ApplicationProtocolNames.HTTP_2) {
      @Override
      protected void configurePipeline(ChannelHandlerContext ctx, String protocol)
          throws Exception {
        if (ApplicationProtocolNames.HTTP_2.contentEquals(protocol)) {
          ctx.pipeline()
              .addLast(
                  Http2FrameCodecBuilder.forServer().build(),
                  new Http2FrameHandler(resolver, serviceStatus));
          return;
        }
        throw new IllegalStateException("Protocol: " + protocol + " not supported");
      }

      @Override
      public void handshakeFailure(ChannelHandlerContext ctx, Throwable cause) {
        final var channel = ctx.channel();
        logger.warn(
            "Handshake failed; remote={}, error={}", channel.remoteAddress(), cause.toString());
      }
    };
  }

  public static String toRfc1123Date(LocalDateTime time) {
    return time.format(DATE_FORMATTER);
  }
}
