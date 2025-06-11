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
package io.isima.bios.deli;

import static io.isima.bios.deli.Constants.CLIENT_CERTIFICATE_FILE;
import static io.isima.bios.deli.Constants.CLIENT_CERTIFICATE_PASSWORD_FILE;
import static io.isima.bios.deli.Constants.ROOT_CA_TRUSTSTORE;
import static io.isima.bios.deli.Constants.ROOT_CA_TRUSTSTORE_PASSWORD;

import io.isima.bios.deli.deliverer.BiosDeliverer;
import io.isima.bios.deli.deliverer.DeliveryChannel;
import io.isima.bios.deli.importer.CdcDataImporter;
import io.isima.bios.deli.importer.DataImporter;
import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.deli.utils.Utils;
import io.isima.bios.models.ImportSourceType;
import io.isima.bios.models.TenantConfig;
import io.isima.bios.sdk.Bios;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Deli {
  private static final Logger logger = LoggerFactory.getLogger(Deli.class);
  private static final AtomicBoolean shuttingDown = new AtomicBoolean(false);
  private static final String SSL_CERT_FILE = "SSL_CERT_FILE";

  public static AtomicBoolean isShuttingDown() {
    return shuttingDown;
  }

  public Deli(String propsFilePath, ImportSourceType importSourceType) throws Exception {
    final Properties properties = new Properties();
    logger.info("Setting up deli...");
    final var deliHome = System.getenv().getOrDefault("DELI_HOME", System.getProperty("user.dir"));
    final var dataDir = System.getenv().getOrDefault("DELI_DATA", System.getProperty("user.dir"));

    try (final InputStream stream = new FileInputStream(propsFilePath)) {
      properties.load(stream);
    } catch (Exception e) {
      logger.error("Please ensure that deli.properties file exists");
      logger.error("Error occurred while loading Deli config file {}", propsFilePath, e);
      throw e;
    }
    // Put javax properties in the properties file to the system
    properties.forEach((key, value) -> {
      final var keyStr = (String) key;
      final var valueStr = (String) value;
      System.setProperty(keyStr, valueStr);
    });
    final var configuration = new Configuration(deliHome, dataDir, properties);

    // Hack for MongoDB importer to set system properties for TLS configuration.
    //
    // Debezium MongoDB connector relies on java.net.ssl.* system properties, but they may
    // come from the import source config. So the MongoDB configurator (that runs in a later stage)
    // writes specified keyStore and/or trustStore to ${DATA_DIR}/keystore.jks and to
    // ${DATA_DIR}/truststore.jks respectively. The writing is done only after the TLS classes
    // are loaded, so setting up the keyStore and trustStore is too late for configure the secured
    // connection in the process then. In order to work around this issue, the MongoDB configurator
    // force terminating the process after setting up the files. When the process restarts,
    // we load the key store and trust store here before the TLS classes are loaded.
    final var clientCertFile = new File(configuration.getDataDir() + CLIENT_CERTIFICATE_FILE);
    if (clientCertFile.exists()) {
      System.setProperty("javax.net.ssl.keyStore",
          configuration.getDataDir() + CLIENT_CERTIFICATE_FILE);
      final var keystorePassword =
          Utils.readFile(configuration.getDataDir() + CLIENT_CERTIFICATE_PASSWORD_FILE);
      if (keystorePassword != null) {
        System.setProperty("javax.net.ssl.keyStorePassword", new String(keystorePassword));
      }
    }

    final var rootCaFile = new File(configuration.getDataDir() + ROOT_CA_TRUSTSTORE);
    if (rootCaFile.exists()) {
      System.setProperty("javax.net.ssl.trustStore",
          configuration.getDataDir() + ROOT_CA_TRUSTSTORE);
      final var truststorePassword =
          Utils.readFile(configuration.getDataDir() + ROOT_CA_TRUSTSTORE_PASSWORD);
      if (truststorePassword != null) {
        System.setProperty("javax.net.ssl.trustStorePassword", new String(truststorePassword));
      }
    }

    try {
      final var tenantConfig = new AtomicReference<TenantConfig>();
      final var url = new URL(properties.getProperty(PropertyNames.ENDPOINT, "https://localhost"));
      final var host = url.getHost();
      final var port = url.getPort() >= 0 ? url.getPort() : 443;
      String user = properties.getProperty(PropertyNames.USER);
      String password = properties.getProperty(PropertyNames.PASSWORD);
      if (user == null || password == null) {
        throw new InvalidConfigurationException("Property file " + propsFilePath
            + ": Parameter 'tenant' or 'user' & 'password' must be set ");
      }
      final int initialSleepSeconds = 2;
      final int maxSleepSeconds = 60;
      final int timeoutSeconds = 300;  // 5 minutes
      final var logContext = String.format("endpoint=%s", url);
      Utils.executeRemoteOperation("Retrieving tenant config", initialSleepSeconds,
          maxSleepSeconds,
          timeoutSeconds, shuttingDown, logger, logContext,
          () -> {
            String sslCertFile = null;
            if (System.getProperty(SSL_CERT_FILE) != null) {
              final var certFilePath = Paths.get(System.getProperty(SSL_CERT_FILE));
              if (Files.exists(certFilePath)) {
                sslCertFile = System.getProperty(SSL_CERT_FILE);
              }
            }
            try (var session = Bios.newSession(host).port(port).user(user).password(password)
                .sslCertFile(sslCertFile).connect()) {
              tenantConfig.set(session.getTenant(true, false));
            } catch (Exception e) {
              if (e instanceof BiosClientException) {
                throw (BiosClientException) e;
              }
              if (e instanceof InterruptedException) {
                throw (InterruptedException) e;
              }
              throw new BiosClientException(e);
            }
          });

      configuration.registerTenantConfig(tenantConfig.get());

      final var importSourceConfigs = configuration.getImportSourceConfigs(importSourceType);
      final var importers = new ArrayList<AtomicReference<DataImporter<?>>>();
      for (var importSourceConfig : importSourceConfigs) {
        try {
          final var importer = new CdcDataImporter(configuration, importSourceConfig);
          importers.add(new AtomicReference<>(importer));
        } catch (InternalError e) {
          // This may happen when the SSL cert is misconfigured
          Throwable cause = e.getCause();
          while (cause != null) {
            if (cause instanceof NoSuchAlgorithmException || cause instanceof KeyStoreException) {
              logger.error("A fatal keystore error encountered:", e);
              if (clientCertFile.exists()) {
                Files.delete(clientCertFile.toPath());
              }
              if (rootCaFile.exists()) {
                Files.delete(rootCaFile.toPath());
              }
              System.exit(1);
            }
            cause = cause.getCause();
          }
          throw e;
        }
      }
      logConfiguration(configuration, importers);
      logger.info("Setting up done.");
      if (importers.isEmpty()) {
        logger.warn("No import sources for {} are found, shutting down immediately.",
            importSourceType);
      }
      CompletableFuture<?>[] shutdownFutures = new CompletableFuture<?>[importSourceConfigs.size()];
      if (importers.size() > 1) {
        throw new InvalidConfigurationException("Multiple importers are not supported");
      }
      for (int i = 0; i < importers.size(); ++i) {
        final var importer = importers.get(i);
        final var shutdownFuture = new CompletableFuture<Void>();
        shutdownFutures[i] = shutdownFuture;
        startImporter(importer, shutdownFuture);
      }
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        logger.info("Shutting down");
        shuttingDown.set(true);
        for (var importer : importers) {
          importer.get().shutdown();
        }
      }));
      CompletableFuture.allOf(shutdownFutures).get();
      logger.info("Bye");
    } catch (Exception e) {
      logger.error("Startup failed", e);
      throw e;
    }
  }

  private void startImporter(AtomicReference<DataImporter<?>> importerRef,
      CompletableFuture<Void> shutdownFuture) {
    final var currentImporter = importerRef.get();
    try {
      currentImporter
          .start()
          .whenComplete((none, t) -> {
                if (t == null) {
                  shutdownFuture.complete(null);
                }
                try {
                  final var nextImporter = new CdcDataImporter(currentImporter.getConfiguration(),
                      currentImporter.getImportSourceConfig());
                  importerRef.set(nextImporter);
                  // ugly but quick to implement
                  CompletableFuture
                      .runAsync(() -> {
                        try {
                          Thread.sleep(10000);
                        } catch (InterruptedException e) {
                          throw new RuntimeException(e);
                        }
                      })
                      .thenRun(() -> startImporter(importerRef, shutdownFuture));
                } catch (InvalidConfigurationException e) {
                  // this shouldn't happen since initialization is done once
                  logger.error("Importer start retry failed", e);
                  shutdownFuture.complete(null);
                }
              }
          );
    } catch (IOException | InvalidConfigurationException e) {
      // Not a recoverable error.
      logger.error("Importer failed to start", e);
      shutdownFuture.completeExceptionally(e);
    }
  }

  private void logConfiguration(Configuration configuration,
      ArrayList<AtomicReference<DataImporter<?>>> importers) {
    if (importers.isEmpty()) {
      return;
    }
    logger.info("Importers:");
    for (var importerRef : importers) {
      final var importer = importerRef.get();
      logger.info("  {}: {}: {}",
          importer.getId(), importer.getImportSourceConfig().getType(), importer.getName());
    }
    logger.info("Delivery Channels:");
    for (var channel : DeliveryChannel.getChannels()) {
      logger.info("  {}: {}: {}",
          channel.getId(), channel.getDestinationConfig().getType(), channel.getName());
    }
    logger.info("Data Flows:");
    for (var importerRef : importers) {
      final var importer = importerRef.get();
      for (var flow : importer.getDataFlows()) {
        final var deliverer = flow.getDeliverer();
        if (deliverer instanceof BiosDeliverer) {
          @SuppressWarnings("rawtypes") final var bdeliv = (BiosDeliverer) deliverer;
          logger.info("  {}: <{}> -> <{}> ({} {})",
              flow.getFlowName(), importer.getName(), bdeliv.getDestinationName(),
              bdeliv.getStreamType(), bdeliv.getStreamName());
        } else {
          logger.info("  {}: <{}> -> <{}>",
              flow.getFlowName(), importer.getName(), deliverer.getDestinationName());
        }
      }
    }
  }

  private static void usage() {
    System.err.println(
        "Usage: java " + Deli.class.getName()
            + " /path/to/deli.properties <mysql|postgres|oracle|sqlserver>");
    System.exit(1);
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      usage();
    }
    final String propsFilePath = args[0];
    ImportSourceType importSourceType = null;
    try {
      importSourceType = ImportSourceType.valueOf(args[1].toUpperCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Unknown importer name " + args[1]);
      usage();
    }
    try {
      new Deli(propsFilePath, importSourceType);
    } catch (Throwable t) {
      logger.info("A fatal error encountered", t);
      System.exit(0);
    }
  }

}
