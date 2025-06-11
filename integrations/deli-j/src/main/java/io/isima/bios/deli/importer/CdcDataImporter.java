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
package io.isima.bios.deli.importer;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import io.isima.bios.deli.flow.DataFlow;
import io.isima.bios.deli.flow.RelationalDataFlow;
import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CdcDataImporter extends DataImporter<Map<String, Object>> {
  private static final Logger logger;
  private static final Map<ImportSourceType, ConnectorConfigurator<?>> configurators;

  static {
    logger = LoggerFactory.getLogger(CdcDataImporter.class);
    configurators = new HashMap<>();
    configurators.put(ImportSourceType.MYSQL, new MySqlConnectorConfigurator());
    configurators.put(ImportSourceType.MONGODB, new MongoDbConnectorConfigurator());
    configurators.put(ImportSourceType.POSTGRES, new PostgresConnectorConfigurator());
    configurators.put(ImportSourceType.ORACLE, new OracleConnectorConfigurator());
    configurators.put(ImportSourceType.SQLSERVER, new SqlServerConnectorConfigurator());
  }

  private final ConnectorConfigurator<?> configurator;
  private final CdcEventHandler eventHandler;

  DebeziumEngine<ChangeEvent<String, String>> engine;
  private final CompletableFuture<Void> shutdownFuture;

  public CdcDataImporter(Configuration configuration, ImportSourceConfig importSourceConfig)
      throws InvalidConfigurationException {
    super(configuration, importSourceConfig);

    final var type = importSourceConfig.getType();
    configurator = configurators.get(type);
    if (configurator == null) {
      throw new UnsupportedOperationException("Data importer type " + type + " is unsupported");
    }

    this.eventHandler = configurator.getCdcEventHandler();
    shutdownFuture = new CompletableFuture<>();
  }

  @Override
  protected DataFlow<Map<String, Object>> buildDataFlow(ImportFlowConfig flowSpec)
      throws InvalidConfigurationException {
    return new RelationalDataFlow(configuration, importSourceConfig, flowSpec);
  }

  @Override
  public CompletableFuture<Void> start() throws IOException, InvalidConfigurationException {
    logger.info("Starting Importer; sourceName={}, sourecId={}, sourceType={}",
        this.getName(), this.getId(), importSourceConfig.getType());
    final var connectorMonitor = new ConnectorMonitor();
    final var completionStatus = new CompletionStatus();
    final var props = configurator.makeProperties(configuration, importSourceConfig, this);

    engine = DebeziumEngine
        .create(Json.class)
        .using(props)
        .using(completionStatus)
        .using(connectorMonitor)
        .notifying(new MyChangeConsumer())
        .build();

    ExecutorService executor = Executors.newSingleThreadExecutor(new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        final String srcName =
            importSourceConfig.getImportSourceName().toLowerCase().replace(" ", "-");
        thread.setName("src-" + srcName.substring(0, Math.min(16, srcName.length())));
        return thread;
      }
    });
    executor.submit(engine);

    return shutdownFuture;
  }

  private void handleEvent(ChangeEvent<String, String> event) throws InterruptedException {
    final var context = new FlowContext(System.currentTimeMillis());
    final var originalData = eventHandler.handle(event, context);
    if (originalData != null) {
      logger.debug("Handle event successful. Publishing data ... {}", originalData);
      publish(originalData, context);
    }
  }

  class MyChangeConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<String, String>> {
    @Override
    public void handleBatch(List<ChangeEvent<String, String>> events,
        DebeziumEngine.RecordCommitter<ChangeEvent<String, String>> recordCommitter)
        throws InterruptedException {
      for (var event : events) {
        handleEvent(event);
        recordCommitter.markProcessed(event);
      }
      recordCommitter.markBatchFinished();
    }
  }

  @Override
  public void stop() {
    if (engine != null) {
      try {
        engine.close();
      } catch (IOException e) {
        logger.warn("Error happened while shutting down importer {}",
            importSourceConfig.getImportSourceName(), e);
      }
    }
  }

  @Accessors(fluent = true)
  @Getter
  private class ConnectorMonitor implements DebeziumEngine.ConnectorCallback {
    private boolean started;

    public ConnectorMonitor() {
      started = false;
    }

    @Override
    public void connectorStarted() {
      logger.debug("CONNECTOR STARTED");
      started = true;
    }

    @Override
    public void connectorStopped() {
      logger.debug("CONNECTOR STOPPED");
      started = false;
    }

    @Override
    public void taskStarted() {
      logger.info("Data Importer started");
    }

    @Override
    public void taskStopped() {
      logger.info("Data Importer stopped");
    }
  }

  @Accessors(fluent = true)
  @Getter
  private class CompletionStatus implements DebeziumEngine.CompletionCallback {
    private boolean completed;
    private boolean success;
    private String message;
    private Throwable error;

    public CompletionStatus() {
      completed = false;
    }

    @Override
    public void handle(boolean success, String message, Throwable error) {
      completed = true;
      this.success = success;
      this.message = message;
      this.error = error;
      if (success) {
        logger.debug("Importer stopped successfully; message={}", message);
        shutdownFuture.complete(null);
      } else if (error != null) {
        logger.error("IMPORTER STOPPED WITH ERROR; message={}", message, error);
        shutdownFuture.completeExceptionally(error);
      } else {
        logger.error("IMPORTER STOPPED WITH ERROR; message={}, error=n/a", message);
        shutdownFuture.completeExceptionally(new RuntimeException(message));
      }
    }
  }
}
