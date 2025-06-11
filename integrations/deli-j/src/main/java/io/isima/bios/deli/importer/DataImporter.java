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

import io.isima.bios.deli.Deli;
import io.isima.bios.deli.PropertyNames;
import io.isima.bios.deli.deliverer.DeliveryChannel;
import io.isima.bios.deli.flow.DataFlow;
import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.deli.utils.Utils;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportDestinationType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DataImporter<OriginalDataT> {
  private static final Logger logger = LoggerFactory.getLogger(DataImporter.class);

  @Getter
  protected final Configuration configuration;
  @Getter
  protected final ImportSourceConfig importSourceConfig;
  @Getter
  protected final List<DataFlow<OriginalDataT>> dataFlows;

  @Getter
  protected ImportDestinationConfig proxyDestinationConfig;

  protected DataImporter(Configuration configuration, ImportSourceConfig importSourceConfig)
      throws InvalidConfigurationException {
    this.configuration = configuration;
    this.importSourceConfig = importSourceConfig;
    if (configuration.isProxyMode()) {
      final var conf = new ImportDestinationConfig();
      final var importSourceId = importSourceConfig.getImportSourceId();
      conf.setImportDestinationId(importSourceId);
      conf.setImportDestinationName(importSourceConfig.getImportSourceName() + " (webhook)");
      conf.setType(ImportDestinationType.WEBHOOK);
      final String destinationUrl = configuration.getAppProperties()
          .getProperty(PropertyNames.PROXY_DESTINATION_URL, "http://localhost:8081");
      try {
        final var encodedSourceId = URLEncoder.encode(importSourceId, "utf-8");
        conf.setEndpoint(String.format("%s/__cdc/%s", destinationUrl, encodedSourceId));
      } catch (UnsupportedEncodingException e) {
        throw new RuntimeException(e);
      }
      proxyDestinationConfig = conf;
      DeliveryChannel.registerChannel(configuration, proxyDestinationConfig);
    }
    dataFlows = buildDataFlows(configuration, importSourceConfig);
  }

  private List<DataFlow<OriginalDataT>> buildDataFlows(Configuration configuration,
      ImportSourceConfig importSourceConfig) {
    assert configuration.getTenantConfig() != null;
    assert configuration.getTenantConfig().getImportFlowSpecs() != null;
    final var dataFlows = new ArrayList<DataFlow<OriginalDataT>>();
    final String id = importSourceConfig.getImportSourceId();
    for (var flowSpec : configuration.getTenantConfig().getImportFlowSpecs()) {
      if (!id.equals(flowSpec.getSourceDataSpec().getImportSourceId())) {
        continue;
      }
      try {
        dataFlows.add(buildDataFlow(flowSpec));
      } catch (InvalidConfigurationException e) {
        logger.error("Failed go create an import flow, skipping; flowName={} flowId={}",
            flowSpec.getImportFlowName(), flowSpec.getImportFlowId(), e);
      }
    }
    return dataFlows;
  }

  protected abstract DataFlow<OriginalDataT> buildDataFlow(ImportFlowConfig flowSpec)
      throws InvalidConfigurationException;

  public abstract CompletableFuture<Void> start() throws IOException, InvalidConfigurationException;

  protected abstract void stop();

  public void shutdown() {
    logger.info("Shutting down");
    stop();
  }

  public void publish(OriginalDataT originalData, FlowContext context) throws InterruptedException {
    for (var dataFlow : dataFlows) {
      if (dataFlow.isForInputData(context)) {
        final int maxNumRetries = 16;
        final int initialSleepSeconds = 2;
        final int maxSleepSeconds = 60;
        final int timeoutSeconds = 900;  // 15 minutes TODO(Naoki): or never?
        final var logContext = String.format("flow=%s", dataFlow.getFlowName());
        try {
          Utils.executeRemoteOperation("Publishing", initialSleepSeconds, maxSleepSeconds,
              timeoutSeconds, Deli.isShuttingDown(), logger, logContext,
              () -> dataFlow.process(originalData, context));
        } catch (BiosClientException e) {
          logger.error("Publishing failed, flow={}, error={}", dataFlow.getFlowName(),
              e.getMessage());
        }
      }
    }
    if (!context.isSentAlready()) {
      logger.warn("No flow picked up the event; source={} ({})",
          importSourceConfig.getImportSourceName(), importSourceConfig.getImportSourceId());
    }
  }

  public String getName() {
    return importSourceConfig.getImportSourceName();
  }

  public String getId() {
    return importSourceConfig.getImportSourceId();
  }
}
