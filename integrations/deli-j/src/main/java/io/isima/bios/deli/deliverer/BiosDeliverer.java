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
package io.isima.bios.deli.deliverer;

import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.BiosStreamConfig;
import io.isima.bios.models.CdcOperationType;
import io.isima.bios.models.ImportDestinationStreamType;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.IntegrationsAuthenticationType;
import io.isima.bios.sdk.Session;
import io.isima.bios.sdk.Statement;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BiosDeliverer extends Deliverer<BiosDeliveryChannel> {
  private static final Logger logger = LoggerFactory.getLogger(BiosDeliverer.class);

  protected final ImportFlowConfig importFlowSpec;

  @Getter
  protected final ImportDestinationStreamType streamType;

  @Getter
  protected final String streamName;
  protected final BiosStreamConfig streamConfig;

  protected final boolean inMessageAuth;

  protected BiosDeliverer(ImportDestinationStreamType streamType, BiosDeliveryChannel channel,
      Configuration configuration, ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException {
    super(channel);
    this.streamType = streamType;
    this.importFlowSpec = importFlowSpec;
    this.streamName = importFlowSpec.getDestinationDataSpec().getName();
    this.streamConfig = getStreamConfig(configuration, importFlowSpec);
    final var authentication =
        configuration.getImportSourceConfig(importFlowSpec.getSourceDataSpec().getImportSourceId())
            .getAuthentication();
    inMessageAuth = authentication != null
        ? authentication.getType() == IntegrationsAuthenticationType.IN_MESSAGE
        : false;
    if (!inMessageAuth && channel.getDefaultUser() == null) {
      throw new InvalidConfigurationException(
          "importFlowSpec[%s]: Destination login credentials must"
              + " be configured in destination '%s' but they are missing",
          importFlowSpec.getImportFlowName(), channel.getName());
    }
  }

  private BiosStreamConfig getStreamConfig(Configuration configuration,
      ImportFlowConfig importFlowSpec) throws InvalidConfigurationException {
    final BiosStreamConfig config;
    switch (streamType) {
      case SIGNAL:
        final var signals = configuration.getTenantConfig().getSignals();
        if (signals == null) {
          throw new InvalidConfigurationException(
              "Retrieved tenant configuration does not have signal configurations."
                  + " Check bootstrap user's permissions");
        }
        config = findStreamConfig(signals, streamName);
        break;
      case CONTEXT:
        final var contexts = configuration.getTenantConfig().getContexts();
        if (contexts == null) {
          throw new InvalidConfigurationException(
              "Retrieved tenant configuration does not have context configurations."
                  + " Check bootstrap user's permissions");
        }
        config = findStreamConfig(contexts, streamName);
        break;
      default:
        throw new UnsupportedOperationException(
            "getStreamConfig unimplemented for destination type " + streamType);
    }
    return config;
  }

  private <T extends BiosStreamConfig> T findStreamConfig(List<T> haystack, String needle)
      throws InvalidConfigurationException {
    for (var piece : haystack) {
      if (piece.getName().equalsIgnoreCase(needle)) {
        return piece;
      }
    }
    throw new InvalidConfigurationException(
        "importFlowSpec[%s].destinationDataSpec: %s '%s' not found in tenant configuration",
        importFlowSpec.getImportFlowName(), streamType.stringify(), streamName);
  }

  // TODO(BIOS-2553): Hack to workaround (looking to be) unnecessary restriction on server
  protected void preprocess(FlowContext context) {}

  public abstract Statement buildStatement(FlowContext context);

  @Override
  public void deliver(FlowContext context) throws BiosClientException {

    preprocess(context);

    final var statement = buildStatement(context);
    if (statement == null) {
      logger.warn("No records to deliver"); // TODO(Naoki): Put some info to identify the request
      return;
    }

    final int maxTrials = 3;
    int trialsCredit = maxTrials;
    while (trialsCredit > 0) {
      --trialsCredit;
      try {
        if (trialsCredit < maxTrials - 1) {
          logger.info("retrying...");
        }
        deliverCore(context, statement);
        break;
      } catch (BiosClientException e) {
        String what = null;
        if (e.getCode() == BiosClientError.SESSION_EXPIRED) {
          what = "Session expired";
          channel.deleteSession(context.getUser());
        } else if (e.getCode() == BiosClientError.OPERATION_CANCELLED) {
          what = "Operation cancelled";
          try {
            Thread.sleep(1);
          } catch (InterruptedException e1) {
            // ok
          }
        }

        if (what != null) {
          logger.warn("{}, retrying to deliver", what);
        } else {
          throw e;
        }
      }
    }
  }

  private void deliverCore(FlowContext context, Statement statement)
      throws BiosClientException {
    final Session session;
    if (inMessageAuth) {
      session = channel.getSession(context.getUser(), context.getPassword());
    } else {
      session = channel.getDefaultSession();
    }
    session.execute(statement);
    logger.info("{}; flow={}, {}={}, elapsed_ms={}",
        context.getOperationType() == CdcOperationType.DELETE ? "deleted" : "inserted",
        importFlowSpec.getImportFlowName(), streamType, streamName, context.getElapsedTime());
  }

  protected String makeCsvText(FlowContext context) {
    final var record = context.getRecord();
    final var text = streamConfig.getAttributes().stream()
        .map((attribute) -> {
          final Object value = record.get(attribute.getName().toLowerCase());
          return (value != null) ? '"' + value.toString().replace("\"", "\"\"") + '"' : "";
        })
        .collect(Collectors.joining(","));
    return text;
  }

}
