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
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.ImportDestinationConfig;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public abstract class DeliveryChannel {

  private static Map<String, DeliveryChannel> channels;

  static {
    channels = new LinkedHashMap<>();
  }

  public static DeliveryChannel registerChannel(Configuration configuration,
      ImportDestinationConfig importDestinationConfig) throws InvalidConfigurationException {
    Objects.requireNonNull(configuration);
    Objects.requireNonNull(importDestinationConfig);

    DeliveryChannel channel = getChannel(importDestinationConfig.getImportDestinationId());
    if (channel != null) {
      // REgistered already
      return channel;
    }
    final var destinationType = importDestinationConfig.getType();
    switch (destinationType) {
      case BIOS:
        channel = new BiosDeliveryChannel(configuration, importDestinationConfig);
        break;
      case WEBHOOK:
        channel = new WebhookDeliveryChannel(configuration, importDestinationConfig);
        break;
      default:
        throw new InvalidConfigurationException(
            "importDestination (%s): Destination type %s is unsupported",
            importDestinationConfig.getImportDestinationName(), destinationType);
    }
    channels.put(importDestinationConfig.getImportDestinationId(), channel);
    return channel;
  }

  public static DeliveryChannel getChannel(String channelId) {
    return channels.get(channelId);
  }

  public static Collection<DeliveryChannel> getChannels() {
    return channels.values();
  }

  protected final Configuration configuration;
  protected final ImportDestinationConfig importDestinationConfig;

  protected DeliveryChannel(Configuration configuration,
      ImportDestinationConfig importDestinationConfig) {
    this.configuration = configuration;
    this.importDestinationConfig = importDestinationConfig;
  }

  public String getName() {
    if (importDestinationConfig.getImportDestinationName() == null) {
      return getId();
    }
    return importDestinationConfig.getImportDestinationName();
  }

  public String getId() {
    return importDestinationConfig.getImportDestinationId();
  }

  public ImportDestinationConfig getDestinationConfig() {
    return importDestinationConfig;
  }

  public abstract Deliverer<?> makeDeliverer(ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException;

  public abstract void start() throws BiosClientException;

  public abstract void stop();

  // Utilities /////////////////////////////////////////////
  protected URL parseUrl(String endpoint, String propertyName)
      throws InvalidConfigurationException {
    try {
      return new URL(endpoint);
    } catch (MalformedURLException e) {
      throw new InvalidConfigurationException("destination '%s': Syntax error in property '%s': %s",
          getName(), propertyName, e.toString());
    }
  }

  protected String retrieveEndpoint(ImportDestinationConfig importDestinationConfig)
      throws InvalidConfigurationException {
    final var endpoint = importDestinationConfig.getEndpoint();
    final var propName = "endpoint";
    if (endpoint == null) {
      throw new InvalidConfigurationException("destination '%s': Property '%s' must be set",
          getName(), propName);
    }
    return endpoint;
  }
}
