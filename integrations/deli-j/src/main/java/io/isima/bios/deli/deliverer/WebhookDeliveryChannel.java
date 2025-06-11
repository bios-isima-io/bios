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
import java.net.http.HttpClient;
import java.time.Duration;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebhookDeliveryChannel extends DeliveryChannel {
  private static final Logger logger = LoggerFactory.getLogger(WebhookDeliveryChannel.class);

  @Getter
  private final String endpointUrl;

  @Getter
  private final HttpClient httpClient;

  protected WebhookDeliveryChannel(Configuration configuration,
      ImportDestinationConfig importDestinationConfig) throws InvalidConfigurationException {
    super(configuration, importDestinationConfig);

    endpointUrl = retrieveEndpoint(importDestinationConfig);

    httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(20))
        .build();
  }

  @Override
  public Deliverer<?> makeDeliverer(ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException {
    final var destinationSpec = importFlowSpec.getDestinationDataSpec();
    if (destinationSpec == null) {
      throw new InvalidConfigurationException(
          "importFlowSpec '%s': Property 'destinationDataSpec' is required",
          importFlowSpec.getImportFlowName());
    }
    return new WebhookDeliverer(this, configuration, importFlowSpec);
  }

  @Override
  public void start() throws BiosClientException {}

  @Override
  public void stop() {}
}
