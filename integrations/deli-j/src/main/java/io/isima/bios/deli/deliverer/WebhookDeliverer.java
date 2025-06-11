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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.deli.PropertyNames;
import io.isima.bios.deli.models.Configuration;
import io.isima.bios.deli.models.FlowContext;
import io.isima.bios.deli.models.InvalidConfigurationException;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.IntegrationsAuthenticationType;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.exceptions.BiosClientException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WebhookDeliverer extends Deliverer {
  private static final Logger logger = LoggerFactory.getLogger(WebhookDeliverer.class);
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final HttpClient httpClient;
  private final String flowName;
  private final URI endpoint;
  private final String userHeaderName;
  private final String passwordHeaderName;
  private final String userName;
  private final String password;

  protected WebhookDeliverer(WebhookDeliveryChannel channel,
      Configuration configuration, ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException {
    super(channel);
    flowName = importFlowSpec.getImportFlowName();
    final String encodedTableName;
    try {
      final String flowSpecName =
          String.format("importFlowSpec: '%s'", importFlowSpec.getImportFlowName());
      if (importFlowSpec.getSourceDataSpec() == null) {
        throw new InvalidConfigurationException("%: Property 'sourceDataSpec' is required",
            flowSpecName);
      }
      if (importFlowSpec.getSourceDataSpec().getTableName() == null) {
        throw new InvalidConfigurationException(
            "%.sourceDataSpec: Property 'tableName' is required", flowSpecName);
      }
      encodedTableName =
          URLEncoder.encode(importFlowSpec.getSourceDataSpec().getTableName(), "utf-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
    endpoint = URI.create(String.format("%s/%s", channel.getEndpointUrl(), encodedTableName));
    httpClient = channel.getHttpClient();
    final var credentials = retrieveUserCredentials(configuration, importFlowSpec);
    final var props = configuration.getAppProperties();
    userHeaderName = props.getProperty(PropertyNames.USER_HEADER_NAME, "x-bios-username");
    passwordHeaderName = props.getProperty(PropertyNames.PASSWORD_HEADER_NAME, "x-bios-password");
    userName = credentials[0];
    password = credentials[1];
  }

  private String[] retrieveUserCredentials(Configuration configuration,
      ImportFlowConfig importFlowSpec)
      throws InvalidConfigurationException {
    final var destinationDataSpec = importFlowSpec.getDestinationDataSpec();
    final String flowSpecName =
        String.format("importFlowSpec: '%s'", importFlowSpec.getImportFlowName());
    if (destinationDataSpec == null) {
      throw new InvalidConfigurationException("%: Property 'destinationDataSpec' is required",
          flowSpecName);
    }
    final var destinationId = destinationDataSpec.getImportDestinationId();
    if (destinationId == null) {
      throw new InvalidConfigurationException(
          "%s: destinationDataSpec is missing 'importDestinationId'", flowSpecName);
    }
    final var destinationConfig = configuration.getImportDestinationConfig(destinationId);
    if (destinationConfig == null) {
      throw new InvalidConfigurationException("%s: Pointing destination '%s' not found",
          flowSpecName, destinationId);
    }
    final var destinationName =
        String.format("importDestination: '%s'", destinationConfig.getImportDestinationName());
    final var auth = destinationConfig.getAuthentication();
    if (auth == null) {
      throw new InvalidConfigurationException("%s, %s: Property 'authentication' is required",
          flowSpecName, destinationName);
    }
    if (auth.getType() != IntegrationsAuthenticationType.LOGIN) {
      throw new InvalidConfigurationException("%s, %s: Authentication type must be 'Login'",
          flowSpecName, destinationName);
    }
    final var user = auth.getUser();
    if (user == null) {
      throw new InvalidConfigurationException("%s, %s: 'authentication.user' is required",
          flowSpecName, destinationName);
    }
    final var pass = auth.getPassword();
    if (pass == null) {
      throw new InvalidConfigurationException("%s, %s: 'authentication.password' is required",
          flowSpecName, destinationName);
    }
    return new String[] {user, pass};
  }

  @Override
  public void deliver(FlowContext context) throws BiosClientException, InterruptedException {
    final var payload = new WebhookPayload(context.getRecord(), context.getMetadata());
    try {
      final var request = HttpRequest.newBuilder()
          .uri(endpoint)
          .timeout(Duration.ofMinutes(1))
          .header("Content-Type", "application/json")
          .header(userHeaderName, userName)
          .header(passwordHeaderName, password)
          .POST(HttpRequest.BodyPublishers.ofByteArray(objectMapper.writeValueAsBytes(payload)))
          .build();
      final var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      checkResponse(response);
      logger.info("forwarded; flow='{}', op={}, url={}, elapsed_ms={}",
          flowName, context.getOperationType(), endpoint, context.getElapsedTime());
    } catch (JsonProcessingException e) {
      throw new BiosClientException(BiosClientError.BAD_INPUT, e.getMessage());
    } catch (IOException e) {
      throw new BiosClientException(BiosClientError.BAD_GATEWAY, e.getMessage());
    }
  }

  private void checkResponse(HttpResponse<String> response) throws BiosClientException {
    final var statusCode = response.statusCode();
    final int nxx = statusCode / 100;
    if (nxx == 2) {
      return;
    }
    final var body = response.body() != null ? response.body().trim() : "";
    logger.debug("Received status {}: {}", statusCode, body);
    final var errorMap =
        Map.of(
            400, BiosClientError.BAD_INPUT,
            401, BiosClientError.UNAUTHORIZED,
            403, BiosClientError.FORBIDDEN,
            404, BiosClientError.NOT_FOUND,
            409, BiosClientError.RESOURCE_ALREADY_EXISTS,
            500, BiosClientError.GENERIC_SERVER_ERROR,
            502, BiosClientError.BAD_GATEWAY,
            503, BiosClientError.SERVICE_UNAVAILABLE,
            504, BiosClientError.TIMEOUT);
    var error = errorMap.get(statusCode);
    if (error == null) {
      error = nxx == 4 ? BiosClientError.BAD_INPUT : BiosClientError.GENERIC_SERVER_ERROR;
    }
    if (error != null) {
      throw new BiosClientException(error, String.format("%s (status=%d)", body, statusCode));
    }
  }
}
