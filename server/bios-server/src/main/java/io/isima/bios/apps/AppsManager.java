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
package io.isima.bios.apps;

import io.isima.bios.admin.ResourceAllocator;
import io.isima.bios.admin.ResourceType;
import io.isima.bios.admin.TenantAppendix;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.errors.exception.AlreadyExistsException;
import io.isima.bios.errors.exception.BiosAppsDeploymentException;
import io.isima.bios.errors.exception.InvalidConfigurationException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AppsInfo;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.utils.Utils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/** Class that manages BiosApps service containers. */
@Slf4j
public class AppsManager {
  // property key to define default Apps service hosts, comma separated list of names.
  public static final String PROP_APPS_HOSTS = "prop.apps.hosts";

  // property key to define reservation seconds for a released resource, i.e., new tenant cannot use
  // released resource for a certain period
  public static final String PROP_APPS_RESOURCE_RESERVATION_SECONDS =
      "prop.apps.resourceReservationSeconds";
  public static final int DEFAULT_RESOURCE_RESERVATION_SECONDS = 30 * 24 * 3600; // 30 days

  // property key to define the deployment command path
  public static final String PROP_APPS_DEPLOYMENT_COMMAND = "prop.apps.deploymentCommand";
  public static final String DEFAULT_DEPLOYMENT_COMMAND = "/opt/bios/provision-apps";

  // parameter necessary for auto provisioning
  public static final String PROP_APPS_LB_HOSTS = "prop.apps.lbHosts";
  public static final String PROP_APPS_MAINTAINER_HOST = "prop.apps.maintainerHost";
  public static final String PROP_APPS_BIOS_ENDPOINT = "prop.apps.biosEndpoint";
  public static final String PROP_APPS_SSH_KEYPAIR = "prop.apps.sshKeypair";
  public static final String DEFAULT_SSH_KEYPAIR = "/opt/bios/bios.pem";

  private final TenantAppendix tenantAppendix;
  private final ResourceAllocator resourceAllocator;
  private final SharedConfig sharedConfig;

  private final int initialControlPort = 9001;
  private final int initialWebhookPort = 8081;

  /** The constructor. */
  public AppsManager(
      TenantAppendix tenantAppendix,
      ResourceAllocator resourceAllocator,
      SharedConfig sharedConfig) {
    this.tenantAppendix = tenantAppendix;
    this.resourceAllocator = resourceAllocator;
    this.sharedConfig = sharedConfig;
  }

  /**
   * Registers an apps service.
   *
   * <p>The method validates the Apps specification and stores it as a tenant appendix.
   *
   * <p>If the control and webhook ports are specified in the appsInfo, the method checks if they
   * are taken by other services already. The method throws AlreadyExistsException if at least on of
   * them has conflict with already registered services.
   *
   * <p>If the control and webhook ports are not specified in the appsInfo, the method allocates
   * available ones and uses them for the registration.
   *
   * @param tenantId ID of the tenant that uses the service
   * @param appsInfo Apps service specification
   * @return Registered apps service specification
   * @throws AlreadyExistsException Thrown to indicate that service is registered already for the
   *     tenant or the specified port numbers are already taken.
   * @throws ApplicationException Thrown to indicate that an unexpected error has happened
   * @throws InvalidConfigurationException Thrown to indicate that a misconfiguration prevented the
   *     operation
   */
  public AppsInfo register(EntityId tenantId, AppsInfo appsInfo)
      throws AlreadyExistsException, ApplicationException, InvalidConfigurationException {
    Objects.requireNonNull(tenantId);
    Objects.requireNonNull(appsInfo);

    final var tenantName = tenantId.getName();

    // We check fist whether the service is reagistered already.
    try {
      final var existing = getAppsInfo(tenantId);
      throw new AlreadyExistsException(
          String.format(
              "Apps service has been registered already; tenant=%s, appsHost=%s, controlPort=%d",
              tenantName, existing.getHosts(), existing.getControlPort()));
    } catch (NoSuchEntityException e) {
      // ok to proceed
    }

    // Resolve hosts
    final List<String> hosts;
    if (appsInfo.getHosts() != null && !appsInfo.getHosts().isEmpty()) {
      hosts = appsInfo.getHosts();
    } else {
      final var confValue = sharedConfig.getProperty(PROP_APPS_HOSTS);
      if (confValue.isBlank()) {
        final var message =
            String.format(
                "property %s is not set, failed to determine default Apps hosts;"
                    + " operation=RegisterAppsService, tenant=%s",
                PROP_APPS_HOSTS, tenantId.getName());
        logger.error(message);
        throw new InvalidConfigurationException(message);
      }
      hosts = new ArrayList<>();
      for (var element : confValue.split(",")) {
        hosts.add(element.trim());
      }
    }

    // Assign port numbers.
    final int controlPort;
    if (appsInfo.getControlPort() != null) {
      controlPort = appsInfo.getControlPort();
      resourceAllocator.assignResource(
          ResourceType.APPS_CONTROL_PORT, Integer.toString(controlPort), tenantName);
    } else {
      controlPort =
          resourceAllocator.allocateIntegerResource(
              ResourceType.APPS_CONTROL_PORT, initialControlPort, tenantName);
    }
    final int webhookPort;
    if (appsInfo.getWebhookPort() != null) {
      webhookPort = appsInfo.getWebhookPort();
      resourceAllocator.assignResource(
          ResourceType.APPS_WEBHOOK_PORT, Integer.toString(webhookPort), tenantName);
    } else {
      webhookPort =
          resourceAllocator.allocateIntegerResource(
              ResourceType.APPS_WEBHOOK_PORT, initialWebhookPort, tenantName);
    }

    // Add the apps info to the appendix
    final var registering = new AppsInfo(tenantName, hosts, controlPort, webhookPort);
    tenantAppendix.createEntry(tenantId, TenantAppendixCategory.APPS_INFO, "", registering);

    return registering;
  }

  /**
   * Returns the registered apps service specification.
   *
   * @param tenantId ID of the tenant that uses the service
   * @return Registered apps service specification
   * @throws NoSuchEntityException Thrown when the service is not registered for the tenant
   * @throws ApplicationException Thrown to indicate that an unexpected error happened
   */
  public AppsInfo getAppsInfo(EntityId tenantId)
      throws NoSuchEntityException, ApplicationException {
    Objects.requireNonNull(tenantId);
    return tenantAppendix.getEntry(tenantId, TenantAppendixCategory.APPS_INFO, "", AppsInfo.class);
  }

  /**
   * Deregisters apps service for a tenant.
   *
   * @param tenantId ID of the tenant
   * @throws NoSuchEntityException Thrown to indicate that the service is not registered for the
   *     tenant
   * @throws InvalidConfigurationException Thrown to indicate that the operation could not complete
   *     due to a server misconfiguration
   * @throws ApplicationException Thrown to indicate that an unexpected error happened
   */
  public void deregister(EntityId tenantId)
      throws NoSuchEntityException, InvalidConfigurationException, ApplicationException {
    Objects.requireNonNull(tenantId);

    // get reservation seconds
    String prop = sharedConfig.getProperty(PROP_APPS_RESOURCE_RESERVATION_SECONDS);
    int reservationSeconds = DEFAULT_RESOURCE_RESERVATION_SECONDS;
    if (!prop.isBlank()) {
      try {
        reservationSeconds = Integer.parseInt(prop);
      } catch (NumberFormatException e) {
        throw new InvalidConfigurationException(
            String.format(
                "Invalid value in shared property %s (=%s); operation=DeregisterAppsService, tenant=%s",
                PROP_APPS_RESOURCE_RESERVATION_SECONDS, prop, tenantId.getName()));
      }
    }

    final var appsInfo = getAppsInfo(tenantId);
    tenantAppendix.deleteEntry(tenantId, TenantAppendixCategory.APPS_INFO, "");

    resourceAllocator.releaseResource(
        ResourceType.APPS_CONTROL_PORT, appsInfo.getControlPort().toString(), reservationSeconds);
    resourceAllocator.releaseResource(
        ResourceType.APPS_WEBHOOK_PORT, appsInfo.getWebhookPort().toString(), reservationSeconds);
  }

  public CompletableFuture<Void> deploy(AppsInfo appsInfo, String keyspace) {
    var prop = sharedConfig.getProperty(PROP_APPS_DEPLOYMENT_COMMAND);
    final var command = prop.isBlank() ? DEFAULT_DEPLOYMENT_COMMAND : prop;
    final var lbHosts = sharedConfig.getProperty(PROP_APPS_LB_HOSTS).replace(" ", "");
    final var maintainerHost = sharedConfig.getProperty(PROP_APPS_MAINTAINER_HOST);
    final var biosEndpoint = sharedConfig.getProperty(PROP_APPS_BIOS_ENDPOINT);
    prop = sharedConfig.getProperty(PROP_APPS_SSH_KEYPAIR);
    final var keypair = prop.isBlank() ? DEFAULT_SSH_KEYPAIR : prop;

    final var processBuilder =
        new ProcessBuilder()
            .command(
                command,
                "--ssh-keypair",
                keypair,
                "--tenant",
                appsInfo.getTenantName(),
                "--control-port",
                appsInfo.getControlPort().toString(),
                "--webhook-port",
                appsInfo.getWebhookPort().toString(),
                "--apps-hosts",
                appsInfo.getHosts().stream().collect(Collectors.joining(",")),
                "--tenant",
                appsInfo.getTenantName(),
                "--keyspace",
                keyspace,
                "--lb-hosts",
                lbHosts,
                "--maintainer-host",
                maintainerHost,
                "--bios-endpoint",
                biosEndpoint);

    return CompletableFuture.supplyAsync(() -> executeDeployment(appsInfo, processBuilder))
        .thenAccept(
            (result) -> {
              if (result) {
                logger.info("biOS Apps services are deployed successfully; appsInfo={}", appsInfo);
              } else {
                logger.error(
                    "Error happened in biOS Apps services deployment; appsInfo={}", appsInfo);
                String nodeName = Utils.getNodeName();
                throw new CompletionException(
                    new BiosAppsDeploymentException("server=" + nodeName));
              }
            });
  }

  private boolean executeDeployment(AppsInfo appsInfo, ProcessBuilder processBuilder) {
    try {
      final var process = processBuilder.start();
      final var outReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
      final var errReader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
      final int status = process.waitFor();
      final var outBuilder = new StringBuilder();
      String line = "";
      while ((line = outReader.readLine()) != null) {
        outBuilder.append(line).append("\\n");
      }
      final var errBuilder = new StringBuilder();
      while ((line = errReader.readLine()) != null) {
        errBuilder.append(line).append("\\n");
      }
      if (status == 0) {
        logger.info(
            "Done deploying biOS Apps service; appsInfo={}, stdout='{}', stderr='{}'",
            appsInfo,
            outBuilder,
            errBuilder);
      } else {
        final var comm = processBuilder.command().stream().collect(Collectors.joining(" "));
        logger.error(
            "Failed to deploy biOS Apps service;"
                + " appsInfo={}, command='{}', status={}, stdout='{}', stderr='{}'",
            appsInfo,
            comm,
            status,
            outBuilder,
            errBuilder);
        return false;
      }
    } catch (IOException e) {
      logger.error("biOS Apps deployment failed; appsInfo={}", appsInfo, e);
      return false;
    } catch (InterruptedException e) {
      logger.error("biOS Apps deployment interrupted; appsInfo={}", appsInfo, e);
      Thread.currentThread().interrupt();
      return false;
    }
    return true;
  }
}
