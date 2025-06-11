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

import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.AppsInfo;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.ImportFlowConfig;
import io.isima.bios.models.ImportSourceConfig;
import io.isima.bios.models.ImportSourceType;
import io.isima.bios.models.TenantConfig;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.xmlrpc.XmlRpcException;
import org.apache.xmlrpc.client.XmlRpcClient;
import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;

/**
 * Class used for notifying BiosApps services import flow configuration changes.
 *
 * <p>Notifications are done by restarting the service actually.
 */
@Slf4j
public class AppsChangeNotifier {

  private final AppsManager appsManager;

  private final Map<String, AppsServiceRestarter> restarters;

  public AppsChangeNotifier(AppsManager appsManager) {
    this.appsManager = appsManager;
    restarters = new ConcurrentHashMap<>();
  }

  /**
   * Notifies a import flow spec change.
   *
   * <p>In order to avoid multiple notifications triggering multiple restarts within a short time,
   * the method creates a restart object which delays in invoking its restart task. If additional
   * notifications come before the invokation, they "share rides" with the first one.
   */
  public void notifyFlowChange(TenantConfig tenantConfig, ImportFlowConfig flowSpec)
      throws ApplicationException {
    final var type = resolveSourceType(tenantConfig, flowSpec);
    if (type == null) {
      // shouldn't happen. should we warn this?
      return;
    }
    final String logContext =
        String.format(
            "tenant=%s, flow=%s (%s)",
            tenantConfig.getName(), flowSpec.getImportFlowId(), flowSpec.getImportFlowName());
    notify(tenantConfig, type, logContext);
  }

  public void notifySourceChange(TenantConfig tenantConfig, ImportSourceConfig sourceConfig)
      throws ApplicationException {
    final String logContext = String.format("tenant=%s", tenantConfig.getName());
    notify(tenantConfig, sourceConfig.getType(), logContext);
  }

  private void notify(TenantConfig tenantConfig, ImportSourceType type, String logContext)
      throws ApplicationException {
    final var tenantId = new EntityId(tenantConfig);
    AppsInfo appsInfo;
    try {
      appsInfo = appsManager.getAppsInfo(tenantId);
    } catch (NoSuchEntityException e) {
      // The bios apps service is not registered for this tenant, but we'll continue and create the
      // restarter object to limit number of warning logs. The restarter terminates without sending
      // restart request in the case.
      appsInfo = null;
    }

    final var restarter = new AppsServiceRestarter(tenantConfig.getName(), type, appsInfo);
    if (restarters.putIfAbsent(restarter.getId(), restarter) == null) {
      if (appsInfo == null) {
        // we warn here to log it only once for multiple operations within a short time
        logger.warn(
            "An import flow spec has been updated but corresponding apps service is not registered,"
                + " restart it manually; {}",
            logContext);
      }
      restarter.trigger();
    }
  }

  private ImportSourceType resolveSourceType(TenantConfig tenantConfig, ImportFlowConfig flowSpec) {
    for (var importSource : tenantConfig.getImportSources()) {
      if (importSource
          .getImportSourceId()
          .equals(flowSpec.getSourceDataSpec().getImportSourceId())) {
        return importSource.getType();
      }
    }
    return null;
  }

  /** Class that restarts a bios-apps service. */
  @Getter
  private class AppsServiceRestarter {
    private static final String SUPERVISOR_STOP_ALL_PROCESSES = "supervisor.stopAllProcesses";
    private static final String SUPERVISOR_START_ALL_PROCESSES = "supervisor.startAllProcesses";

    private static final int INITIAL_DELAY = 10;
    private static final int RESTART_INTERVAL = 10;

    private final String id;
    private final ImportSourceType type;
    private final AppsInfo appsInfo;
    private final String tenantName;

    private int hostIndex;

    public AppsServiceRestarter(String tenant, ImportSourceType type, AppsInfo appsInfo) {
      Objects.requireNonNull(tenant);
      Objects.requireNonNull(type);
      id = tenant + "." + type.name();
      this.type = type;
      this.appsInfo = appsInfo;
      this.tenantName = appsInfo != null ? appsInfo.getTenantName() : null;
    }

    /** Triggers rolling restart of the Apps nodes. */
    public void trigger() {
      hostIndex = 0;
      scheduleNextRestart(INITIAL_DELAY);
    }

    private void scheduleNextRestart(int delay) {
      CompletableFuture.runAsync(
          this::restartNext, CompletableFuture.delayedExecutor(delay, TimeUnit.SECONDS));
    }

    private void restartNext() {
      // ends ride share
      restarters.remove(id);

      // we stop here if there's nothing to restart
      if (appsInfo == null || hostIndex >= appsInfo.getHosts().size()) {
        return;
      }

      final String host = appsInfo.getHosts().get(hostIndex++);

      final var client = new XmlRpcClient();
      final var config = new XmlRpcClientConfigImpl();
      final var url = String.format("http://%s:%d/RPC2", host, appsInfo.getControlPort());
      try {
        config.setServerURL(new URL(url));
      } catch (MalformedURLException e) {
        logger.error("Apps service URL {} is broken", url, e);
        return;
      }
      config.setBasicUserName(Bios2Config.getAppsXmlrpcUser());
      config.setBasicPassword(Bios2Config.getAppsXmlrpcPassword());
      client.setConfig(config);
      try {
        final Object stopResult =
            client.execute(SUPERVISOR_STOP_ALL_PROCESSES, List.of(Boolean.TRUE));
        final var stopStatuses = new ArrayList<String>();
        for (Object element : (Object[]) stopResult) {
          final var entry = (Map<String, String>) element;
          stopStatuses.add(
              String.format("{name=%s, result=%s}", entry.get("name"), entry.get("description")));
        }
        logger.info(
            "BiosServer Apps processes stopped; tenant={}, statuses={}", tenantName, stopStatuses);
        final Object startResult =
            client.execute(SUPERVISOR_START_ALL_PROCESSES, List.of(Boolean.TRUE));
        final var startStatuses = new ArrayList<String>();
        for (Object element : (Object[]) startResult) {
          final var entry = (Map<String, String>) element;
          startStatuses.add(
              String.format("{name=%s, result=%s}", entry.get("name"), entry.get("description")));
        }
        logger.info(
            "BiosServer Apps processes started; tenant={}, statuses={}, controlUrl={}",
            tenantName,
            startStatuses,
            url);
      } catch (XmlRpcException e) {
        logger.warn(
            "Reloading bios apps failed; tenant={}, controlUrl={}, error={}",
            tenantName,
            url,
            e.toString());
      }

      // Schedule next if the nodes are remaining
      if (hostIndex < appsInfo.getHosts().size()) {
        scheduleNextRestart(RESTART_INTERVAL);
      }
    }
  }
}
