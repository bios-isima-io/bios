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
package io.isima.bios.export;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.isima.bios.admin.Admin;
import io.isima.bios.admin.TenantAppendix;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.DataPublisher;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.Event;
import io.isima.bios.models.ExportDestinationConfig;
import io.isima.bios.models.ExportStatus;
import io.isima.bios.models.TenantAppendixCategory;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main export service logic that threads together all the components of the data export service sub
 * system.
 */
public class ExportServiceImpl implements ExportService, DataPublisher {
  private static final Logger logger = LoggerFactory.getLogger(ExportServiceImpl.class);

  private static final int MINUTES_BEFORE_REVALIDATING = 5;
  private static final String KEY_DELIMITER = ":";

  private final Admin admin;
  private final TenantAppendix tenantAppendix;
  private final LoadingCache<String, ExportDestinationConfig> exportConfigCache;
  private final Transformers transformers;

  public ExportServiceImpl(
      Admin admin, TenantAppendix tenantAppendix, DataEngine engine, DataExportWorker worker) {
    this.admin = admin;
    this.tenantAppendix = tenantAppendix;
    this.transformers = new Transformers(worker);
    this.exportConfigCache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(Duration.ofMinutes(MINUTES_BEFORE_REVALIDATING))
            .build(
                new CacheLoader<>() {
                  @Override
                  public ExportDestinationConfig load(String key) {
                    final var keys = fromKey(key);
                    try {
                      return fetchAndLoadExportConfig(keys[0], keys[1]);
                    } catch (ApplicationException | TfosException e) {
                      logger.warn("Unable to find config for {}:{}", keys[0], keys[1]);
                      return null;
                    }
                  }
                });
    engine.registerDataPublisher(this);
  }

  @Override
  public ExportDestinationConfig createDestination(
      String tenantName, ExportDestinationConfig config)
      throws TfosException, ApplicationException {
    if (config.getExportDestinationId() == null) {
      config.setExportDestinationId(UUID.randomUUID().toString());
    }
    if (config.getStorageType() == null) {
      throw new InvalidRequestException("Property 'storageType' is required");
    }
    if ("S3".equalsIgnoreCase(config.getStorageType())) {
      final var configValidator = new S3ExportConfig(config);
      configValidator.validateExportConfig();

      final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
      final EntityId tenantId = new EntityId(tenantConfig);

      tenantAppendix.createEntry(
          tenantId, TenantAppendixCategory.EXPORT_TARGETS, config.getExportDestinationId(), config);
      return config;
    }

    throw new InvalidRequestException("Unsupported storage type: " + config.getStorageType());
  }

  @Override
  public ExportDestinationConfig updateDestination(
      String tenantName, String exportDestinationId, ExportDestinationConfig config)
      throws TfosException, ApplicationException {

    config.setExportDestinationId(exportDestinationId);

    if (config.getStorageType() == null) {
      throw new InvalidRequestException("Property 'storageType' is required");
    }
    if ("S3".equalsIgnoreCase(config.getStorageType())) {
      final var configValidator = new S3ExportConfig(config);
      configValidator.validateExportConfig();

      final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
      final EntityId tenantId = new EntityId(tenantConfig);

      tenantAppendix.updateEntry(
          tenantId, TenantAppendixCategory.EXPORT_TARGETS, exportDestinationId, config);
      return config;
    }

    throw new InvalidRequestException("Unsupported storage type: " + config.getStorageType());
  }

  @Override
  public ExportDestinationConfig getDestination(String tenantName, String exportDestinationId)
      throws ApplicationException,
          NoSuchTenantException,
          InvalidRequestException,
          NoSuchEntityException {
    final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
    final EntityId tenantId = new EntityId(tenantConfig);
    final var entry =
        tenantAppendix.getEntry(
            tenantId,
            TenantAppendixCategory.EXPORT_TARGETS,
            exportDestinationId,
            ExportDestinationConfig.class);
    logger.debug("Entity = {} {}", entry.getStatus(), entry);
    return entry;
  }

  @Override
  public ExportDestinationConfig getDestination(
      String tenantName, Long tenantVersion, String exportDestinationId)
      throws ApplicationException, NoSuchEntityException {
    final EntityId tenantId = new EntityId(tenantName, tenantVersion);
    final var entry =
        tenantAppendix.getEntry(
            tenantId,
            TenantAppendixCategory.EXPORT_TARGETS,
            exportDestinationId,
            ExportDestinationConfig.class);
    logger.debug("Entity = {} {}", entry.getStatus(), entry);
    return entry;
  }

  @Override
  public void deleteDestination(String tenantName, String exportDestinationId)
      throws NoSuchTenantException,
          ApplicationException,
          InvalidRequestException,
          NoSuchEntityException,
          ConstraintViolationException {
    Objects.requireNonNull(tenantName);
    Objects.requireNonNull(exportDestinationId);
    final var tenantConfig = admin.getTenant(tenantName, true, true, false, List.of());

    final var dependents = new ArrayList<String>();
    tenantConfig
        .getSignals()
        .forEach(
            (signal) -> {
              if (exportDestinationId.equals(signal.getExportDestinationId())) {
                dependents.add(signal.getName());
              }
            });
    if (!dependents.isEmpty()) {
      throw new ConstraintViolationException(
          String.format(
              "There are signals using this destination; tenant=%s, exportDestination=%s, signals=%s",
              tenantConfig.getName(), exportDestinationId, dependents));
    }

    final EntityId tenantId = new EntityId(tenantConfig);
    tenantAppendix.deleteEntry(
        tenantId, TenantAppendixCategory.EXPORT_TARGETS, exportDestinationId);
  }

  @Override
  public void startService(String tenantName, String exportDestinationName)
      throws TfosException, ApplicationException {
    toggleState(tenantName, exportDestinationName, ExportStatus.ENABLED);
  }

  @Override
  public void stopService(String tenantName, String exportDestinationName)
      throws TfosException, ApplicationException {
    toggleState(tenantName, exportDestinationName, ExportStatus.DISABLED);
  }

  @Override
  public void shutdown() {
    transformers.shutdown();
    exportConfigCache.invalidateAll();
  }

  private void toggleState(String tenantName, String exportDestinationName, ExportStatus newStatus)
      throws TfosException, ApplicationException {
    final var exportConfig = getDestination(tenantName, exportDestinationName);
    if (!newStatus.equals(exportConfig.getStatus())) {
      final var update = exportConfig.duplicate();
      update.setStatus(newStatus);

      final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
      final EntityId tenantId = new EntityId(tenantConfig);
      logger.debug("Updating to {} {}", newStatus, update.toString());

      tenantAppendix.updateEntry(
          tenantId, TenantAppendixCategory.EXPORT_TARGETS, exportDestinationName, update);
    }
  }

  @Override
  public void publishRawData(StreamDesc signalDesc, List<Event> rawEvents) {
    logger.debug("Publishing {} events", rawEvents.size());
    if (signalDesc.getExportDestinationId() == null || rawEvents.isEmpty()) {
      // no export configured for this signal or no events, return quickly
      if (signalDesc.getExportDestinationId() == null) {
        logger.debug("signal {} has no export config", signalDesc.getName());
      } else {
        logger.debug(
            "signal {} with export config {} has no events",
            signalDesc.getName(),
            signalDesc.getExportDestinationId());
      }
      return;
    }
    final var key = toKey(signalDesc.getParent().getName(), signalDesc.getExportDestinationId());
    try {
      final var exportConfig = exportConfigCache.get(key);
      if (exportConfig.getStatus().equals(ExportStatus.ENABLED)) {
        logger.debug("Transforming {} events", rawEvents.size());
        transformers.transformEvents(signalDesc, rawEvents, exportConfig);
      }
    } catch (ExecutionException e) {
      // log export errors as warnings and return
      logger.warn(
          "Unable to publish raw data for export {}; {}",
          e.getCause().getMessage(),
          e.getStackTrace());
    } catch (Throwable e) {
      logger.warn(
          "Unable to publish raw data for export {}; {}",
          e.getMessage(),
          Arrays.toString(e.getStackTrace()));
    }
  }

  // TO BE IMPLEMENTED
  @Override
  public void publishRollupData(StreamDesc rollupStreamDesc, List<Event> rolledupEvents) {}

  /**
   * Periodically fetches config and optionally informs the transformer to load again, in case the
   * export config has changed.
   *
   * <p>Periodic fetching of export config must happen to ensure that any changes to the config done
   * on another node is seen on other nodes eventually.
   *
   * @param tenantName name of the tenant, in lower case
   * @param exportDestinationId name of the export config
   * @return Latest export config
   * @throws TfosException if config is not found
   * @throws ApplicationException if cassandra errors happen
   */
  private ExportDestinationConfig fetchAndLoadExportConfig(
      String tenantName, String exportDestinationId) throws TfosException, ApplicationException {
    final var tenantConfig = admin.getTenant(tenantName, false, false, false, List.of());
    final EntityId tenantId = new EntityId(tenantConfig);
    final var retConfig =
        tenantAppendix.getEntry(
            tenantId,
            TenantAppendixCategory.EXPORT_TARGETS,
            exportDestinationId,
            ExportDestinationConfig.class);
    // reload s3client if export config has changed. Other changes in the tenant should not
    // require a reload
    transformers.reLoadIfRequired(tenantName, retConfig);
    return retConfig;
  }

  private static String toKey(String tenantName, String exportDestinationId) {
    return tenantName.toLowerCase() + KEY_DELIMITER + exportDestinationId;
  }

  private static String[] fromKey(String key) {
    return key.split(KEY_DELIMITER);
  }
}
