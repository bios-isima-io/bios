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
package io.isima.bios.data.impl.maintenance;

import static io.isima.bios.common.BiosConstants.TENANT_SYSTEM;
import static io.isima.bios.data.impl.DataEngineImpl.PROP_MAX_KEYSPACE_DELETIONS_PER_MAINTENANCE;
import static io.isima.bios.data.impl.DataEngineImpl.PROP_MAX_TABLE_DELETIONS_PER_MAINTENANCE;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.PREFIX_CONTEXT_TABLE;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.PREFIX_CONTEXT_TABLE_OLD;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import io.isima.bios.admin.v1.StreamStoreDesc;
import io.isima.bios.admin.v1.TenantStoreDesc;
import io.isima.bios.admin.v1.store.impl.AdminMaintenanceInfo;
import io.isima.bios.admin.v1.store.impl.AdminStoreImpl;
import io.isima.bios.admin.v1.store.impl.TenantStreamPair;
import io.isima.bios.audit.AuditOperation;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.StreamId;
import io.isima.bios.data.impl.models.MaintenanceMode;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.ContextCassStream;
import io.isima.bios.data.impl.storage.TableInfo;
import io.isima.bios.data.storage.cassandra.CassandraDataStoreUtils;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AppType;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Rollup;
import io.isima.bios.models.TenantInfo;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.models.v1.PostprocessDesc;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.utils.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableMaintainer {
  private static final Logger logger = LoggerFactory.getLogger(TableMaintainer.class);

  private static final long NUM_MILLISECONDS_ONE_MINUTE = 60000;

  private final CassandraConnection cassandraConnection;
  private final Session session;

  public TableMaintainer(CassandraConnection cassandraConnection) {
    this.cassandraConnection = cassandraConnection;
    this.session = cassandraConnection.getSession();
  }

  public void dropStaleTablesAndKeyspaces(
      MaintenanceMode tableMaintenanceMode, DataEngineMaintenance dataEngineMaintenance) {
    if (tableMaintenanceMode == MaintenanceMode.DISABLED) {
      return;
    }

    try {
      final var adminMaintenanceInfo = retrieveAdminMaintenanceInfo();
      Map<String, TenantInfo> keyspaceToTenantInfo = findKeyspaces(adminMaintenanceInfo);
      keyspaceToTenantInfo.forEach(
          (keyspace, tenantInfo) -> {
            if (tenantInfo.getTenantName() == null) {
              logger.warn("Maintenance: Orphan tenant keyspace: {}", keyspace);
            }
          });

      Map<String, TableInfo> tablesToDrop =
          findTablesToDrop(
              adminMaintenanceInfo,
              keyspaceToTenantInfo,
              new AtomicInteger(0),
              new AtomicInteger(0),
              new AtomicInteger(0));

      if (!tablesToDrop.isEmpty() || !keyspaceToTenantInfo.isEmpty()) {
        // HACK: Used as a mutable integer object
        final var numKeyspacesToDrop =
            new AtomicInteger(
                SharedProperties.getInteger(PROP_MAX_KEYSPACE_DELETIONS_PER_MAINTENANCE, 0));
        final var allowedNumTablesToDrop =
            new AtomicInteger(
                SharedProperties.getInteger(PROP_MAX_TABLE_DELETIONS_PER_MAINTENANCE, 1));
        dropObsoleteTables(
            tablesToDrop, allowedNumTablesToDrop, tableMaintenanceMode, dataEngineMaintenance);
        dropObsoleteKeyspaces(
            keyspaceToTenantInfo,
            numKeyspacesToDrop,
            allowedNumTablesToDrop,
            tableMaintenanceMode,
            dataEngineMaintenance);
      }
    } catch (ApplicationException e) {
      logger.warn("Got exception {} during table maintenance", e.toString());
    }
  }

  public AdminMaintenanceInfo retrieveAdminMaintenanceInfo() throws ApplicationException {
    final var adminStoreImpl = (AdminStoreImpl) BiosModules.getAdminStore();
    return adminStoreImpl.getAdminTableDataForMaintenance();
  }

  public Map<String, TenantInfo> findKeyspaces(AdminMaintenanceInfo adminMaintenanceInfo)
      throws ApplicationException {
    final var keyspaces = new HashMap<String, TenantInfo>();

    final var existingKeyspaces = getExistingKeyspaces();

    Map<String, TreeSet<TenantStoreDesc>> tenantRowMap = adminMaintenanceInfo.getTenantRowsMap();
    tenantRowMap.forEach(
        (tenantName, tenantRowsSorted) -> {
          boolean isTenantActive = true;
          Long deletionTime = null;
          for (var tenantStoreDesc : tenantRowsSorted) {
            assert tenantStoreDesc != null;
            final var version = tenantStoreDesc.getVersion();
            final var keyspace = CassandraDataStoreUtils.generateKeyspaceName(tenantName, version);
            if (tenantStoreDesc.isDeleted()) {
              isTenantActive = false;
              deletionTime = version;
            }
            boolean keyspaceExists = existingKeyspaces.remove(keyspace);
            final var tenantInfo =
                new TenantInfo(
                    keyspace, tenantName, version, isTenantActive, deletionTime, keyspaceExists);
            keyspaces.put(keyspace, tenantInfo);
            // second and later tenants are all inactive
            isTenantActive = false;
          }
        });

    // add orphans
    for (var keyspace : existingKeyspaces) {
      keyspaces.put(keyspace, new TenantInfo(keyspace));
    }

    return keyspaces;
  }

  public Map<String, TableInfo> findTablesToDrop(
      AdminMaintenanceInfo adminMaintenanceInfo,
      Map<String, TenantInfo> keyspaces,
      AtomicInteger activeTablesCount,
      AtomicInteger inactiveTablesCount,
      AtomicInteger removedTablesCount,
      String... targetKeyspaces) {
    final var keyspacesToHandle = Set.of(targetKeyspaces);

    final var tablesToRemove = new HashMap<String, TableInfo>();
    final var handledKeyspaces = new HashSet<String>();

    final Map<String, Map<String, TableMetadata>> keyspaceToTable = new TreeMap<>();
    session.getCluster().getMetadata().getKeyspaces().stream()
        .filter((keyspaceMetadata) -> keyspaceMetadata.getName().startsWith("tfos_d_"))
        .forEach(
            (keyspaceMetadata) -> {
              final var tables = new TreeMap<String, TableMetadata>();
              keyspaceMetadata.getTables().stream()
                  .filter((tableMetadata) -> !tableMetadata.getName().startsWith("sketch_"))
                  .forEach((tableMetadata) -> tables.put(tableMetadata.getName(), tableMetadata));
              keyspaceToTable.put(keyspaceMetadata.getName(), tables);
            });

    final var alreadyHandled = new HashSet<StreamId>();
    final var streamRowMap = adminMaintenanceInfo.getStreamRowsMap();
    streamRowMap.forEach(
        (tenantStreamPair, streamRowsSorted) -> {
          final var tenantName = tenantStreamPair.getTenant();
          final var tenantVersion = tenantStreamPair.getTenantVersion();
          String keyspace = CassandraDataStoreUtils.generateKeyspaceName(tenantName, tenantVersion);
          if (!keyspacesToHandle.isEmpty() && !keyspacesToHandle.contains(keyspace)) {
            return;
          }
          handledKeyspaces.add(keyspace);
          final var tenantInfo = keyspaces.get(keyspace);
          if (tenantInfo == null
              || (tenantInfo.getIsTenantActive() && !tenantInfo.getIsExisting())) {
            logger.warn(
                "Keyspace not found for stream maintenance: {}; tenant={}, stream={}",
                tenantName,
                tenantStreamPair.getStream());
            return;
          }
          final boolean isDeletedTenant = !tenantInfo.getIsTenantActive();
          Set<StreamId> references = new HashSet<>();
          Map<StreamId, StreamStoreDesc> activeStreams = new HashMap<>();
          Map<StreamId, StreamStoreDesc> inactiveStreams = new HashMap<>();
          boolean isLatest = true;
          for (var streamStoreDesc : streamRowsSorted) {
            assert streamStoreDesc != null;
            evaluateStream(
                tenantStreamPair,
                streamStoreDesc,
                isLatest,
                references,
                activeStreams,
                inactiveStreams);
            isLatest = false;
          }
          activeTablesCount.set(activeStreams.size());
          final var tables = keyspaceToTable.get(keyspace);
          for (var stream : activeStreams.keySet()) {
            final var tableName =
                CassStream.generateTableName(
                    stream.getType(), stream.getStream(), stream.getVersion());
            if (tables != null && tables.remove(tableName) == null && !isDeletedTenant) {
              logger.warn(
                  "table to keep {} not found, stream={}, alreadyHandled={}",
                  tableName,
                  stream,
                  alreadyHandled.contains(stream));
            }
            alreadyHandled.add(stream);
            if (tableName.startsWith(PREFIX_CONTEXT_TABLE)) {
              final var obsoleteTableName =
                  tableName.replace(PREFIX_CONTEXT_TABLE, PREFIX_CONTEXT_TABLE_OLD);
              addTableToRemove(
                  keyspace,
                  obsoleteTableName,
                  tenantInfo,
                  stream,
                  inactiveStreams,
                  tables,
                  tablesToRemove,
                  inactiveTablesCount,
                  removedTablesCount,
                  alreadyHandled);
            }
          }
          for (var stream : inactiveStreams.keySet()) {
            final var tableName =
                CassStream.generateTableName(
                    stream.getType(), stream.getStream(), stream.getVersion());
            addTableToRemove(
                keyspace,
                tableName,
                tenantInfo,
                stream,
                inactiveStreams,
                tables,
                tablesToRemove,
                inactiveTablesCount,
                removedTablesCount,
                alreadyHandled);
          }
        });

    keyspaceToTable.forEach(
        (keyspace, tables) -> {
          if (handledKeyspaces.contains(keyspace) && !tables.isEmpty()) {
            logger.warn("Unhandled tables in keyspace {}:", keyspace);
            tables.forEach(
                (tableName, tableMetadata) ->
                    logger.warn("  {}: {}", tableName, tableMetadata.getOptions().getComment()));
          }
        });
    return tablesToRemove;
  }

  final void addTableToRemove(
      String keyspace,
      String tableName,
      TenantInfo tenantInfo,
      StreamId stream,
      Map<StreamId, StreamStoreDesc> inactiveStreams,
      Map<String, TableMetadata> tables,
      Map<String, TableInfo> tablesToRemove,
      AtomicInteger inactiveTablesCount,
      AtomicInteger removedTablesCount,
      Set<StreamId> alreadyHandled) {
    if (tables != null && tables.remove(tableName) != null) {
      inactiveTablesCount.incrementAndGet();
      final var tableInfo = new TableInfo(keyspace, tableName, tenantInfo, stream);
      final var parent = inactiveStreams.get(stream);
      if (parent != null) {
        tableInfo.setParentStream(parent.getName());
        tableInfo.setParentStreamVersion(parent.getVersion());
      }
      tablesToRemove.put(keyspace + "." + tableName, tableInfo);
    } else {
      removedTablesCount.incrementAndGet();
    }
    alreadyHandled.add(stream);
  }

  private void evaluateStream(
      TenantStreamPair tenantStreamPair,
      StreamStoreDesc streamStoreDesc,
      boolean isLatest,
      Set<StreamId> references,
      Map<StreamId, StreamStoreDesc> activeStreamsAndParents,
      Map<StreamId, StreamStoreDesc> inactiveStreamsAndParents) {
    final var tenantName = tenantStreamPair.getTenant();
    final var streamName = streamStoreDesc.getName();
    if (DataEngineMaintenance.SKIP_LIST.contains(streamName.toLowerCase())) {
      return;
    }
    final var streamVersion = streamStoreDesc.getVersion();
    final var schemaVersion =
        streamStoreDesc.getSchemaVersion() != null
            ? streamStoreDesc.getSchemaVersion()
            : streamVersion;
    final var streamId = new StreamId(tenantName, streamName, streamVersion);
    final var schemaId =
        new StreamId(tenantName, streamName, schemaVersion, streamStoreDesc.getType());
    boolean isActive = (isLatest || references.contains(streamId)) && !streamStoreDesc.isDeleted();
    if (!isActive
        && !activeStreamsAndParents.containsKey(schemaId)
        && isOldEnoughToBeDeleted(streamVersion)) {
      inactiveStreamsAndParents.put(schemaId, null);
    } else {
      if (streamStoreDesc.getPrevName() != null && streamStoreDesc.getType() == StreamType.SIGNAL) {
        StreamId prevStreamId =
            new StreamId(
                tenantStreamPair.getTenant(),
                streamStoreDesc.getPrevName(),
                streamStoreDesc.getPrevVersion(),
                streamStoreDesc.getType());
        references.add(prevStreamId);
      }
      activeStreamsAndParents.put(schemaId, null);
    }

    List<ViewDesc> viewDescList = streamStoreDesc.getViews();
    if (viewDescList != null) {
      viewDescList.forEach(
          viewDesc -> {
            if (viewDesc.getIndexTableEnabled() != Boolean.TRUE) {
              return;
            }
            String viewQualifiedName =
                CassStream.generateQualifiedNameSubstream(
                    StreamType.VIEW, tenantStreamPair.getStream(), viewDesc.getName());
            String indexQualifiedName =
                CassStream.generateQualifiedNameSubstream(
                    StreamType.INDEX, tenantStreamPair.getStream(), viewDesc.getName());

            Long version = viewDesc.getSchemaVersion();
            if (version == null) {
              version = streamStoreDesc.getVersion();
            }
            final var streams =
                (isLatest && isActive) || !isOldEnoughToBeDeleted(version)
                    ? activeStreamsAndParents
                    : inactiveStreamsAndParents;
            var viewId =
                new StreamId(
                    tenantStreamPair.getTenant(), viewQualifiedName, version, StreamType.VIEW);
            var indexId =
                new StreamId(
                    tenantStreamPair.getTenant(), indexQualifiedName, version, StreamType.INDEX);
            if (!activeStreamsAndParents.containsKey(viewId)) {
              streams.put(viewId, streamStoreDesc);
            }
            if (!activeStreamsAndParents.containsKey(indexId)) {
              streams.put(indexId, streamStoreDesc);
            }
          });
    }

    // Add rollup substreams
    List<PostprocessDesc> postprocessDescList = streamStoreDesc.getPostprocesses();
    if (postprocessDescList != null) {
      postprocessDescList.forEach(
          postprocessDesc -> {
            List<Rollup> rollupList = postprocessDesc.getRollups();
            final var viewName = postprocessDesc.getView();
            final var view = streamStoreDesc.getView(viewName);
            if (view != null
                && view.getDataSketches() != null
                && view.getDataSketches().contains(DataSketchType.LAST_N)) {
              return;
            }
            if (rollupList != null) {
              rollupList.forEach(
                  rollup -> {
                    Long version = rollup.getSchemaVersion();
                    if (version == null) {
                      version = streamStoreDesc.getVersion();
                    }
                    final var streams =
                        (isLatest && isActive) || !isOldEnoughToBeDeleted(version)
                            ? activeStreamsAndParents
                            : inactiveStreamsAndParents;
                    final var rollupId =
                        new StreamId(
                            tenantStreamPair.getTenant(),
                            rollup.getName(),
                            version,
                            StreamType.ROLLUP);
                    // logger.debug("rollup={}", rollupId);
                    if (!activeStreamsAndParents.containsKey(rollupId)) {
                      streams.put(rollupId, streamStoreDesc);
                    }
                  });
            }
          });
    }

    if (streamStoreDesc.getType() == StreamType.CONTEXT) {
      // Add context index substreams
      List<FeatureDesc> featureConfigList = streamStoreDesc.getFeatures();
      if (featureConfigList != null) {
        featureConfigList.forEach(
            feature -> {
              Long version = feature.getBiosVersion();
              if (version == null) {
                version = streamStoreDesc.getVersion();
              }
              final boolean toKeep = (isLatest && isActive) || !isOldEnoughToBeDeleted(version);
              final var streams = toKeep ? activeStreamsAndParents : inactiveStreamsAndParents;

              if (feature.getIndexed() == Boolean.TRUE) {
                String indexStreamName =
                    CassStream.generateQualifiedNameSubstream(
                        StreamType.CONTEXT_INDEX, tenantStreamPair.getStream(), feature.getName());
                final var indexId =
                    new StreamId(
                        tenantStreamPair.getTenant(),
                        indexStreamName,
                        version,
                        StreamType.CONTEXT_INDEX);
                streams.put(indexId, streamStoreDesc);
              }

              String featureStreamName =
                  CassStream.generateQualifiedNameSubstream(
                      StreamType.CONTEXT_FEATURE, tenantStreamPair.getStream(), feature.getName());
              final var featureId =
                  new StreamId(
                      tenantStreamPair.getTenant(),
                      featureStreamName,
                      version,
                      StreamType.CONTEXT_FEATURE);
              streams.put(featureId, streamStoreDesc);
            });
      }
    }
  }

  public Set<String> getExistingKeyspaces() throws ApplicationException {
    final var existingKeyspaces = new TreeSet<String>();
    final String statement = "SELECT keyspace_name from system_schema.keyspaces";
    final var resultSet = cassandraConnection.execute("Fetch keyspace names", logger, statement);
    while (!resultSet.isExhausted()) {
      final var row = resultSet.one();
      final var keyspace = row.getString("keyspace_name");
      if (keyspace.startsWith("tfos_d_")) {
        existingKeyspaces.add(keyspace);
      }
    }
    return existingKeyspaces;
  }

  /*
   * Determines if the version is older than 10 minutes and is safe for deletion from DB
   */
  private boolean isOldEnoughToBeDeleted(long version) {
    final long currentTime = System.currentTimeMillis();
    final long timeDifference = currentTime - version;
    final long threshold =
        TfosConfig.getMaintenanceGraceTimeInMinutesForDatabaseCleanup()
            * NUM_MILLISECONDS_ONE_MINUTE;
    return timeDifference > threshold;
  }

  private void dropObsoleteKeyspaces(
      Map<String, TenantInfo> keyspaceToTenant,
      AtomicInteger allowedNumKeyspacesToDrop,
      AtomicInteger allowedNumTablesToDrop,
      MaintenanceMode tableMaintenanceMode,
      DataEngineMaintenance dataEngineMaintenance) {
    logger.debug("Cleaning up keyspaces for old tenants");
    final boolean isDryRun = tableMaintenanceMode == MaintenanceMode.DRY_RUN;
    int dropCount = 0;
    int skipCount = 0;
    for (var entry : keyspaceToTenant.entrySet()) {
      if (dataEngineMaintenance.isShutdown()) {
        break;
      }
      final String keyspaceName = entry.getKey();
      final TenantInfo tenantInfo = entry.getValue();
      final String tenantName = tenantInfo.getTenantName();
      final Long version = tenantInfo.getTenantVersion();
      final boolean isActive = tenantInfo.getIsTenantActive() == Boolean.TRUE;
      Long deletionTime = tenantInfo.getDeletionTime();
      if (!isActive && deletionTime == null) {
        deletionTime = version;
      }
      if (version == null
          || (isActive || !isOldEnoughToBeDeleted(deletionTime))
          || !tenantInfo.getIsExisting()) {
        continue;
      }
      logger.debug(
          "deleting tenant; tenant={}, keyspace={}, version={} ({}), deletion={} ({})",
          tenantName,
          keyspaceName,
          version,
          StringUtils.tsToIso8601Millis(version),
          deletionTime,
          deletionTime != null ? StringUtils.tsToIso8601Millis(deletionTime) : "");
      final var keyspaceMetadata = session.getCluster().getMetadata().getKeyspace(keyspaceName);
      if (keyspaceMetadata == null) {
        logger.warn(
            "Maintenance: keyspace not found: {}, tenant={}, version={}",
            keyspaceName,
            tenantName,
            version);
        continue;
      }
      final var userContext =
          new UserContext(
              0L,
              0L,
              "systemadmin",
              TENANT_SYSTEM,
              List.of(Permission.SUPERADMIN.getId()),
              "",
              AppType.BATCH);
      try {
        final var tablesMetadata = keyspaceMetadata.getTables();

        final var remainingTables =
            tablesMetadata.stream()
                .map((tableMetadata) -> tableMetadata.getName())
                .collect(Collectors.toList());
        if (!remainingTables.isEmpty()) {
          for (var tableName : remainingTables) {
            if (allowedNumTablesToDrop.get() > 0 && !isDryRun) {
              final var auditManager = BiosModules.getAuditManager();
              logger.info(
                  "Dropping table {}.{}; tenant={}, version={}",
                  keyspaceName,
                  tableName,
                  tenantName,
                  version);
              final var auditInfo =
                  auditManager.begin(
                      tenantName, "", AuditOperation.DROP_TABLE, keyspaceName + "." + tableName);
              auditInfo.setUserContext(userContext);
              try {
                cassandraConnection.dropTable(keyspaceName, tableName);
                auditManager.logSuccess(auditInfo, "");
              } catch (Throwable t) {
                auditManager.logFailure(auditInfo, t);
              }
              allowedNumTablesToDrop.decrementAndGet();
            } else {
              logger.debug(
                  "Dry run: (not) Dropping table {}.{}; tenant={}, version={}",
                  keyspaceName,
                  tableName,
                  tenantName,
                  version);
            }
          }
          // we'll drop the keyspace in the next maintenance cycle
          continue;
        }

        if (cassandraConnection.verifyKeyspace(keyspaceName)) {
          if (allowedNumKeyspacesToDrop.get() > 0) {
            logger.info(
                "{}Dropping keyspace {}; tenant={}, version={}",
                isDryRun ? "(dry-run) " : "",
                keyspaceName,
                tenantName,
                version);
            ++dropCount;
            if (!isDryRun) {
              cassandraConnection.dropKeyspace(keyspaceName);
              if (cassandraConnection.verifyKeyspace(keyspaceName)) {
                logger.warn("Dropped keyspace {} still exists", keyspaceName);
              }
            }
            allowedNumKeyspacesToDrop.decrementAndGet();
          } else {
            logger.debug(
                "Skipping to drop keyspace {}; tenant={}, version={}",
                keyspaceName,
                tenantName,
                version);
            ++skipCount;
          }
        }
      } catch (Throwable e) {
        logger.info("Keyspace {} deletion failed with exception {}", keyspaceName, e);
      }
    }

    if (dropCount > 0 || skipCount > 0) {
      logger.info("{}Keyspaces dropped: {}", isDryRun ? "(dry-run) " : "", dropCount);
      if (skipCount > 0) {
        logger.info("          skipped: {}", skipCount);
        logger.info(
            "Set/modify {} to change the number of keyspaces to drop per maintenance",
            PROP_MAX_KEYSPACE_DELETIONS_PER_MAINTENANCE);
      }
    }
  }

  private void dropObsoleteTables(
      Map<String, TableInfo> tablesToDrop,
      AtomicInteger allowedNumTablesToDrop,
      MaintenanceMode tableMaintenanceMode,
      DataEngineMaintenance dataEngineMaintenance) {
    logger.debug("Cleaning up stale streams");
    final var keyspaceToTables = new TreeMap<String, List<TableInfo>>();
    for (var entry : tablesToDrop.entrySet()) {
      final TableInfo tableInfo = entry.getValue();
      final String keyspaceName = tableInfo.getKeyspace();
      keyspaceToTables.putIfAbsent(keyspaceName, new ArrayList<>());
      keyspaceToTables.get(keyspaceName).add(tableInfo);
    }

    final var userContext =
        new UserContext(
            0L,
            0L,
            "systemadmin",
            TENANT_SYSTEM,
            List.of(Permission.SUPERADMIN.getId()),
            "",
            AppType.BATCH);

    int dropCount = 0;
    int skipCount = 0;
    final boolean isDryRun = tableMaintenanceMode == MaintenanceMode.DRY_RUN;
    for (var entry : keyspaceToTables.entrySet()) {
      if (dataEngineMaintenance.isShutdown()) {
        break;
      }
      final String keyspaceName = entry.getKey();
      try {
        for (TableInfo tableInfo : entry.getValue()) {
          final String tableQualifiedName = keyspaceName + "." + tableInfo.getTableName();
          final StreamId streamId = tableInfo.getStreamId();
          if (cassandraConnection.verifyQualifiedTable(tableQualifiedName)) {
            if (allowedNumTablesToDrop.get() > 0) {
              logger.info(
                  "{}Dropping table {}; tenant={}({}) stream={}({})",
                  isDryRun ? "(dry-run) " : "",
                  tableQualifiedName,
                  tableInfo.getTenantInfo().getTenantName(),
                  tableInfo.getTenantInfo().getTenantVersion(),
                  streamId.getStream(),
                  streamId.getVersion());
              ++dropCount;
              final var auditManager = BiosModules.getAuditManager();
              final var auditInfo =
                  auditManager.begin(
                      streamId.getTenant(),
                      streamId.getStream() + "." + streamId.getVersion(),
                      AuditOperation.DROP_TABLE,
                      keyspaceName + "." + tableInfo.getTableName());
              auditInfo.setUserContext(userContext);
              if (!isDryRun) {
                try {
                  cassandraConnection.dropTable(keyspaceName, tableInfo.getTableName());
                  auditManager.logSuccess(auditInfo, "");
                  if (streamId.getType() == StreamType.CONTEXT) {
                    BiosModules.getSharedProperties()
                        .deleteProperty(
                            ContextCassStream.PROPERTY_LAST_TIMESTAMP_PREFIX + tableQualifiedName);
                    BiosModules.getSharedProperties()
                        .deleteProperty(
                            ContextCassStream.PROPERTY_NEXT_TOKEN_PREFIX + tableQualifiedName);
                  }
                } catch (Throwable t) {
                  auditManager.logFailure(auditInfo, t);
                  logger.error(
                      "Failed to drop table {}; stream={}", tableQualifiedName, streamId, t);
                  throw t;
                }
                if (cassandraConnection.verifyQualifiedTable(tableQualifiedName)) {
                  logger.warn(
                      "Deleted table {} still exists; stream={}", tableQualifiedName, streamId);
                }
              } else {
                auditManager.logSuccess(auditInfo, "dry-run");
              }
              allowedNumTablesToDrop.decrementAndGet();
            } else {
              logger.debug(
                  "Skipping to drop table {}; tenant={}({}) stream={}({})",
                  tableQualifiedName,
                  tableInfo.getTenantInfo().getTenantName(),
                  tableInfo.getTenantInfo().getTenantVersion(),
                  streamId.getStream(),
                  streamId.getVersion());
              ++skipCount;
            }
          }
        }
      } catch (Throwable e) {
        logger.error("Failed to remove table", e);
      }
    }
    if (dropCount > 0 || skipCount > 0) {
      logger.info("Tables dropped{}: {}", isDryRun ? " (dry-run)" : "", dropCount);
      if (skipCount > 0) {
        logger.info("       skipped: {}", skipCount);
        logger.info(
            "Set/modify {} to change the number of tables to drop per maintenance",
            PROP_MAX_TABLE_DELETIONS_PER_MAINTENANCE);
      }
    }
  }
}
