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
package io.isima.bios.admin.v1.store.impl;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminStore;
import io.isima.bios.admin.v1.StreamStoreDesc;
import io.isima.bios.admin.v1.TenantStoreDesc;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.storage.cassandra.CassandraConnection;
import io.isima.bios.storage.cassandra.RetryHandler;
import io.isima.bios.utils.StringUtils;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An implementation of ControlEngine backed by Cassandra. */
public class AdminStoreImpl implements AdminStore {
  private static final Logger logger = LoggerFactory.getLogger(AdminStoreImpl.class);

  private static final String QUERY_CREATE_ADMIN_TABLE =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s.%s "
              + "(tenant text, version timestamp, stream text, config text, is_deleted boolean,"
              + "PRIMARY KEY (tenant, version, stream)) "
              + "WITH CLUSTERING ORDER BY (version ASC, stream ASC)",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN);

  private static final String QUERY_CREATE_ADMIN_TABLE_ARCHIVED =
      String.format(
          "CREATE TABLE IF NOT EXISTS %s.%s "
              + "(tenant text, version timestamp, stream text, config text, is_deleted boolean,"
              + "PRIMARY KEY (tenant, version, stream)) "
              + "WITH CLUSTERING ORDER BY (version ASC, stream ASC)",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN_ARCHIVED);

  private static final String QUERY_GET_ALL_TENANTS =
      String.format(
          "SELECT tenant, stream, version, config, is_deleted FROM %s.%s",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN);

  private static final String QUERY_GET_ALL_ROWS_FOR_DELETED_TENANT =
      String.format(
          "SELECT tenant, stream, version, config, is_deleted FROM %s.%s" + " WHERE tenant = ?",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN);

  private static final String QUERY_INSERT_ADMIN_ENTRY =
      String.format(
          "INSERT INTO %s.%s (tenant, stream, version, config, is_deleted) VALUES (?, ?, ?, ?, ?)",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN);

  private static final String QUERY_INSERT_ADMIN_ENTRY_ARCHIVED =
      String.format(
          "INSERT INTO %s.%s (tenant, stream, version, config, is_deleted) VALUES (?, ?, ?, ?, ?)",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN_ARCHIVED);

  private static final String QUERY_DELETE_TENANT =
      String.format(
          "DELETE FROM %s.%s WHERE tenant = ?",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN);

  private static final String QUERY_GET_STREAM =
      String.format(
          "SELECT config, is_deleted, version FROM %s.%s"
              + " WHERE tenant = ? AND version = ? AND STREAM = ?",
          CassandraConstants.KEYSPACE_ADMIN, CassandraConstants.TABLE_ADMIN);

  protected static final String ROOT_ENTRY_OLD = "_root_";
  protected static final String ROOT_ENTRY = ".root";

  private final ObjectMapper mapper = TfosObjectMapperProvider.get();
  private Session session;

  /**
   * The constructor.
   *
   * @param connection CassandraConnection instance
   */
  public AdminStoreImpl(CassandraConnection connection) {
    logger.debug("AdminStoreImpl initializing ###");
    this.session = connection.getSession();

    try {
      logger.debug("creating admin table: {}", QUERY_CREATE_ADMIN_TABLE);
      session.execute(QUERY_CREATE_ADMIN_TABLE);

      logger.debug("Creating admin archive table {}", QUERY_CREATE_ADMIN_TABLE_ARCHIVED);
      session.execute(QUERY_CREATE_ADMIN_TABLE_ARCHIVED);
    } catch (NoHostAvailableException
        | QueryExecutionException
        | QueryValidationException
        | UnsupportedFeatureException e) {
      logger.error("creating admin table failed", e);
      throw e;
    }

    logger.debug("### AdminStoreImpl initialized");
  }

  /**
   * Returns map of tenant name and config by querying Cassandra DB.
   *
   * @return map
   * @throws ApplicationException when unexpected error happened.
   */
  @Override
  public Map<String, Deque<TenantStoreDesc>> getTenantStoreDescMap() throws ApplicationException {
    ResultSet results = getAdminTableResults();

    // TODO(TFOS-1984): By design, the stream name for tenant should be the first in order for a
    // version. But it was not true for old root entry name "_root_" when a stream name starts with
    // a number such as "123". Because of this, we cannot rely on SELECT output order.
    // The first block of the following logic is for sorting the SELECT output in desirable order.
    // This can be simplified later when we know the ROOT_ENTRY_OLD entries do not exist anymore in
    // any of the deployed TFOS admin tables.
    final Map<String, AdminTableRow> source = new TreeMap<>();
    for (Row row : results.all()) {
      final String tenant = row.getString(0);
      final String stream = ROOT_ENTRY_OLD.equals(row.getString(1)) ? ROOT_ENTRY : row.getString(1);
      final Long version = row.getTimestamp(2).getTime();
      final String config = row.getString(3);
      final boolean isDeleted = row.getBool(4);
      logger.debug(
          "tenant={}, stream={}, version={}({}), streamConfig={}",
          tenant,
          stream,
          version,
          StringUtils.tsToIso8601(version),
          config);
      final String key = tenant + "." + version + "." + stream;
      source.put(key, new AdminTableRow(tenant, stream, version, config, isDeleted));
    }

    Map<String, Deque<TenantStoreDesc>> tenantConfigs = new ConcurrentHashMap<>();
    for (AdminTableRow entry : source.values()) {
      final String tenant = entry.tenant;
      final String stream = entry.stream;
      final Long version = entry.version;
      final String config = entry.config;
      final boolean isDeleted = entry.isDeleted;
      if (stream.equals(ROOT_ENTRY)) {
        TenantStoreDesc tenantStoreDesc;
        try {
          tenantStoreDesc = mapper.readValue(config, TenantStoreDesc.class);
          tenantStoreDesc.setDeleted(isDeleted);
        } catch (IOException e) {
          String message =
              String.format(
                  "failed to deserialize tenant config, tenant=%s config=%s", tenant, config);
          throw new ApplicationException(message, e);
        }
        tenantStoreDesc.setVersion(version);
        Deque<TenantStoreDesc> tenantList = tenantConfigs.get(tenant.toLowerCase());
        if (tenantList == null) {
          tenantList = new ConcurrentLinkedDeque<>();
          tenantConfigs.put(tenant.toLowerCase(), tenantList);
        }
        tenantList.addFirst(tenantStoreDesc);
      } else {
        StreamStoreDesc streamStoreDesc;
        try {
          streamStoreDesc = mapper.readValue(config, StreamStoreDesc.class);
          streamStoreDesc.setDeleted(isDeleted);
        } catch (IOException e) {
          String message =
              String.format(
                  "failed to deserialize stream config, tenant=%s config=%s", tenant, config);
          throw new ApplicationException(message, e);
        }
        streamStoreDesc.setVersion(version);
        Deque<TenantStoreDesc> tenantList = tenantConfigs.get(tenant.toLowerCase());
        if (tenantList == null || tenantList.peek() == null) {
          throw new ApplicationException(
              String.format(
                  "A stream entry appeared before tenant in table tfos_admin.admin;"
                      + " tenant=%s, stream=%s",
                  tenant, streamStoreDesc.getName()));
        }
        assert tenantList.peek().getVersion() <= streamStoreDesc.getVersion();
        assert tenantList.peek() != null;
        tenantList.peek().addStream(streamStoreDesc);
      }
    }
    return tenantConfigs;
  }

  public AdminMaintenanceInfo getAdminTableDataForMaintenance() throws ApplicationException {
    ResultSet results = getAdminTableResults();
    Map<String, TreeSet<TenantStoreDesc>> tenantRowsMap = new HashMap<>();
    Map<TenantStreamPair, TreeSet<StreamStoreDesc>> streamRowsMap = new HashMap<>();
    Long tenantVersion = null;
    for (Row row : results.all()) {
      final String tenant = row.getString(0);
      final String stream = row.getString(1);
      final Long version = row.getTimestamp(2).getTime();
      final String config = row.getString(3);
      final boolean isDeleted = row.getBool(4);

      if (stream.equals(ROOT_ENTRY)) {
        tenantVersion = version;
        try {
          final TenantStoreDesc tenantStoreDesc = mapper.readValue(config, TenantStoreDesc.class);
          tenantStoreDesc.setDeleted(isDeleted);
          tenantStoreDesc.setVersion(version);
          TreeSet<TenantStoreDesc> tenantRows =
              tenantRowsMap.computeIfAbsent(
                  tenant,
                  k -> new TreeSet<>((o1, o2) -> o2.getVersion() >= o1.getVersion() ? 1 : -1));
          tenantRows.add(tenantStoreDesc);
        } catch (IOException e) {
          String message =
              String.format(
                  "failed to deserialize tenant config, tenant=%s config=%s", tenant, config);
          throw new ApplicationException(message, e);
        }
      } else {
        final TenantStreamPair key = new TenantStreamPair(tenant, tenantVersion, stream);
        try {
          StreamStoreDesc streamStoreDesc = mapper.readValue(config, StreamStoreDesc.class);
          streamStoreDesc.setDeleted(isDeleted);
          streamStoreDesc.setVersion(version);
          TreeSet<StreamStoreDesc> streamRows =
              streamRowsMap.computeIfAbsent(
                  key, k -> new TreeSet<>((o1, o2) -> o2.getVersion() >= o1.getVersion() ? 1 : -1));
          streamRows.add(streamStoreDesc);
        } catch (IOException e) {
          String message =
              String.format(
                  "failed to deserialize stream config, tenant=%s stream=%s" + " config=%s",
                  tenant, stream, config);
          throw new ApplicationException(message, e);
        }
      }
    }
    return new AdminMaintenanceInfo(tenantRowsMap, streamRowsMap);
  }

  private ResultSet getAdminTableResults() throws ApplicationException {
    ResultSet results;
    RetryHandler retryHandler = new RetryHandler("load tenants and streams", logger, "");
    while (true) {
      try {
        results =
            session.execute(
                new SimpleStatement(QUERY_GET_ALL_TENANTS)
                    .setConsistencyLevel(ConsistencyLevel.ALL));
        break;
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }
    return results;
  }

  /** Stores the tenant config as well as non-deleted streams that belong to this tenant. */
  @Override
  public void storeTenant(TenantStoreDesc tenantStoreDesc) throws ApplicationException {
    storeTenantCore(tenantStoreDesc, ROOT_ENTRY);
  }

  /** Stores only the tenant config and not the streams that belong to the tenant. */
  @Override
  public void storeTenantOnly(TenantStoreDesc tenantStoreDesc) throws ApplicationException {
    storeTenantCore(tenantStoreDesc, ROOT_ENTRY, true);
  }

  protected void storeTenantCore(TenantStoreDesc tenantConfig, String rootStreamName)
      throws ApplicationException {
    storeTenantCore(tenantConfig, rootStreamName, false);
  }

  protected void storeTenantCore(
      TenantStoreDesc tenantStoreDesc, String rootStreamName, boolean skipStoringStreams)
      throws ApplicationException {
    if (tenantStoreDesc == null) {
      throw new IllegalArgumentException("tenantConfig may not be null");
    }
    String tenantName = tenantStoreDesc.getName();
    try {
      TenantStoreDesc clone = tenantStoreDesc.duplicate();
      clone.setStreams(null);
      Long version = clone.getVersion();
      if (version == null) {
        throw new IllegalArgumentException(
            String.format(
                "TenantConfig version must be set when storing to DB, tenant=%s",
                tenantStoreDesc.getName()));
      }
      String config = mapper.writeValueAsString(clone);
      session.execute(
          QUERY_INSERT_ADMIN_ENTRY,
          tenantName.toLowerCase(),
          rootStreamName,
          version,
          config,
          clone.isDeleted());
      logger.info(
          "Writing tenant to store. tenant={}, version={}, deleted={}, "
              + "maxAllocatedStreamNameProxy={}, skipStoringStreams={}",
          tenantStoreDesc.getName(),
          tenantStoreDesc.getVersion(),
          tenantStoreDesc.getDeleted(),
          tenantStoreDesc.getMaxAllocatedStreamNameProxy(),
          skipStoringStreams);
      if (!skipStoringStreams) {
        for (StreamStoreDesc streamStoreDesc : tenantStoreDesc.getStreams()) {
          storeStream(tenantName, streamStoreDesc);
        }
      }
    } catch (JsonProcessingException e) {
      throw new ApplicationException("failed to stringify config", e);
    } catch (NoHostAvailableException
        | QueryExecutionException
        | QueryValidationException
        | UnsupportedFeatureException e) {
      throw new ApplicationException("storeTenant failed", e);
    }
  }

  @Override
  public void deleteTenant(String tenant) throws ApplicationException {
    deleteTenantFromDb(session, tenant);
  }

  public static void deleteTenantFromDb(Session session, String tenant)
      throws ApplicationException {
    try {
      copyRowsToAdminArchiveTable(session, tenant);
      session.execute(QUERY_DELETE_TENANT, tenant.toLowerCase());
    } catch (NoHostAvailableException
        | QueryExecutionException
        | QueryValidationException
        | UnsupportedFeatureException e) {
      throw new ApplicationException("storeTenant failed", e);
    }
  }

  private static void copyRowsToAdminArchiveTable(Session session, String tenant)
      throws ApplicationException {
    ResultSet archivedRows;
    RetryHandler retryHandler = new RetryHandler("load tenants and streams", logger, "");
    while (true) {
      try {
        archivedRows =
            session.execute(
                new SimpleStatement(QUERY_GET_ALL_ROWS_FOR_DELETED_TENANT, tenant)
                    .setConsistencyLevel(ConsistencyLevel.ALL));
        break;
      } catch (DriverException e) {
        retryHandler.handleError(e);
      }
    }

    List<AdminTableRow> adminTableRows = new ArrayList<>();
    while (!archivedRows.isExhausted()) {
      Row row = archivedRows.one();
      final String tenantName = row.getString(0);
      final String stream = ROOT_ENTRY_OLD.equals(row.getString(1)) ? ROOT_ENTRY : row.getString(1);
      final Long version = row.getTimestamp(2).getTime();
      final String config = row.getString(3);
      final boolean isDeleted = true;
      adminTableRows.add(new AdminTableRow(tenantName, stream, version, config, isDeleted));
    }

    for (AdminTableRow adminTableRow : adminTableRows) {
      session.execute(
          QUERY_INSERT_ADMIN_ENTRY_ARCHIVED,
          adminTableRow.tenant,
          adminTableRow.stream,
          adminTableRow.version,
          adminTableRow.config,
          adminTableRow.isDeleted);
    }
  }

  @Override
  public void storeStream(String tenant, StreamStoreDesc streamStoreDesc)
      throws ApplicationException {
    if (tenant == null) {
      throw new IllegalArgumentException("tenant name may not be null");
    }
    if (streamStoreDesc == null) {
      throw new IllegalArgumentException("streamConfig may not be null");
    }
    final Long version = streamStoreDesc.getVersion();
    if (version == null) {
      throw new IllegalArgumentException("streamConfig version may not be null");
    }
    // TODO(TFOS-1080): This stream type check is hack to avoid internal stream being stored
    final StreamType type = streamStoreDesc.getType();
    if (type != StreamType.SIGNAL && type != StreamType.CONTEXT && type != StreamType.METRICS) {
      return;
    }
    try {
      final StreamStoreDesc clone = streamStoreDesc.duplicate();
      clone.setVersion(null);
      // final constraint check -- this should not happen
      for (AttributeDesc desc : streamStoreDesc.getAttributes()) {
        if (desc.getAttributeType() == InternalAttributeType.ENUM) {
          final List<String> enumList = desc.getEnum();
          final Object defaultValue = desc.getDefaultValue();
          if (enumList == null || (defaultValue != null && !enumList.contains(defaultValue))) {
            throw new ApplicationException("enum default must match one of the entries: " + clone);
          }
        }
      }
      final String config = mapper.writeValueAsString(clone);
      logger.debug("Storing stream config: {}", config);
      RetryHandler retryHandler = new RetryHandler("store stream", logger, "");
      while (true) {
        try {
          session.execute(
              QUERY_INSERT_ADMIN_ENTRY,
              tenant.toLowerCase(),
              clone.getName().toLowerCase(),
              version,
              config,
              clone.isDeleted());
          break;
        } catch (DriverException e) {
          retryHandler.handleError(e);
        }
      }
    } catch (JsonProcessingException e) {
      String message =
          String.format(
              "failed to stringify StreamConfig, tenant=%s conf=%s",
              tenant, streamStoreDesc.toString());
      throw new ApplicationException(message, e);
    }
  }

  @Override
  public TenantConfig getTenant(String tenantName, Long timestamp) throws ApplicationException {
    ResultSet results;
    try {
      results = session.execute(QUERY_GET_STREAM, tenantName.toLowerCase(), timestamp, ROOT_ENTRY);
    } catch (NoHostAvailableException
        | QueryExecutionException
        | QueryValidationException
        | UnsupportedFeatureException e) {
      throw new ApplicationException("getTenantConfig failed", e);
    }
    while (!results.isExhausted()) {
      Row row = results.one();
      String config = row.getString(0);
      boolean isDeleted = row.getBool(1);
      Date version = row.getTimestamp(2);
      try {
        TenantConfig tenantConfig = mapper.readValue(config, TenantConfig.class);
        tenantConfig.setDeleted(isDeleted);
        tenantConfig.setVersion(version.getTime());
        return tenantConfig;
      } catch (IOException e) {
        String message =
            String.format(
                "failed to deserialize tenant config, tenant=%s config=%s", tenantName, config);
        throw new ApplicationException(message, e);
      }
    }
    return null;
  }

  @Override
  public StreamStoreDesc getStream(String tenantName, String streamName, Long timestamp)
      throws ApplicationException {
    ResultSet results;
    try {
      results =
          session.execute(
              QUERY_GET_STREAM, tenantName.toLowerCase(), timestamp, streamName.toLowerCase());
    } catch (NoHostAvailableException
        | QueryExecutionException
        | QueryValidationException
        | UnsupportedFeatureException e) {
      throw new ApplicationException("getStream failed", e);
    }
    while (!results.isExhausted()) {
      Row row = results.one();
      String config = row.getString(0);
      boolean isDeleted = row.getBool(1);
      Date version = row.getTimestamp(2);
      try {
        final StreamStoreDesc streamConfig = mapper.readValue(config, StreamStoreDesc.class);
        streamConfig.setDeleted(isDeleted);
        streamConfig.setVersion(version.getTime());
        return streamConfig;
      } catch (IOException e) {
        String message =
            String.format(
                "failed to deserialize stream config, tenant=%s config=%s", tenantName, config);
        throw new ApplicationException(message, e);
      }
    }
    return null;
  }
}
