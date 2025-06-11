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
package io.isima.bios.admin;

import static io.isima.bios.data.storage.cassandra.CassandraConstants.KEYSPACE_ADMIN;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.TABLE_APPENDIX;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder.Direction;
import io.isima.bios.errors.exception.AlreadyExistsException;
import io.isima.bios.errors.exception.NoSuchEntityException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.models.EntityId;
import io.isima.bios.models.TenantAppendixCategory;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/** Tenant appendix implementation backed by Cassandra. */
@Slf4j
public class TenantAppendixImpl implements TenantAppendix {
  public static final String COL_TENANT = "tenant";
  public static final String COL_TENANT_VERSION = "tenant_version";
  public static final String COL_CATEGORY = "category";
  public static final String COL_ENTRY_ID = "entry_id";
  public static final String COL_ENTRY_VERSION = "entry_version";
  public static final String COL_CONFIG = "config";
  public static final String COL_IS_DELETED = "is_deleted";

  private final CassandraConnection cassandraConnection;

  /** The constructor. */
  public TenantAppendixImpl(CassandraConnection cassandraConnection) {
    this.cassandraConnection = cassandraConnection;
    try {
      bootstrap();
    } catch (ApplicationException e) {
      throw new RuntimeException("Failed to start TenantAppendixImpl", e);
    }
  }

  /**
   * Ensure the existence of the appendix table.
   *
   * @throws ApplicationException thrown to indicate that creating table failed
   */
  private void bootstrap() throws ApplicationException {
    final var statement =
        SchemaBuilder.createTable(KEYSPACE_ADMIN, TABLE_APPENDIX)
            .ifNotExists()
            .addPartitionKey(COL_TENANT, DataType.text())
            .addClusteringColumn(COL_TENANT_VERSION, DataType.timestamp())
            .addClusteringColumn(COL_CATEGORY, DataType.text())
            .addClusteringColumn(COL_ENTRY_ID, DataType.text())
            .addClusteringColumn(COL_ENTRY_VERSION, DataType.timestamp())
            .addColumn(COL_CONFIG, DataType.text())
            .addColumn(COL_IS_DELETED, DataType.cboolean())
            .withOptions()
            .clusteringOrder(COL_TENANT_VERSION, Direction.DESC)
            .clusteringOrder(COL_CATEGORY, Direction.ASC)
            .clusteringOrder(COL_ENTRY_ID, Direction.ASC)
            .clusteringOrder(COL_ENTRY_VERSION, Direction.DESC);
    cassandraConnection.createTable(KEYSPACE_ADMIN, TABLE_APPENDIX, statement, logger);
  }

  @Override
  public <T> void createEntry(
      EntityId tenantId, TenantAppendixCategory category, String entryId, T newEntity)
      throws AlreadyExistsException, ApplicationException {
    Objects.requireNonNull(newEntity);
    if (getEntryOrNull(tenantId, category, entryId) != null) {
      throw new AlreadyExistsException(
          "tenant=" + tenantId.getName() + ", category=" + category + ", id=" + entryId);
    }
    insertEntry(tenantId, category, newEntity, entryId);
  }

  private <T> TenantAppendixEntry insertEntry(
      EntityId tenantId, TenantAppendixCategory category, T newEntity, String entryId)
      throws ApplicationException {
    Objects.requireNonNull(tenantId);
    Objects.requireNonNull(category);
    Objects.requireNonNull(entryId);

    final TenantAppendixEntry entry =
        (newEntity != null)
            ? TenantAppendixEntry.build(entryId, newEntity)
            : TenantAppendixEntry.buildBlankEntry(entryId);

    final var statement =
        QueryBuilder.insertInto(KEYSPACE_ADMIN, TABLE_APPENDIX)
            .value(COL_TENANT, tenantId.getName())
            .value(COL_TENANT_VERSION, tenantId.getVersion())
            .value(COL_CATEGORY, category.name())
            .value(COL_ENTRY_ID, entry.getEntryId())
            .value(COL_ENTRY_VERSION, entry.getEntryVersion())
            .value(COL_CONFIG, entry.getConfig())
            .value(COL_IS_DELETED, newEntity == null);
    cassandraConnection.execute("CreateTenantAppendix", logger, statement);

    return entry;
  }

  @Override
  public <T> List<T> getEntries(EntityId tenantId, TenantAppendixCategory category, Class<T> clazz)
      throws ApplicationException {
    Objects.requireNonNull(tenantId);
    Objects.requireNonNull(category);

    final var statement =
        QueryBuilder.select()
            .all()
            .from(KEYSPACE_ADMIN, TABLE_APPENDIX)
            .where(QueryBuilder.eq(COL_TENANT, tenantId.getName()))
            .and(QueryBuilder.eq(COL_TENANT_VERSION, tenantId.getVersion()))
            .and(QueryBuilder.eq(COL_CATEGORY, category.name()));
    final var result = cassandraConnection.execute("GetTenantAppendixEntries", logger, statement);
    final var entries = new ArrayList<T>();
    final var entryIds = new HashSet<String>();
    for (final var row : result.all()) {
      final String entryId = row.getString(COL_ENTRY_ID);
      if (entryIds.contains(entryId) || row.getBool(COL_IS_DELETED)) {
        entryIds.add(entryId);
        continue;
      }
      entryIds.add(entryId);
      final String config = row.getString(COL_CONFIG);
      final long entryVersion = row.getLong(COL_ENTRY_VERSION);
      entries.add(new TenantAppendixEntry(entryId, config, entryVersion).decode(clazz));
    }
    return entries;
  }

  @Override
  public <T> T getEntry(
      EntityId tenantId, TenantAppendixCategory category, String entryId, Class<T> clazz)
      throws ApplicationException, NoSuchEntityException {
    final var entry = getEntryCore(tenantId, category, entryId);
    return entry.decode(clazz);
  }

  private TenantAppendixEntry getEntryCore(
      EntityId tenantId, TenantAppendixCategory category, String entryId)
      throws ApplicationException, NoSuchEntityException {
    final var entry = getEntryOrNull(tenantId, category, entryId);
    if (entry == null) {
      throw new NoSuchEntityException(
          "tenant appendix",
          String.format("tenant=%s, category=%s, id=%s", tenantId.getName(), category, entryId));
    }
    return entry;
  }

  private TenantAppendixEntry getEntryOrNull(
      EntityId tenantId, TenantAppendixCategory category, String entryId)
      throws ApplicationException {
    Objects.requireNonNull(tenantId);
    Objects.requireNonNull(category);
    Objects.requireNonNull(entryId);

    final var statement =
        QueryBuilder.select()
            .all()
            .from(KEYSPACE_ADMIN, TABLE_APPENDIX)
            .where(QueryBuilder.eq(COL_TENANT, tenantId.getName()))
            .and(QueryBuilder.eq(COL_TENANT_VERSION, tenantId.getVersion()))
            .and(QueryBuilder.eq(COL_CATEGORY, category.name()))
            .and(QueryBuilder.eq(COL_ENTRY_ID, entryId))
            .limit(1);
    final var result = cassandraConnection.execute("GetTenantAppendixEntry", logger, statement);
    if (result.isExhausted()) {
      return null;
    }
    final var row = result.one();
    if (row.getBool(COL_IS_DELETED)) {
      return null;
    }
    final String config = row.getString(COL_CONFIG);
    final long entryVersion = row.getLong(COL_ENTRY_VERSION);
    return new TenantAppendixEntry(entryId, config, entryVersion);
  }

  @Override
  public <T> void updateEntry(
      EntityId tenantId, TenantAppendixCategory category, String entryId, T newEntity)
      throws NoSuchEntityException, ApplicationException {
    Objects.requireNonNull(tenantId);
    Objects.requireNonNull(category);
    Objects.requireNonNull(entryId);
    Objects.requireNonNull(newEntity);

    final var entry = getEntryCore(tenantId, category, entryId);

    final TenantAppendixEntry newEntry = insertEntry(tenantId, category, newEntity, entryId);
    assert newEntry.getEntryVersion() > entry.getEntryVersion();
  }

  @Override
  public void deleteEntry(EntityId tenantId, TenantAppendixCategory category, String entryId)
      throws NoSuchEntityException, ApplicationException {
    Objects.requireNonNull(tenantId);
    Objects.requireNonNull(category);
    Objects.requireNonNull(entryId);

    final var entry = getEntryCore(tenantId, category, entryId);
    final var newEntry = insertEntry(tenantId, category, null, entryId);
    assert newEntry.getEntryVersion() > entry.getEntryVersion();
  }

  @Override
  public void hardDelete(EntityId tenantId) throws ApplicationException {
    final var statement =
        QueryBuilder.delete()
            .from(KEYSPACE_ADMIN, TABLE_APPENDIX)
            .where(QueryBuilder.eq(COL_TENANT, tenantId.getName()));
    cassandraConnection.execute("GetTenantAppendixEntry", logger, statement);
  }
}
