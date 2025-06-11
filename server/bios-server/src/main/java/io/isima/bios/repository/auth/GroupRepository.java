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
package io.isima.bios.repository.auth;

import static com.datastax.driver.core.DataType.ascii;
import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.driver.core.DataType.cboolean;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.text;
import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.auth.Group;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupRepository {

  private static final Logger logger = LoggerFactory.getLogger(GroupRepository.class);

  private final CassandraConnection cassandraConnection;

  /**
   * Create repository instance.
   *
   * @param cassandraConnection CassandraConnection instance
   */
  public GroupRepository(CassandraConnection cassandraConnection) {
    this.cassandraConnection = cassandraConnection;
    try {
      createTable(cassandraConnection);
    } catch (ApplicationException e) {
      throw new RuntimeException("Failed to initialize GroupRepository", e);
    }
  }

  private Create generateTableSchema() {
    return SchemaBuilder.createTable(CassandraConstants.KEYSPACE_BI, Group.TABLE_NAME)
        .ifNotExists()
        .addPartitionKey("key", ascii())
        .addClusteringColumn("id", bigint())
        .addColumn("org_id", bigint())
        .addColumn("name", text())
        .addColumn("sys_defined", cboolean())
        .addColumn("enable", cboolean())
        .addColumn("permissions", ascii())
        .addColumn("members", list(bigint()))
        .addColumn("created_at", timestamp())
        .addColumn("updated_at", timestamp());
  }

  private void createTable(CassandraConnection cassandraConnection) throws ApplicationException {
    cassandraConnection.execute("GroupRepository table", logger, generateTableSchema());
  }

  /**
   * Method to retrieve groups by org id and list of group id.
   *
   * @param orgId Organization Id
   * @param groupIds List of group id
   */
  public CompletableFuture<List<Group>> findByIds(
      Long orgId, List<Long> groupIds, ExecutionState state) {
    CompletableFuture<List<Group>> promise = new CompletableFuture<>();

    final var statement =
        select()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, Group.TABLE_NAME)
            .where(eq("key", Group.KEY_NAME))
            .and(in("id", groupIds));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          List<Group> groups = parseResult(result);
          groups.removeIf(group -> !Objects.equals(group.getOrgId(), orgId));
          promise.complete(groups);
        },
        (t) -> {
          logger.warn("Failed to get group", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to store group.
   *
   * @param group Group Object
   */
  public CompletableFuture<Group> save(Group group, ExecutionState state) {
    CompletableFuture<Group> promise = new CompletableFuture<>();

    final StringBuilder builder = new StringBuilder(256);
    builder
        .append("INSERT INTO ")
        .append(CassandraConstants.KEYSPACE_BI)
        .append(".")
        .append(Group.TABLE_NAME)
        .append(" ")
        .append("(")
        .append("key, id, org_id, name, sys_defined, enable, ")
        .append("permissions, members, created_at, updated_at")
        .append(") VALUES (")
        .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?")
        .append(")");

    final var statement =
        new SimpleStatement(
            builder.toString(),
            group.getKey(),
            group.getId(),
            group.getOrgId(),
            group.getName(),
            group.isSystemDefined(),
            group.isEnable(),
            group.getPermissions(),
            group.getMembers(),
            group.getCreatedAt(),
            group.getUpdatedAt());

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> promise.complete(group),
        (t) -> {
          logger.warn("Failed to save group", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to update group.
   *
   * @param group Group Object
   */
  public CompletableFuture<Group> update(Group group, ExecutionState state) {
    CompletableFuture<Group> promise = new CompletableFuture<>();

    final StringBuilder builder = new StringBuilder(256);
    builder
        .append("UPDATE ")
        .append(CassandraConstants.KEYSPACE_BI)
        .append(".")
        .append(Group.TABLE_NAME)
        .append(" ")
        .append("SET ")
        .append("name = ?, ")
        .append("enable = ?, ")
        .append("permissions = ?, ")
        .append("members = ?, ")
        .append("updated_at = ? ")
        .append("WHERE ")
        .append("key = ")
        .append("'")
        .append(Group.KEY_NAME)
        .append("'")
        .append(" AND ")
        .append("id = ?");

    final var statement =
        new SimpleStatement(
            builder.toString(),
            group.getName(),
            group.isEnable(),
            group.getPermissions(),
            group.getMembers(),
            group.getUpdatedAt(),
            group.getId());

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> promise.complete(group),
        (t) -> {
          logger.warn("Failed to update group", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to remove group.
   *
   * @param orgId Organization Id
   * @param groupId Group Id
   */
  public CompletableFuture<Void> remove(Long orgId, Long groupId, ExecutionState state) {
    CompletableFuture<Void> promise = new CompletableFuture<>();

    final var statement =
        delete()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, Group.TABLE_NAME)
            .where(eq("key", Group.KEY_NAME))
            .and(eq("id", groupId));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> promise.complete(null),
        (t) -> {
          logger.warn("Failed to delete group", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  private List<Group> parseResult(ResultSet result) {
    List<Group> groups = new ArrayList<>();

    while (!result.isExhausted()) {
      Row row = result.one();
      Group group = new Group();

      group.setOrgId(row.getLong("org_id"));
      group.setId(row.getLong("id"));
      group.setName(row.getString("name"));
      group.setSystemDefined(row.getBool("sys_defined"));
      group.setEnable(row.getBool("enable"));
      group.setPermissions(row.getString("permissions"));
      group.setMembers(row.getList("members", Long.class));
      group.setCreatedAt(row.getTimestamp("created_at"));
      group.setUpdatedAt(row.getTimestamp("updated_at"));

      groups.add(group);
    }

    return groups;
  }
}
