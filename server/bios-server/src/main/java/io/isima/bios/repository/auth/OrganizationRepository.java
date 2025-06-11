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
import static com.datastax.driver.core.DataType.text;
import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.auth.Organization;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrganizationRepository {
  private static final Logger logger = LoggerFactory.getLogger(OrganizationRepository.class);

  private final CassandraConnection cassandraConnection;

  /**
   * Constructor.
   *
   * @param cassandraConnection CassandraConnection instance
   */
  public OrganizationRepository(CassandraConnection cassandraConnection) {
    this.cassandraConnection = cassandraConnection;
    try {
      createTable(cassandraConnection);
    } catch (ApplicationException e) {
      throw new RuntimeException("Initializing OrganizationRepository failed", e);
    }
  }

  private Create generateTableSchema() {
    return SchemaBuilder.createTable(CassandraConstants.KEYSPACE_BI, Organization.TABLE_NAME)
        .ifNotExists()
        .addPartitionKey("key", ascii())
        .addClusteringColumn("id", bigint())
        .addColumn("name", text())
        .addColumn("tenant", text())
        .addColumn("settings", text())
        .addColumn("created_at", timestamp())
        .addColumn("updated_at", timestamp());
  }

  private void createTable(CassandraConnection cassandraConnection) throws ApplicationException {
    cassandraConnection.execute(
        "Create OrganizationRepository table", logger, generateTableSchema());
  }

  /**
   * Method to retrieve organization by id.
   *
   * @param id Organization Id
   */
  public CompletableFuture<Organization> findById(Long id, ExecutionState state) {
    CompletableFuture<Organization> promise = new CompletableFuture<>();

    final var statement =
        select()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, Organization.TABLE_NAME)
            .where(eq("key", Organization.KEY_NAME))
            .and(eq("id", id));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          List<Organization> orgs = parseResult(result);
          if (orgs.isEmpty()) {
            promise.complete(null);
          } else {
            promise.complete(orgs.get(0));
          }
        },
        (t) -> {
          logger.warn("Failed to get organization", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to retrieve organization by name.
   *
   * @param domain Organization name (domain)
   */
  public CompletableFuture<Organization> findByDomain(String domain, ExecutionState state) {
    CompletableFuture<Organization> promise = new CompletableFuture<>();

    final var statement =
        select()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, Organization.TABLE_NAME)
            .where(eq("key", Organization.KEY_NAME));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          for (Organization org : parseResult(result)) {
            if (domain != null && org.getName() != null && domain.equalsIgnoreCase(org.getName())) {
              promise.complete(org);
              return;
            }
          }
          promise.complete(null);
        },
        (t) -> {
          logger.warn("Failed to get organization", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to retrieve organization by tenant name.
   *
   * @param tenantName Tenant name
   */
  public CompletableFuture<Organization> findByTenantName(String tenantName, ExecutionState state) {
    Objects.requireNonNull(tenantName);
    CompletableFuture<Organization> promise = new CompletableFuture<>();

    final var statement =
        select()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, Organization.TABLE_NAME)
            .where(eq("key", Organization.KEY_NAME));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          for (Organization org : parseResult(result)) {
            if (tenantName != null
                && org.getTenant() != null
                && tenantName.equalsIgnoreCase(org.getTenant())) {
              promise.complete(org);
              return;
            }
          }
          promise.complete(null);
        },
        (t) -> {
          logger.warn("Failed to get organization", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to store organization.
   *
   * @param org organization Object
   */
  public CompletableFuture<Organization> save(Organization org, ExecutionState state) {
    CompletableFuture<Organization> promise = new CompletableFuture<>();

    final StringBuilder builder = new StringBuilder(256);
    builder
        .append("INSERT INTO ")
        .append(CassandraConstants.KEYSPACE_BI)
        .append(".")
        .append(Organization.TABLE_NAME)
        .append(" ")
        .append("(")
        .append("key, id, name, tenant, settings, created_at, updated_at")
        .append(") VALUES (")
        .append("?, ?, ?, ?, ?, ?, ?")
        .append(")");

    final var statement =
        new SimpleStatement(
            builder.toString(),
            org.getKey(),
            org.getId(),
            org.getName(),
            org.getTenant(),
            org.getSettings(),
            org.getCreatedAt(),
            org.getUpdatedAt());

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> promise.complete(org),
        (t) -> {
          logger.warn("Failed to save organization", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to remove organization.
   *
   * @param orgId Organization Id
   */
  public CompletableFuture<Boolean> remove(Long orgId, ExecutionState state) {
    CompletableFuture<Boolean> promise = new CompletableFuture<>();

    final var statement =
        delete()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, Organization.TABLE_NAME)
            .where(eq("key", Organization.KEY_NAME))
            .and(eq("id", orgId));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          promise.complete(true);
        },
        (t) -> {
          logger.warn("Failed to delete organization", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to setup organization.
   *
   * @param orgId Organization Id
   * @param orgName Organization Name
   * @param tenantName Tenant Name
   */
  public CompletableFuture<Organization> setupOrg(
      Long orgId, String orgName, String tenantName, ExecutionState state) {
    Objects.requireNonNull(orgId);
    Objects.requireNonNull(orgName);
    Objects.requireNonNull(tenantName);

    Date date = new Date();

    Organization newOrg = new Organization();
    newOrg.setId(orgId);
    newOrg.setName(orgName);
    newOrg.setTenant(tenantName);
    newOrg.setSettings("{}");
    newOrg.setCreatedAt(date);
    newOrg.setUpdatedAt(date);

    return save(newOrg, state);
  }

  private List<Organization> parseResult(ResultSet result) {
    List<Organization> organizations = new ArrayList<>();

    while (!result.isExhausted()) {
      Row row = result.one();
      Organization organization = new Organization();

      organization.setId(row.getLong("id"));
      organization.setName(row.getString("name"));
      organization.setTenant(row.getString("tenant"));
      organization.setSettings(row.getString("settings"));
      organization.setCreatedAt(row.getTimestamp("created_at"));
      organization.setUpdatedAt(row.getTimestamp("updated_at"));

      organizations.add(organization);
    }

    return organizations;
  }
}
