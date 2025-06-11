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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.KEYSPACE_ADMIN;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.TABLE_RESOURCE_ALLOCATION;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.AlreadyExistsException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.Date;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ResourceAllocator {

  private static final String RESOURCE_TYPE = "resource_type";
  private static final String RESOURCE = "resource";
  private static final String RESOURCE_USER = "resource_user";
  private static final String UPDATED_AT = "updated_at";

  private final CassandraConnection cassandraConnection;

  /** The constructor. */
  public ResourceAllocator(CassandraConnection cassandraConnection) {
    this.cassandraConnection = cassandraConnection;

    try {
      final var statement =
          SchemaBuilder.createTable(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION)
              .addPartitionKey(RESOURCE_TYPE, DataType.text())
              .addClusteringColumn(RESOURCE, DataType.text())
              .addColumn(RESOURCE_USER, DataType.text())
              .addColumn(UPDATED_AT, DataType.timestamp());
      cassandraConnection.createTable(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION, statement, logger);
    } catch (ApplicationException e) {
      logger.error(
          "Failed to start ResourceAllocator, failed to create the resource allocation table: {}",
          e.toString());
      throw new RuntimeException(e);
    }
  }

  /**
   * Assign a resource to the user.
   *
   * <p>The method throws {@link AlreadyExistsException} if the resource is already taken.
   *
   * @param resourceType Resource type
   * @param resource Resource to assign
   * @param resourceUser Assignee name/identifier
   * @throws AlreadyExistsException Thrown to indicate that the resource is taken already * @throws
   *     ApplicationException Thrown to indicate that an unexpected error has occurred
   */
  public void assignResource(ResourceType resourceType, String resource, String resourceUser)
      throws AlreadyExistsException, ApplicationException {
    Objects.requireNonNull(resourceType);
    Objects.requireNonNull(resource);
    Objects.requireNonNull(resourceUser);

    if (!tryInsert(resourceType, resource, resourceUser)) {
      throw new AlreadyExistsException(
          String.format(
              "%s already exists in apps resource %s",
              resource, resourceType.name().toLowerCase()));
    }
  }

  /**
   * Allocates a resource to the user.
   *
   * <p>The method first tries to assign the initial value. If it is occupied, the method assigns
   * current maximum value plus one.
   *
   * @param resourceType Resource type
   * @param initialValue Initial value to assign
   * @param resourceUser Assignee name/identifier
   * @return Assigned integer resource, the value may be different from the specified initialValue
   * @throws ApplicationException Thrown to indicate that an unexpected error has occurred
   */
  public int allocateIntegerResource(
      ResourceType resourceType, int initialValue, String resourceUser)
      throws ApplicationException {
    Objects.requireNonNull(resourceType);
    Objects.requireNonNull(resourceUser);

    String resource = Integer.toString(initialValue);

    // Try the requested resource first
    if (tryInsert(resourceType, resource, resourceUser)) {
      return initialValue;
    }

    // The requested resource is occupied, find an available resource
    final int maxTrials = 10;
    long backoffMillis = 100;
    for (int i = 0; i < maxTrials; ++i) {
      final int assigned = findAvailableIntegerResource(resourceType);
      if (tryInsert(resourceType, Integer.toString(assigned), resourceUser)) {
        return assigned;
      }
      try {
        Thread.sleep(backoffMillis);
      } catch (InterruptedException e) {
        logger.error("Allocation interrupted; type=%s, user=%s", resourceType, resourceUser);
        Thread.currentThread().interrupt();
      }
      backoffMillis *= 2;
    }
    throw new ApplicationException(
        String.format(
            "Available resources are not found; type=%s, user=%s", resourceType, resourceUser));
  }

  /**
   * Checks whether the specified resource is allocated already.
   *
   * @return Resource info if allocated, otherwise null
   * @throws ApplicationException thrown to indicate that an unexpected error has happened
   */
  public Resource checkResource(ResourceType resourceType, String resource)
      throws ApplicationException {
    Objects.requireNonNull(resourceType);
    Objects.requireNonNull(resource);
    final var statement =
        QueryBuilder.select()
            .from(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION)
            .where(eq(RESOURCE_TYPE, resourceType.name()))
            .and(eq(RESOURCE, resource));
    final var resultSet = executeLwt(statement, "GetAllocatedResource");
    if (resultSet.isExhausted()) {
      return null;
    }
    final var row = resultSet.one();
    return new Resource(
        ResourceType.valueOf(row.getString(RESOURCE_TYPE)),
        row.getString(RESOURCE),
        row.getString(RESOURCE_USER),
        row.getTimestamp(UPDATED_AT));
  }

  /**
   * Releases a resource.
   *
   * @param resourceType Resource type
   * @param resource Resource to release
   * @param expirationSeconds If greater than zero, the method leaves the allocation in the table
   *     for reservation/blackout period
   * @throws ApplicationException Thrown to indicate that there's an unexpected error
   */
  public void releaseResource(ResourceType resourceType, String resource, int expirationSeconds)
      throws ApplicationException {
    Objects.requireNonNull(resourceType);
    Objects.requireNonNull(resource);
    final var deleteStatement =
        QueryBuilder.delete()
            .from(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION)
            .where(eq(RESOURCE_TYPE, resourceType.name()))
            .and(eq(RESOURCE, resource))
            .ifExists();
    executeLwt(deleteStatement, "DeleteResource");

    if (expirationSeconds > 0) {
      // There's a gap between deleting the entry above and creating the "reservation" entry below,
      // so there's a slim chance for another user coming in and take this resource. There's no way
      // to avoid it for now, but possibility to happen this should be low.
      final var statement =
          QueryBuilder.insertInto(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION)
              .using(ttl(expirationSeconds))
              .value(RESOURCE_TYPE, resourceType.name())
              .value(RESOURCE, resource)
              .value(RESOURCE_USER, "released")
              .value(UPDATED_AT, new Date())
              .ifNotExists();
      executeLwt(statement, "ReserveResource");
    }
  }

  private boolean tryInsert(ResourceType resourceType, String resource, String resourceUser)
      throws ApplicationException {
    var statement =
        QueryBuilder.insertInto(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION)
            .value(RESOURCE_TYPE, resourceType.name())
            .value(RESOURCE, resource)
            .value(RESOURCE_USER, resourceUser)
            .value(UPDATED_AT, new Date())
            .ifNotExists();
    final var resultSet = executeLwt(statement, "AllocateResource");
    return resultSet.wasApplied();
  }

  private int findAvailableIntegerResource(ResourceType resourceType) throws ApplicationException {
    final var statement =
        QueryBuilder.select(RESOURCE)
            .from(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION)
            .where(eq(RESOURCE_TYPE, resourceType.name()));

    final var resultSet =
        cassandraConnection.execute("Select allocated resources", logger, statement);
    int max = Integer.MIN_VALUE;
    while (!resultSet.isExhausted()) {
      final var row = resultSet.one();
      int currentResource = Integer.parseInt(row.getString(RESOURCE));
      max = Math.max(currentResource, max);
    }
    return max + 1;
  }

  /**
   * Clear all resources in the specified allocation type.
   *
   * <p>This method is meant to be used only by test programs.
   */
  protected void clearAllocation(ResourceType resourceType) throws ApplicationException {
    final var statement =
        QueryBuilder.delete()
            .from(KEYSPACE_ADMIN, TABLE_RESOURCE_ALLOCATION)
            .where(eq(RESOURCE_TYPE, resourceType.name()));
    cassandraConnection.execute("Clear allocation", logger, statement);
  }

  private ResultSet executeLwt(Statement statement, String operation) throws ApplicationException {
    long sleep = TfosConfig.CASSANDRA_OP_RETRY_SLEEP_MILLIS;
    final int maxRetry = 10;
    try {
      int trial = 0;
      while (true) {
        try {
          final ResultSet result = cassandraConnection.getSession().execute(statement);
          return result;
        } catch (WriteTimeoutException e) {
          if (e.getWriteType() == WriteType.CAS && trial++ < maxRetry) {
            logger.debug(
                "Retrying CAS for {}; trial={}, error={}", operation, trial, e.getMessage());
            try {
              Thread.sleep(sleep);
            } catch (InterruptedException ei) {
              logger.error("Retry interrupted");
              Thread.currentThread().interrupt();
            }
            sleep = (long) (sleep * 1.5);
          } else {
            logger.warn("Failed to run LWT; operation={}, error={}", operation, e.getMessage());
            throw e;
          }
        }
      }
    } catch (DriverException e) {
      throw new ApplicationException(
          String.format("Failed to run LWT; operation=%s", operation), e);
    }
  }

  @AllArgsConstructor
  @Getter
  public static class Resource {
    private ResourceType resourceType;
    private String resource;
    private String resourceUser;
    private Date updatedAt;
  }
}
