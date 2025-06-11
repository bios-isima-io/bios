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
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.isima.bios.common.FilterCriteria;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.MemberStatus;
import io.isima.bios.models.auth.User;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.repository.Repository;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserRepository extends Repository {
  private static final Logger logger = LoggerFactory.getLogger(UserRepository.class);

  private static final String INDEX_TABLE_NAME = "users_index";
  private static final String INDEX_EMAIL = "email";
  private static final String INDEX_ID = "id";

  private final CassandraConnection cassandraConnection;

  private PreparedStatement insertStatement;
  private PreparedStatement selectStatement;
  private PreparedStatement statementToFindByEmal;
  private PreparedStatement updateStatement;

  /**
   * Create repository instance.
   *
   * @param cassandraConnection CassandraConnection instance
   */
  public UserRepository(CassandraConnection cassandraConnection) {
    this.cassandraConnection = cassandraConnection;
    try {
      createTable(cassandraConnection);
      createInsertStatement(cassandraConnection);
      createSelectStatement(cassandraConnection);
      createStatementToFindByEmail(cassandraConnection);
      createUpdateStatement(cassandraConnection);
    } catch (ApplicationException e) {
      throw new RuntimeException("Initializing UserRepository failed", e);
    }
  }

  private Create generateTableSchema() {
    return SchemaBuilder.createTable(CassandraConstants.KEYSPACE_BI, User.TABLE_NAME)
        .ifNotExists()
        .addPartitionKey("key", ascii())
        .addClusteringColumn("id", bigint())
        .addColumn("org_id", bigint())
        .addColumn("name", text())
        .addColumn("email", text())
        .addColumn("password_hash", text())
        .addColumn("enable", cboolean())
        .addColumn("permissions", ascii())
        .addColumn("access_id", text())
        .addColumn("secret_key", text())
        .addColumn("image_url", text())
        .addColumn("groups", list(bigint()))
        .addColumn("fav_reports", list(bigint()))
        .addColumn("fav_dashboards", list(bigint()))
        .addColumn("created_at", timestamp())
        .addColumn("updated_at", timestamp())
        .addColumn("ssh_user", text())
        .addColumn("ssh_pkey", text())
        .addColumn("ssh_ip", text())
        .addColumn("ssh_port", bigint())
        .addColumn("cloud_id", bigint())
        .addColumn("dev_instance", text())
        .addColumn("status", text());
  }

  private Create generateIndexSchema() {
    return SchemaBuilder.createTable(CassandraConstants.KEYSPACE_BI, INDEX_TABLE_NAME)
        .ifNotExists()
        .addPartitionKey(INDEX_EMAIL, text())
        .addColumn(INDEX_ID, bigint());
  }

  private void createTable(CassandraConnection cassandraConnection) throws ApplicationException {
    cassandraConnection.execute("Create UserRepository table", logger, generateTableSchema());
    cassandraConnection.execute("Create UserRepository index", logger, generateIndexSchema());
  }

  private void createInsertStatement(CassandraConnection cassandraConnection)
      throws ApplicationException {
    final StringBuilder builder = new StringBuilder(256);
    builder
        .append("INSERT INTO ")
        .append(CassandraConstants.KEYSPACE_BI)
        .append(".")
        .append(User.TABLE_NAME)
        .append(" (key, org_id, id, name, email, password_hash, enable, permissions,")
        .append(" access_id, secret_key, image_url, groups, fav_reports, fav_dashboards,")
        .append(" status, created_at, updated_at")
        .append(") VALUES (")
        .append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?")
        .append(")");

    insertStatement =
        cassandraConnection.createPreparedStatement(
            builder.toString(), "UserRepository insert", logger);
  }

  private void createUpdateStatement(CassandraConnection cassandraConnection)
      throws ApplicationException {
    final StringBuilder builder = new StringBuilder(256);
    builder
        .append("UPDATE ")
        .append(CassandraConstants.KEYSPACE_BI)
        .append(".")
        .append(User.TABLE_NAME)
        .append(" SET name = ?, password_hash = ?, enable = ?, permissions = ?, access_id = ?,")
        .append(" secret_key = ?, groups = ?, fav_reports = ?, fav_dashboards = ?, updated_at = ?,")
        .append(" ssh_pkey= ?, ssh_ip = ?, ssh_user = ?, dev_instance = ?, ssh_port = ?,")
        .append(" cloud_id = ?, status = ?")
        .append(" WHERE")
        .append(" key = '")
        .append(User.KEY_NAME)
        .append("' AND")
        .append(" id = ?");
    updateStatement =
        cassandraConnection.createPreparedStatement(
            builder.toString(), "UserRepository update", logger);
  }

  private void createSelectStatement(CassandraConnection cassandraConnection)
      throws ApplicationException {
    final StringBuilder builder = new StringBuilder(256);
    builder
        .append("SELECT")
        .append(" key, org_id, id, name, email, password_hash, enable, permissions,")
        .append(" access_id, secret_key, image_url, groups, fav_reports, fav_dashboards,")
        .append(" created_at, updated_at, ssh_user, ssh_pkey, ssh_ip, dev_instance, ssh_port,")
        .append(" cloud_id, status")
        .append(" FROM ")
        .append(CassandraConstants.KEYSPACE_BI)
        .append(".")
        .append(User.TABLE_NAME)
        .append(" WHERE")
        .append(" key = '")
        .append(User.KEY_NAME)
        .append("' AND")
        .append(" id = ?");

    selectStatement =
        cassandraConnection.createPreparedStatement(
            builder.toString(), "UserRepository select", logger);
  }

  private void createStatementToFindByEmail(CassandraConnection cassandraConnection)
      throws ApplicationException {
    final var statement =
        select()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, INDEX_TABLE_NAME)
            .where(eq(INDEX_EMAIL, bindMarker()));

    statementToFindByEmal =
        cassandraConnection.createPreparedStatement(
            statement, "UserRepository findByEmail", logger);
  }

  /**
   * Method to retrieve all users.
   *
   * @param orgId Organization Id
   * @param criteria for filtering by name, enable/disable etc.
   */
  public CompletableFuture<List<User>> findAllByOrg(
      Long orgId, FilterCriteria criteria, ExecutionState state) {

    final CompletableFuture<List<User>> promise = new CompletableFuture<>();

    final var statement =
        select()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, User.TABLE_NAME)
            .where(eq("key", User.KEY_NAME));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          List<User> users = null;
          if (criteria == null) {
            users = parseResult(result);
          } else {
            BiPredicate<Row, FilterCriteria> predicate = null;
            String searchTerm = criteria.getSearchTerm();

            Boolean onlyDisabledUsers = criteria.isDisabledOnly();
            if (onlyDisabledUsers == null) {
              predicate = (row, crit) -> true;
            } else if (onlyDisabledUsers == Boolean.TRUE) {
              predicate = byDisabled;
            } else {
              predicate = byEnabled;
            }
            if (StringUtils.isNotBlank(searchTerm)) {
              predicate = predicate.and(bySearchTerm);
            }
            users = parseResult(result, criteria, predicate);
          }

          users.removeIf(user -> !Objects.equals(user.getOrgId(), orgId));
          promise.complete(users);
        },
        (t) -> {
          logger.warn("Failed to get all users", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to retrieve user by org and id.
   *
   * @param orgId Organization Id
   * @param userId User Id
   * @param ignoreUserState Whether to ignore enabled or disabled
   */
  public CompletableFuture<User> findById(
      Long orgId, Long userId, boolean ignoreUserState, ExecutionState state) {
    CompletableFuture<User> promise = new CompletableFuture<>();

    BoundStatement statement = selectStatement.bind(userId);

    final var session = cassandraConnection.getSession();
    final var executor = state.getExecutor();
    final var resultFuture =
        Futures.transformAsync(
            session.executeAsync(statement),
            iterateToFindByOrgId(orgId, ignoreUserState, promise, executor),
            executor);

    Futures.addCallback(
        resultFuture,
        new FutureCallback<>() {
          @Override
          public void onSuccess(ResultSet result) {
            // do nothing, the promise was fulfilled by the iteration function already
          }

          @Override
          public void onFailure(Throwable t) {
            logger.warn("Failed to find user by id", t);
            promise.completeExceptionally(t);
          }
        },
        state.getExecutor());

    return promise;
  }

  private AsyncFunction<ResultSet, ResultSet> iterateToFindByOrgId(
      Long orgId, boolean ignoreUserState, CompletableFuture<User> promise, Executor executor) {
    return new AsyncFunction<>() {
      @Override
      public ListenableFuture<ResultSet> apply(ResultSet rows) {
        int remainingInPage = rows.getAvailableWithoutFetching();

        for (var row : rows) {
          final User user = parseResultRow(row);
          if ((orgId == null || Objects.equals(user.getOrgId(), orgId))
              && (user.getStatus() == MemberStatus.ACTIVE || ignoreUserState)) {
            promise.complete(user);
            return Futures.immediateFuture(rows);
          }
          if (--remainingInPage == 0) {
            break;
          }
        }

        boolean wasLastPage = rows.getExecutionInfo().getPagingState() == null;
        if (wasLastPage) {
          promise.complete(null);
          return Futures.immediateFuture(rows);
        }
        ListenableFuture<ResultSet> future = rows.fetchMoreResults();
        return Futures.transformAsync(
            future, iterateToFindByOrgId(orgId, ignoreUserState, promise, executor), executor);
      }
    };
  }

  /**
   * Method to retrieve user by org and id.
   *
   * @param orgId Organization Id
   * @param userId User Id
   */
  public CompletableFuture<User> findById(Long orgId, Long userId, ExecutionState state) {
    return findById(orgId, userId, false, state);
  }

  /**
   * Method to retrieve user by email.
   *
   * @param email User's email id
   */
  public CompletableFuture<User> findByEmail(String email, ExecutionState state) {
    return findByEmail(null, email, state);
  }

  /**
   * Method to retrieve user by email.
   *
   * @param orgId Organization Id
   * @param email User's email id
   */
  public CompletableFuture<User> findByEmail(Long orgId, String email, ExecutionState state) {

    final var fistStatement = statementToFindByEmal.bind(email.toLowerCase());

    return cassandraConnection
        .executeAsync(fistStatement, state)
        .thenComposeAsync(
            (firstResult) -> {
              if (firstResult.isExhausted()) {
                return CompletableFuture.completedStage(null);
              }
              final var id = firstResult.one().getLong(INDEX_ID);
              final var secondStatement = selectStatement.bind(id);
              return cassandraConnection
                  .executeAsync(secondStatement, state)
                  .thenApplyAsync(
                      (secondResult) -> {
                        if (secondResult.isExhausted()) {
                          return null;
                        }
                        return parseResultRow(secondResult.one());
                      },
                      state.getExecutor());
            },
            state.getExecutor())
        .toCompletableFuture();
  }

  /**
   * Method to create user.
   *
   * @param orgId Organization Id
   * @param userId User Id
   * @param userName User Name
   * @param userEmail User Email
   * @param passwordHash Hash of password
   * @param permissions List of permissions
   * @param userStatus User status
   * @return CompletableFuture of User
   */
  public CompletableFuture<User> setupUser(
      Long orgId,
      Long userId,
      String userName,
      String userEmail,
      String passwordHash,
      List<Permission> permissions,
      MemberStatus userStatus,
      ExecutionState state) {
    logger.debug("creating {} user", userName);

    Date date = new Date();

    User newUser = new User();
    newUser.setId(userId);
    newUser.setOrgId(orgId);
    newUser.setName(userName);
    newUser.setEmail(userEmail);
    newUser.setPassword(passwordHash);
    newUser.setStatus(Objects.requireNonNull(userStatus));
    newUser.setCreatedAt(date);
    newUser.setUpdatedAt(date);
    List<Integer> permissionIds =
        permissions.stream().map(Permission::getId).collect(Collectors.toList());
    newUser.setPermissions(StringUtils.join(permissionIds, ","));

    return save(newUser, state);
  }

  /**
   * Method to store user.
   *
   * @param user User Object
   */
  public CompletableFuture<User> save(User user, ExecutionState state) {
    CompletableFuture<User> promise = new CompletableFuture<>();

    final var userStatus = Objects.requireNonNull(user.getStatus());
    boolean isEnabled = userStatus == MemberStatus.ACTIVE;

    final Statement userEntryStatement =
        insertStatement
            .bind(
                user.getKey(),
                user.getOrgId(),
                user.getId(),
                user.getName(),
                user.getEmail().toLowerCase(),
                user.getPassword(),
                isEnabled,
                user.getPermissions(),
                user.getAccessId(),
                user.getSecretKey(),
                user.getImageUrl(),
                user.getGroupIds(),
                user.getFavoriteReports(),
                user.getFavoriteDashboards(),
                userStatus.name(),
                user.getCreatedAt(),
                user.getUpdatedAt())
            .setConsistencyLevel(ConsistencyLevel.ALL);

    final Statement userIndexStatement =
        insertInto(CassandraConstants.KEYSPACE_BI, INDEX_TABLE_NAME)
            .value(INDEX_EMAIL, user.getEmail().toLowerCase())
            .value(INDEX_ID, user.getId())
            .setConsistencyLevel(ConsistencyLevel.ALL);

    final var batch = new BatchStatement();
    batch.add(userEntryStatement);
    batch.add(userIndexStatement);

    cassandraConnection.executeAsync(
        batch,
        state,
        (result) -> promise.complete(user),
        (t) -> {
          logger.warn("Failed to save user", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to update user.
   *
   * @param user User Object
   */
  public CompletableFuture<User> update(User user, ExecutionState state) {

    final var userStatus = Objects.requireNonNull(user.getStatus());
    boolean isEnabled = userStatus == MemberStatus.ACTIVE;

    CompletableFuture<User> promise = new CompletableFuture<>();
    Statement statement =
        updateStatement
            .bind(
                user.getName(),
                user.getPassword(),
                isEnabled,
                user.getPermissions(),
                user.getAccessId(),
                user.getSecretKey(),
                user.getGroupIds(),
                user.getFavoriteReports(),
                user.getFavoriteDashboards(),
                user.getUpdatedAt(),
                user.getSshKey(),
                user.getSshIp(),
                user.getSshUser(),
                user.getDevInstance(),
                user.getSshPort(),
                user.getCloudId(),
                userStatus.name(),
                user.getId())
            .setConsistencyLevel(ConsistencyLevel.ALL);

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> promise.complete(user),
        (t) -> {
          logger.warn("Failed to update user", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  /**
   * Method to remove user.
   *
   * @param orgId Organization Id
   * @param userId User Id
   */
  public CompletableFuture<Void> remove(Long orgId, Long userId, ExecutionState state) {
    return findById(orgId, userId, true, state)
        .thenComposeAsync(
            (user) -> {
              final var batch = new BatchStatement();

              final var entryStatement =
                  delete()
                      .all()
                      .from(CassandraConstants.KEYSPACE_BI, User.TABLE_NAME)
                      .where(eq("key", User.KEY_NAME))
                      .and(in("id", userId))
                      .setConsistencyLevel(ConsistencyLevel.ALL);
              batch.add(entryStatement);

              if (user != null) {
                final var indexStatement =
                    delete()
                        .all()
                        .from(CassandraConstants.KEYSPACE_BI, INDEX_TABLE_NAME)
                        .where(eq(INDEX_EMAIL, user.getEmail().toLowerCase()))
                        .setConsistencyLevel(ConsistencyLevel.ALL);
                batch.add(indexStatement);
              }
              return cassandraConnection.executeAsync(batch, state).thenAccept((none) -> {});
            });
  }

  /**
   * Method to remove user.
   *
   * @param orgId Organization Id
   * @param userIds List of User Ids
   */
  public CompletableFuture<Void> remove(Long orgId, List<Long> userIds, ExecutionState state) {

    final var futures = new CompletableFuture[userIds.size()];

    for (int i = 0; i < userIds.size(); ++i) {
      final var id = userIds.get(i);
      futures[i] = remove(orgId, id, state);
    }

    return CompletableFuture.allOf(futures);
  }

  private List<User> parseResult(
      ResultSet result, FilterCriteria criteria, BiPredicate<Row, FilterCriteria> predicate) {
    List<User> users = new ArrayList<>();

    while (!result.isExhausted()) {
      Row row = result.one();
      if (predicate.test(row, criteria)) {
        User user = parseResultRow(row);
        users.add(user);
      }
    }

    return users;
  }

  private List<User> parseResult(ResultSet result) {
    List<User> users = new ArrayList<>();

    while (!result.isExhausted()) {
      Row row = result.one();

      User user = parseResultRow(row);
      users.add(user);
    }

    return users;
  }

  private User parseResultRow(Row row) {
    User user = new User();

    MemberStatus status = null;
    final String rowStatus = row.getString("status");
    if (rowStatus != null) {
      try {
        status = MemberStatus.valueOf(rowStatus);
      } catch (IllegalArgumentException e) {
        logger.error("Unknown user status: {}, row={}", rowStatus, row);
        status = MemberStatus.SUSPENDED;
      }
    } else {
      status = row.getBool("enable") ? MemberStatus.ACTIVE : MemberStatus.SUSPENDED;
    }

    user.setOrgId(row.getLong("org_id"));
    user.setId(row.getLong("id"));
    user.setName(row.getString("name"));
    user.setEmail(row.getString("email"));
    user.setPassword(row.getString("password_hash"));
    user.setPermissions(row.getString("permissions"));
    user.setAccessId(row.getString("access_id"));
    user.setSecretKey(row.getString("secret_key"));
    user.setImageUrl(row.getString("image_url"));
    user.setGroupIds(row.getList("groups", Long.class));
    user.setFavoriteReports(row.getList("fav_reports", Long.class));
    user.setFavoriteDashboards(row.getList("fav_dashboards", Long.class));
    user.setCreatedAt(row.getTimestamp("created_at"));
    user.setUpdatedAt(row.getTimestamp("updated_at"));
    user.setSshIp(row.getString("ssh_ip"));
    user.setSshKey(row.getString("ssh_pkey"));
    user.setSshUser(row.getString("ssh_user"));
    user.setDevInstance(row.getString("dev_instance"));
    user.setSshPort(row.getLong("ssh_port"));
    user.setCloudId(row.getLong("cloud_id"));
    user.setStatus(status);
    return user;
  }
}
