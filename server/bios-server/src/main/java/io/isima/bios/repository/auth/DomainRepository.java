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
import static com.datastax.driver.core.DataType.text;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import io.isima.bios.data.storage.cassandra.CassandraConstants;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.auth.Domain;
import io.isima.bios.models.auth.DomainApprovalAction;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DomainRepository {
  private static final Logger logger = LoggerFactory.getLogger(DomainRepository.class);

  private static final String KEY = "key";
  private static final String DOMAIN_NAME = "domain_name";
  private static final String APPROVAL_ACTION = "approval_action";

  private final CassandraConnection cassandraConnection;

  /**
   * Constructor.
   *
   * @param cassandraConnection CassandraConnection instance
   */
  public DomainRepository(CassandraConnection cassandraConnection) throws ApplicationException {
    this.cassandraConnection = cassandraConnection;
    try {
      createTable(cassandraConnection);
    } catch (ApplicationException e) {
      throw new ApplicationException("Initializing Domain failed", e);
    }
  }

  private Create generateTableSchema() {
    return SchemaBuilder.createTable(CassandraConstants.KEYSPACE_BI, Domain.TABLE_NAME)
        .ifNotExists()
        .addPartitionKey(KEY, ascii())
        .addClusteringColumn(DOMAIN_NAME, text())
        .addColumn(APPROVAL_ACTION, text());
  }

  private void createTable(CassandraConnection cassandraConnection) throws ApplicationException {
    cassandraConnection.execute("Create DomainRepository table", logger, generateTableSchema());
  }

  /**
   * Method to retrieve List of domainObjects by domainNames.
   *
   * @param domainNameList list of domain names
   */
  public CompletableFuture<List<Domain>> findByDomainName(
      List<String> domainNameList, ExecutionState state) {
    CompletableFuture<List<Domain>> promise = new CompletableFuture<>();

    final var statement =
        select()
            .all()
            .from(CassandraConstants.KEYSPACE_BI, Domain.TABLE_NAME)
            .where(eq(KEY, Domain.KEY_NAME))
            .and(in(DOMAIN_NAME, domainNameList.toArray()));

    cassandraConnection.executeAsync(
        statement,
        state,
        (result) -> {
          List<Domain> domainList = parseResult(result);
          if (domainList.isEmpty()) {
            promise.complete(new ArrayList<>());
          } else {
            promise.complete(domainList);
          }
        },
        (t) -> {
          logger.warn("Failed to get domain", t);
          promise.completeExceptionally(t);
        });

    return promise;
  }

  private List<Domain> parseResult(ResultSet result) {
    List<Domain> domainList = new ArrayList<>();
    while (!result.isExhausted()) {
      Row row = result.one();
      Domain domain = new Domain();
      domain.setApprovalAction(
          DomainApprovalAction.getApprovalAction(row.getString(APPROVAL_ACTION)));
      domain.setDomainName(row.getString(DOMAIN_NAME));
      domainList.add(domain);
    }
    return domainList;
  }
}
