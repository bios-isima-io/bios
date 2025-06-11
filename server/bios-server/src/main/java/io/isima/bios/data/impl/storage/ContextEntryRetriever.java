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
package io.isima.bios.data.impl.storage;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.ReadFailureException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.stats.Timer;
import io.isima.bios.storage.cassandra.CassandraConnection;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextEntryRetriever {
  private static final Logger logger = LoggerFactory.getLogger(ContextEntryRetriever.class);

  public enum Type {
    ONLY_PRIMARY_KEY,
    INCLUDE_WRITE_TIME,
    ALL
  }

  private final ContextCassStream contextCassStream;
  @Getter private final Type type;
  @Getter private long tokenStart;
  private long tokenEnd;
  private int batchSize;

  private final ExecutionState state;

  private final CassandraConnection cassandraConnection;

  private final List<String> allColumnNames;
  private final int partitionKeyIndex;
  private final List<AttributeDesc> allAttributes;

  private final String firstStatement;
  private final String thenStatement;

  private final String queryGetToken;

  @Getter private boolean cancelled;

  List<Object[]> allValues = List.of();

  public ContextEntryRetriever(
      ContextCassStream contextCassStream,
      ContextIterationState state,
      long nextToken,
      int batchSize,
      Type type)
      throws ApplicationException {
    this.type = Objects.requireNonNull(type);
    this.contextCassStream = Objects.requireNonNull(contextCassStream);
    this.cassandraConnection = contextCassStream.getCassandraConnection();
    this.state = Objects.requireNonNull(state);
    this.tokenStart = nextToken;
    this.tokenEnd = Long.MAX_VALUE;
    this.batchSize = batchSize;

    final var keyspace = contextCassStream.getKeyspaceName();
    final var table = contextCassStream.getTableName();
    switch (type) {
      case ONLY_PRIMARY_KEY:
        allColumnNames = contextCassStream.getKeyColumns();
        allAttributes = contextCassStream.getStreamDesc().getAttributes();
        partitionKeyIndex = 0;
        break;
      case INCLUDE_WRITE_TIME:
        allColumnNames = new ArrayList<>();
        allAttributes = new ArrayList<>();
        allColumnNames.addAll(contextCassStream.getKeyColumns());
        allColumnNames.forEach(
            (name) -> allAttributes.add(contextCassStream.getAttributeDesc(name)));
        allColumnNames.add(ContextCassStream.COLUMN_WRITE_TIME);
        allAttributes.add(null);
        partitionKeyIndex = 0;
        break;
      case ALL:
        {
          allColumnNames = new ArrayList<>();
          allAttributes = new ArrayList<>();
          int index = -1;
          final var attributes = contextCassStream.getStreamDesc().getAttributes();
          final var attributeToIndex = new HashMap<String, Integer>();
          for (int i = 0; i < attributes.size(); ++i) {
            final var attributeName = attributes.get(i).getName();
            final var attribute = contextCassStream.getAttributeDesc(attributeName);
            if (attribute.getName().equals(contextCassStream.getPrimaryKeyNames().get(0))) {
              index = i;
            }
            allColumnNames.add(attribute.getColumn());
            allAttributes.add(attribute);
            attributeToIndex.put(attributeName, i);
          }
          partitionKeyIndex = index;
          allColumnNames.add(ContextCassStream.COLUMN_WRITE_TIME);
          allAttributes.add(null);
          allColumnNames.add(ContextCassStream.COLUMN_ENTRY_ID);
          allAttributes.add(null);
          state.setAttributeNameToIndex(attributeToIndex);
          break;
        }
      default:
        throw new UnsupportedOperationException("type " + type + " not supported");
    }

    final var selectColumns = allColumnNames.stream().collect(Collectors.joining(", "));

    final var partitionKeyColumn = contextCassStream.getKeyColumns().get(0);

    firstStatement =
        String.format(
            "SELECT %s FROM %s.%s WHERE TOKEN(%s) <= ? LIMIT %s",
            selectColumns, keyspace, table, partitionKeyColumn, batchSize);

    thenStatement =
        String.format(
            "SELECT %s FROM %s.%s WHERE TOKEN(%s) >= ? AND TOKEN(%s) <= ? LIMIT %s",
            selectColumns, keyspace, table, partitionKeyColumn, partitionKeyColumn, batchSize);

    final var where = new StringJoiner(" AND ");
    contextCassStream
        .getKeyColumns()
        .forEach(
            (column) -> {
              where.add(column + " = ?");
            });

    queryGetToken =
        String.format(
            "SELECT TOKEN(%s) FROM %s.%s WHERE %s = ? LIMIT 1",
            partitionKeyColumn, keyspace, table, partitionKeyColumn);
  }

  public ContextEntryRetriever(
      ContextCassStream contextCassStream, ContextIterationState state, int batchSize, Type type)
      throws ApplicationException {
    this(contextCassStream, state, Long.MIN_VALUE, batchSize, type);
  }

  public boolean hasNext() {
    return tokenStart < Long.MAX_VALUE;
  }

  public CompletableFuture<List<Object[]>> getNextEntries() {
    final var future = new CompletableFuture<List<Object[]>>();
    getNextEntriesCore(future, 10);
    return future;
  }

  public void cancel() {
    cancelled = true;
  }

  private void getNextEntriesCore(CompletableFuture<List<Object[]>> future, int retryCredit) {
    final Statement statement;
    if (tokenStart == Long.MIN_VALUE) {
      statement = new SimpleStatement(firstStatement, tokenEnd);
    } else {
      statement = new SimpleStatement(thenStatement, tokenStart, tokenEnd);
    }
    final Optional<Timer> dbTimer = state.startStorageAccess();
    if (tokenStart == Long.MAX_VALUE) {
      future.complete(List.of());
      return;
    }
    cassandraConnection
        .executeAsync(statement, state)
        .thenAcceptAsync(
            (keyResults) ->
                ExecutionHelper.run(
                    () -> {
                      dbTimer.ifPresent((timer) -> timer.commit());
                      final var prevAllValues = allValues;
                      allValues = new ArrayList<>();
                      int overlapCheckOffset = Math.min(50, prevAllValues.size());
                      Object lastPartitionKey = null;
                      int numRows = 0;
                      while (!keyResults.isExhausted()) {
                        ++numRows;
                        final var row = keyResults.one();
                        final var values = new Object[allColumnNames.size()];
                        for (int i = 0; i < values.length; ++i) {
                          Object value = row.getObject(allColumnNames.get(i));
                          if (value == null && allAttributes.get(i) != null) {
                            value = allAttributes.get(i).getInternalDefaultValue();
                          }
                          if (value == null) {
                            throw new CompletionException(
                                new ApplicationException(
                                    String.format(
                                        "Context table returns null column; tenant=%s.%d context=%s.%d",
                                        contextCassStream.getTenantName(),
                                        contextCassStream.getTenantVersion(),
                                        contextCassStream.getStreamName(),
                                        contextCassStream.getStreamVersion())));
                          }
                          values[i] = value;
                        }
                        lastPartitionKey = values[partitionKeyIndex];
                        boolean isDuplicate = false;
                        while (overlapCheckOffset > 0) {
                          final var prevPartitionKey =
                              prevAllValues
                                  .get(prevAllValues.size() - overlapCheckOffset)[
                                  partitionKeyIndex];
                          --overlapCheckOffset;
                          if (prevPartitionKey.equals(lastPartitionKey)) {
                            isDuplicate = true;
                            break;
                          }
                        }
                        if (!isDuplicate) {
                          allValues.add(values);
                        }
                      }
                      if (numRows < batchSize) {
                        tokenStart = Long.MAX_VALUE;
                        future.complete(allValues);
                        return;
                      }

                      // Update the next token
                      getTokenAsync(lastPartitionKey)
                          .thenAccept(
                              (nextToken) -> {
                                final long startDelta = nextToken - tokenStart;
                                final long endDelta;
                                if (startDelta < 0) {
                                  // overflow
                                  endDelta = Long.MAX_VALUE - tokenEnd;
                                } else {
                                  endDelta = Math.min(Long.MAX_VALUE - tokenEnd, startDelta);
                                }
                                tokenStart = nextToken;
                                tokenEnd += endDelta;
                                future.complete(allValues);
                              });
                    }),
            state.getExecutor())
        .whenCompleteAsync(
            (none, t) -> {
              if (t != null) {
                if (t instanceof ReadFailureException) {
                  // It's likely to be the too-many-tombstones issue. Narrow the token range and
                  // retry.
                  if (retryCredit > 0) {
                    long prevEnd = tokenEnd;
                    tokenEnd /= 2;
                    tokenEnd += tokenStart / 2;
                    logger.warn(
                        "DB read failure during {}, retrying with narrower token range;"
                            + " tenant={}, context={}, start={}, end={}, newEnd={}, error={}",
                        contextCassStream.getTenantName(),
                        contextCassStream.getStreamName(),
                        tokenStart,
                        prevEnd,
                        tokenEnd,
                        t.toString());
                    getNextEntriesCore(future, retryCredit - 1);
                    return;
                  } else {
                    logger.error(
                        "DB read failure retry limit, giving up;"
                            + " tenant={}, context={}, error={}",
                        contextCassStream.getTenantName(),
                        contextCassStream.getStreamName(),
                        t.toString());
                  }
                }
                future.completeExceptionally(t);
              }
            },
            state.getExecutor());
  }

  private CompletableFuture<Long> getTokenAsync(Object partitionKey) {
    return ExecutionHelper.supply(
            () ->
                cassandraConnection
                    .executeAsync(new SimpleStatement(queryGetToken, partitionKey), state)
                    .thenApply(
                        (result) -> {
                          if (result.isExhausted()) {
                            throw new CompletionException(
                                new ApplicationException(
                                    String.format(
                                        "Context table data is inconsistent; tenant=%s.%d context=%s.%d entry=%s",
                                        contextCassStream.getTenantName(),
                                        contextCassStream.getTenantVersion(),
                                        contextCassStream.getStreamName(),
                                        contextCassStream.getStreamVersion(),
                                        partitionKey)));
                          }
                          final var row = result.one();
                          return row.getLong(0);
                        }))
        .toCompletableFuture();
  }
}
