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

import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_DUMMY_PARTITION_KEY;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.PREFIX_EVENTS_COLUMN;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.ContextQueryState;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.data.filter.FilterTerm;
import io.isima.bios.data.impl.ContextIndexHelper;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.data.impl.maintenance.DigestSpecifier;
import io.isima.bios.dto.SelectContextEntriesResponse;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.exception.FilterNotApplicableException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.Range;
import io.isima.bios.models.v1.AttributeDesc;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContextIndexCassStream extends CassStream {
  private static final Logger logger = LoggerFactory.getLogger(ContextIndexCassStream.class);

  private static final String FORMAT_CREATE_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.%s (%sPRIMARY KEY (%s))"
          + " WITH comment = 'tenant=%s (version=%d), parentContext=%s, index=%s (version=%d)'"
          + " AND compaction = {"
          + "   'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'}";

  private PreparedStatement deletePreparedStatement;

  protected ContextIndexCassStream(CassTenant cassTenant, StreamDesc streamDesc) {
    super(cassTenant, streamDesc);
  }

  @Override
  public void initialize(String keyspaceName) throws ApplicationException {
    super.initialize(keyspaceName);
    deletePreparedStatement =
        cassandraConnection.createPreparedStatement(
            deleteQueryString, "prepare deleteContextIndex", logger);
  }

  @Override
  public String makeCreateTableStatement(final String keyspaceName) {
    // Context index stream attributes includes all required attributes.
    // Add them all to the columns list.
    IndexType indexType = streamDesc.getFeatures().get(0).getIndexType();

    final StringBuilder columnsBuilder = new StringBuilder();
    if (indexType == IndexType.RANGE_QUERY) {
      columnsBuilder.append(COL_DUMMY_PARTITION_KEY).append(" boolean, ");
    }
    streamDesc
        .getAttributes()
        .forEach(
            (attrDesc) -> {
              final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());
              columnsBuilder
                  .append(cassAttrDesc.getColumn())
                  .append(" ")
                  .append(valueTypeToCassDataTypeName(cassAttrDesc.getAttributeType()))
                  .append(", ");
            });

    int numAttrInPartitionKey = 1;
    if (indexType == IndexType.EXACT_MATCH) {
      numAttrInPartitionKey = streamDesc.getFeatures().get(0).getDimensions().size();
    }

    final StringBuilder indexKeyBuilder = new StringBuilder();
    int numAttrsFilledInKey = 0;
    if (numAttrInPartitionKey > 0) {
      indexKeyBuilder.append("(");
    }
    if (indexType == IndexType.RANGE_QUERY) {
      indexKeyBuilder.append(COL_DUMMY_PARTITION_KEY);
      numAttrsFilledInKey++;
    }

    // Attributes that should be part of the index key are present in additionalAttributes.
    Iterator<AttributeDesc> iter = streamDesc.getAdditionalAttributes().iterator();
    while (iter.hasNext()) {
      final AttributeDesc attrDesc = iter.next();
      final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());

      if ((numAttrInPartitionKey > 0) && (numAttrsFilledInKey == numAttrInPartitionKey)) {
        indexKeyBuilder.append(")");
      }
      if (numAttrsFilledInKey > 0) {
        indexKeyBuilder.append(", ");
      }
      indexKeyBuilder.append(cassAttrDesc.getColumn());
      numAttrsFilledInKey++;
    }

    String statement =
        String.format(
            FORMAT_CREATE_TABLE,
            keyspaceName,
            getTableName(),
            columnsBuilder.toString(),
            indexKeyBuilder.toString(),
            cassTenant.getName(),
            cassTenant.getVersion(),
            streamDesc.getParentStream().getName(),
            streamDesc.getName(),
            streamDesc.getVersion());
    logger.debug("Create context index statement: {}", statement);
    return statement;
  }

  @Override
  protected String generateIngestQueryString(final String keyspaceName) {
    final StringBuilder statementBuilder =
        new StringBuilder(String.format("INSERT INTO %s.%s (", keyspaceName, tableName));
    final StringBuilder valuesPartBuilder = new StringBuilder(") VALUES (");

    int numAttrsFilled = 0;

    if (streamDesc.getFeatures().get(0).getIndexType() == IndexType.RANGE_QUERY) {
      statementBuilder.append(COL_DUMMY_PARTITION_KEY);
      valuesPartBuilder.append("?");
      numAttrsFilled++;
    }

    Iterator<AttributeDesc> iter = streamDesc.getAttributes().iterator();
    while (iter.hasNext()) {
      final AttributeDesc attrDesc = iter.next();
      final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());

      if (numAttrsFilled > 0) {
        statementBuilder.append(", ");
        valuesPartBuilder.append(", ");
      }

      statementBuilder.append(cassAttrDesc.getColumn());
      valuesPartBuilder.append("?");
      numAttrsFilled++;
    }

    statementBuilder.append(valuesPartBuilder).append(") USING TIMESTAMP ?");
    return statementBuilder.toString();
  }

  @Override
  protected String generateDeleteQueryString(String keyspaceName) {
    int numAttrsFilled = 0;
    final StringBuilder statementBuilder =
        new StringBuilder(
            String.format("DELETE FROM %s.%s USING TIMESTAMP ? WHERE ", keyspaceName, tableName));

    if (streamDesc.getFeatures().get(0).getIndexType() == IndexType.RANGE_QUERY) {
      statementBuilder.append(COL_DUMMY_PARTITION_KEY);
      statementBuilder.append(" = ?");
      numAttrsFilled++;
    }

    Iterator<AttributeDesc> iter = streamDesc.getAdditionalAttributes().iterator();
    while (iter.hasNext()) {
      final AttributeDesc attrDesc = iter.next();
      final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());

      if (numAttrsFilled > 0) {
        statementBuilder.append(" AND ");
      }

      statementBuilder.append(cassAttrDesc.getColumn());
      statementBuilder.append(" = ?");
      numAttrsFilled++;
    }
    return statementBuilder.toString();
  }

  @Override
  public Statement makeInsertStatement(final Event event) throws ApplicationException {
    IndexType indexType = streamDesc.getFeatures().get(0).getIndexType();
    int numReservedColumns = 0;
    if (indexType == IndexType.RANGE_QUERY) {
      numReservedColumns = 1;
    }
    final int numAttributes = listSize(streamDesc.getAttributes());

    final Object[] values = new Object[numReservedColumns + numAttributes + 1];
    int index = 0;
    if (indexType == IndexType.RANGE_QUERY) {
      values[index++] = Boolean.FALSE;
    }
    index = populateIngestValues(index, event, streamDesc.getAttributes(), values);

    // Cassandra expects timestamp in microseconds.
    values[index++] = event.getIngestTimestamp().getTime() * 1000;

    final PreparedStatement prepared = getPreparedIngest();
    if (prepared != null) {
      return prepared.bind(values);
    } else {
      return new SimpleStatement(getIngestQueryString(), values);
    }
  }

  @Override
  public Statement makeDeleteStatement(Event event) throws ApplicationException {
    IndexType indexType = streamDesc.getFeatures().get(0).getIndexType();
    int numReservedColumns = 0;
    if (indexType == IndexType.RANGE_QUERY) {
      numReservedColumns = 1;
    }
    final int numAttributes = listSize(streamDesc.getAdditionalAttributes());

    final Object[] values = new Object[numReservedColumns + numAttributes + 1];
    int index = 0;
    values[index++] = event.getIngestTimestamp().getTime() * 1000;
    if (indexType == IndexType.RANGE_QUERY) {
      values[index++] = Boolean.FALSE;
    }
    populateIngestValues(index, event, streamDesc.getAdditionalAttributes(), values);

    return deletePreparedStatement.bind(values);
  }

  @Override
  public List<QueryInfo> makeExtractStatements(
      QueryExecutionState state,
      Collection<CassAttributeDesc> attributes,
      final Long startTime,
      final Long endTime)
      throws InvalidFilterException, FilterNotApplicableException {
    return null;
  }

  public String makeSelectStatement(SelectContextRequest request, ContextQueryState queryState) {
    StringBuilder query =
        new StringBuilder(String.format("SELECT * FROM %s.%s ", keyspaceName, tableName));

    IndexType indexType = streamDesc.getFeatures().get(0).getIndexType();

    String whereClause = request.getWhere();
    if (whereClause != null && !whereClause.isEmpty()) {
      final List<SingleColumnRelation> relations = queryState.getFilter();
      query.append("WHERE ");
      if (indexType == IndexType.RANGE_QUERY) {
        query.append(COL_DUMMY_PARTITION_KEY).append(" = false AND ");
      }
      int numConditions = 0;
      Iterator<SingleColumnRelation> iter = relations.iterator();
      while (iter.hasNext()) {
        final var relation = iter.next();
        if (numConditions > 0) {
          query.append(" AND ");
        }
        final Operator operator = relation.operator();
        final var attributeName = PREFIX_EVENTS_COLUMN + relation.getEntity().rawText();
        query.append(attributeName).append(" ");
        query.append(operator.toString()).append(" ");
        if (operator != Operator.IN) {
          query.append(relation.getValue());
        } else {
          String values =
              relation.getInValues().stream().map(String::valueOf).collect(Collectors.joining(","));
          query.append("(").append(values).append(")");
        }
        numConditions++;
      }
    }

    boolean orderBySkipped = false;
    if (request.getOrderBy() != null) {
      // Use orderBy only if it is supported
      if ((indexType == IndexType.RANGE_QUERY)
          && (request
                  .getOrderBy()
                  .getKeyName()
                  .compareToIgnoreCase(streamDesc.getFeatures().get(0).getDimensions().get(0))
              == 0)) {
        String by = PREFIX_EVENTS_COLUMN + request.getOrderBy().getKeyName();
        queryState.setSortSpecifiedInQuery(true);
        query.append(" ORDER BY ").append(by);
        if (request.getOrderBy().isReverse()) {
          query.append(" DESC");
        } else {
          query.append(" ASC");
        }
      } else {
        orderBySkipped = true;
      }
    }

    // Set limitation here, but we'll limit later when orderBy is skipped.
    // Sorting happens later in the case, then limiting is possible only after that.
    if (request.getLimit() != null && !orderBySkipped) {
      query.append(" LIMIT ").append(request.getLimit());
    }

    return query.toString();
  }

  public CompletionStage<SelectContextEntriesResponse> select(
      ContextCassStream contextCassStream,
      SelectContextRequest request,
      ContextQueryState queryState,
      ContextOpState state) {
    final var contextDesc = state.getContextDesc();
    state.addHistory("indexQuery{");
    // If onTheFly is specified, we need to get the latest events from the audit signal
    // and apply the filter. For that, we need to know the time up to which the indexes
    // are maintained.
    CompletableFuture<DigestSpecifier> specFuture = CompletableFuture.completedFuture(null);
    if (request.getOnTheFly() == Boolean.TRUE) {
      // Get the time up to which the indexes have been updated.
      specFuture =
          dataEngine
              .getPostProcessScheduler()
              .getDigestionCoverage(contextDesc, streamDesc, state)
              .thenApply(
                  (spec) -> {
                    // Estimated last interval may be used later to take margin time for index
                    // update
                    long interval = streamDesc.getFeatures().get(0).getFeatureInterval();
                    return new DigestSpecifier(
                        spec.getName(),
                        spec.getVersion(),
                        spec.getDoneUntil() - interval,
                        spec.getDoneUntil(),
                        spec.getDoneSince(),
                        spec.getDoneUntil(),
                        spec.getInterval(),
                        spec.getRequested());
                  });
    }

    return specFuture.thenComposeAsync(
        (indexSpec) ->
            cassandraConnection
                // Get results from the context index.
                .executeAsync(new SimpleStatement(makeSelectStatement(request, queryState)), state)
                .thenComposeAsync(
                    (indexResultSet) -> {
                      final var currentTime = System.currentTimeMillis();
                      List<Event> records = getRecordsFromIndex(indexResultSet, state);
                      // Get the latest events from the audit signal since the time indexes were
                      // updated.
                      //
                      // The latest index may have been just created after the initial query.
                      // We'll fetch the last index interval in such a case for safety.
                      // The start time in spec is filled as doneUntil - interval
                      final Range auditTimeRange;
                      if (indexSpec != null) {
                        auditTimeRange = new Range(indexSpec.getStartTime(), currentTime);
                      } else {
                        auditTimeRange = null;
                      }
                      return contextCassStream
                          .getAuditEvents(request, state, auditTimeRange, currentTime)
                          .thenApplyAsync(
                              (auditEvents) ->
                                  applyAuditEventsToRecords(
                                      contextCassStream,
                                      request,
                                      queryState,
                                      state,
                                      auditTimeRange,
                                      currentTime,
                                      auditEvents,
                                      records),
                              state.getExecutor());
                    },
                    state.getExecutor())
                .thenApplyAsync(
                    (records) -> {
                      state.addHistory("(finalProcessing");
                      var out = records;
                      if (queryState.getSortSpec() != null
                          && !queryState.isSortSpecifiedInQuery()) {
                        state.addHistory("(applyOrderBy");
                        out =
                            ExtractView.createSortFunction(queryState.getSortSpec(), this)
                                .apply(records);
                        state.addHistory(")");
                      }
                      if (request.getLimit() != null && out.size() > request.getLimit()) {
                        state.addHistory("(limit");
                        out = out.subList(0, request.getLimit());
                        state.addHistory(")");
                      }
                      state.addHistory(")");
                      SelectContextEntriesResponse resp = entriesToSelectResponse(out);
                      state.addHistory("}");
                      return resp;
                    },
                    state.getExecutor()));
  }

  private List<Event> getRecordsFromIndex(ResultSet indexResultSet, ContextOpState state) {
    Iterator<Row> rowIterator = indexResultSet.iterator();
    List<Event> records = new ArrayList<Event>();
    while (rowIterator.hasNext()) {
      Row row = rowIterator.next();
      final var record = new HashMap<String, Object>();
      for (var attrDesc : streamDesc.getAttributes()) {
        final String attrName = attrDesc.getName();
        final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrName);
        record.put(attrName, row.getObject(cassAttrDesc.getColumn()));
      }
      Event event = new EventJson();
      event.setAttributes(record);
      records.add(event);
      state.addHistory(")");
    }
    return records;
  }

  private List<Event> applyAuditEventsToRecords(
      ContextCassStream contextCassStream,
      SelectContextRequest request,
      ContextQueryState queryState,
      ContextOpState state,
      Range auditTimeRange,
      final long currentTime,
      List<Event> auditEvents,
      List<Event> records) {
    if (auditEvents.isEmpty()) {
      // nothing to do in this method
      return records;
    }
    state.addHistory("(onTheFly");
    logger.debug(
        "onTheFly auditEvents.size={}, initial records.size={}",
        auditEvents.size(),
        records.size());
    // Figure out the effective context entries added / deleted.
    HashMap<String, List<Event>> eventsMap =
        ContextIndexHelper.collectIndexEntries(
            contextCassStream.getStreamDesc(),
            streamDesc.getFeatures().get(0),
            auditTimeRange,
            auditEvents,
            null);
    List<Event> newEvents = eventsMap.get("new");
    List<Event> deletedEvents = eventsMap.get("deleted");

    logger.trace(
        "Received onTheFly query for context {} with {} new events and "
            + "{} deleted events. New events: \n{}\nDeleted events: \n{}\n",
        streamDesc.getName(),
        newEvents.size(),
        deletedEvents.size(),
        newEvents.toString(),
        deletedEvents.toString());

    // TODO(Naoki): Consolidate this into the indexFunction.apply() above. It does something similar
    final var primaryKeysInAudit = new HashSet<List<Object>>();
    newEvents.forEach((event) -> primaryKeysInAudit.add(contextCassStream.makePrimaryKey(event)));
    deletedEvents.forEach(
        (event) -> primaryKeysInAudit.add(contextCassStream.makePrimaryKey(event)));

    // Exclude entries in audit log
    final var outRecords = new ArrayList<Event>();
    records.forEach(
        (event) -> {
          if (!primaryKeysInAudit.contains(contextCassStream.makePrimaryKey(event))) {
            outRecords.add(event);
          }
        });

    // Apply the filter to new events and add those that pass to the list of records.
    String whereClause = request.getWhere();
    if (whereClause != null && !whereClause.isEmpty()) {
      // First build a compiled filter that can be applied to each event.
      final var compiledFilter = new HashMap<String, List<FilterTerm>>();
      List<SingleColumnRelation> filter = queryState.getFilter();
      for (var relation : filter) {
        final var attribute = streamDesc.findAttribute(relation.getEntity().rawText());
        final var name = attribute.getName();
        List<FilterTerm> terms = compiledFilter.get(name);
        if (terms == null) {
          terms = new ArrayList<>();
          compiledFilter.put(name, terms);
        }
        try {
          terms.add(CqlFilterCompiler.createTerm(relation, attribute));
        } catch (InvalidFilterException e) {
          throw new CompletionException(e);
        }
      }
      // Then apply the compiled filter to each event.
      for (var newEvent : newEvents) {
        boolean filterSatisfied = true;
        for (var entry : newEvent.getAttributes().entrySet()) {
          List<FilterTerm> terms = compiledFilter.get(entry.getKey());
          if (terms != null) {
            for (var term : terms) {
              if (!term.test(entry.getValue())) {
                filterSatisfied = false;
                break;
              }
            }
          }
          if (!filterSatisfied) {
            break;
          }
        }
        if (filterSatisfied) {
          outRecords.add(newEvent);
        }
      }
    }
    logger.debug(
        "onTheFly newEvents.size={}, now records.size={}", newEvents.size(), records.size());
    state.addHistory(")");
    return outRecords;
  }

  private SelectContextEntriesResponse entriesToSelectResponse(List<Event> entries) {
    final var response = new SelectContextEntriesResponse();
    final var records = new ArrayList<Map<String, Object>>();
    for (var entry : entries) {
      records.add(entry.getAttributes());
    }
    response.setEntries(records);
    return response;
  }
}
