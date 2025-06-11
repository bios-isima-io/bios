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

import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_TIME_INDEX;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.errors.exception.FilterNotApplicableException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.models.Event;
import io.isima.bios.models.Range;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.ViewDesc;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexCassStream extends SignalCassStream {
  private static final Logger logger = LoggerFactory.getLogger(IndexCassStream.class);

  private String queryColumns;

  private PreparedStatement insertStatement;

  // Omitting CLUSTERING option; All clustering keys to be sorted in ascending order
  private static final String FORMAT_CREATE_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.%s (%s timestamp%s,"
          + " PRIMARY KEY (%s))"
          + " WITH comment = 'tenant=%s (version=%d), index=%s (version=%d)'"
          + " AND default_time_to_live = %d"
          + " AND gc_grace_seconds = %d"
          + " AND compaction = %s"
          + " AND bloom_filter_fp_chance = %s";

  protected IndexCassStream(CassTenant cassTenant, StreamDesc streamDesc) {
    super(cassTenant, streamDesc);
  }

  @Override
  public String makeCreateTableStatement(final String keyspaceName) {

    final StringBuilder columnsBuilder = new StringBuilder();

    final StringJoiner primaryKeyJoiner = new StringJoiner(", ").add(COL_TIME_INDEX);

    // add groupBy to column and to primary key
    streamDesc
        .getAttributes()
        .forEach(
            (attrDesc) -> {
              final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());
              columnsBuilder
                  .append(", ")
                  .append(cassAttrDesc.getColumn())
                  .append(" ")
                  .append(valueTypeToCassDataTypeName(cassAttrDesc.getAttributeType()));

              primaryKeyJoiner.add(cassAttrDesc.getColumn());
            });

    final String primaryKey = primaryKeyJoiner.toString();
    queryColumns = primaryKey;

    return String.format(
        FORMAT_CREATE_TABLE,
        keyspaceName,
        getTableName(),
        COL_TIME_INDEX,
        columnsBuilder.toString(),
        primaryKey,
        cassTenant.getName(),
        cassTenant.getVersion(),
        streamDesc.getName(),
        streamDesc.getVersion(),
        TfosConfig.featureRecordsDefaultTimeToLive(),
        TfosConfig.eventsGcGraceSeconds(),
        TfosConfig.eventsCompactionConfig(),
        TfosConfig.eventsBloomFilterFpChance());
  }

  @Override
  protected String generateIngestQueryString(final String keyspaceName) {
    final StringBuilder statementBuilder =
        new StringBuilder(
            String.format("INSERT INTO %s.%s (%s", keyspaceName, tableName, COL_TIME_INDEX));
    final StringBuilder valuesPartBuilder = new StringBuilder(") VALUES (?");

    streamDesc
        .getAttributes()
        .forEach(
            (attrDesc) -> {
              final CassAttributeDesc cassAttrDesc = getAttributeDesc(attrDesc.getName());
              statementBuilder.append(", ").append(cassAttrDesc.getColumn());
              valuesPartBuilder.append(", ?");
            });

    statementBuilder.append(valuesPartBuilder).append(")");
    return statementBuilder.toString();
  }

  @Override
  public Statement makeInsertStatement(final Event event) throws ApplicationException {
    // reserved column is 'time_index'
    final int numReservedColumns = 1;
    final int numAttributes = listSize(streamDesc.getAttributes());

    final Object[] values = new Object[numReservedColumns + numAttributes];
    int index = 0;
    values[index++] = makeTimeIndex(event.getEventId());
    if (numAttributes > 0) {
      index = populateIngestValues(index, event, streamDesc.getAttributes(), values);
    }

    final PreparedStatement prepared = getPreparedIngest();
    if (prepared != null) {
      return prepared.bind(values);
    } else {
      return new SimpleStatement(getIngestQueryString(), values);
    }
  }

  /**
   * Method to make an insert statement from Cassandra session and values.
   *
   * @param session Cassandra session
   * @param values Values to be bound to the statement. Order of the values must be time_index
   *     followed by defined attributes (order is seen in streamConfig.getAttributes()).
   * @return Insert statement that is ready to execute.
   */
  // TODO(TFOS-1080): session is unnecessary if the constructor takes the session.
  public Statement makeInsertStatement(Session session, Object[] values) {
    if (insertStatement == null) {
      insertStatement = session.prepare(generateIngestQueryString(cassTenant.getKeyspaceName()));
    }
    return insertStatement.bind(values);
  }

  @Override
  public List<QueryInfo> makeExtractStatements(
      QueryExecutionState state,
      Collection<CassAttributeDesc> attributes,
      final Long startTime,
      final Long endTime)
      throws InvalidFilterException, FilterNotApplicableException {
    if (startTime == null || endTime == null) {
      throw new IllegalArgumentException("makeExtractQueries parameters may not be null");
    }
    long start = startTime;
    long end = endTime;
    if (startTime > endTime) {
      start = endTime;
      end = startTime;
    }

    // We don't use input columns since index query must fetch all columns.
    List<QueryInfo> queries = new ArrayList<>();
    long timeIndex = makeTimeIndex(start);
    final long right = end;

    final String filter;
    if (state.getFilter() != null) {
      final String interpreted = interpretFilter(state.getFilter());
      if (interpreted == null) {
        // never matches, no statements to return
        return queries;
      }
      filter = " AND " + interpreted + " ALLOW FILTERING";
    } else {
      filter = "";
    }

    while (timeIndex < right) {
      final String statement =
          String.format(
              "SELECT %s FROM %s.%s WHERE %s = %d%s",
              queryColumns, keyspaceName, tableName, COL_TIME_INDEX, timeIndex, filter);
      logger.debug("From index={} extract query statement {}", streamDesc.getName(), statement);
      queries.add(
          new QueryInfo(
              statement,
              new Range(timeIndex, Math.min(timeIndex + timeIndexWindowWidth, right)),
              this,
              attributes));
      timeIndex += timeIndexWindowWidth;
    }
    return queries;
  }

  /**
   * Method to generate statements for the first step of index-based summarize operation.
   *
   * <p>The methodology to generate the statement is the same with extract operation, but the time
   * range has to be adjusted for the summarize operation. The statement will look for index entries
   * for the time indexes in the specified time range.
   */
  @Override
  public CompletableFuture<List<QueryInfo>> makeSummarizeStatements(
      SummarizeState state, Set<CassAttributeDesc> attributes, long startTime, Long endTime) {

    state.addHistory("via_index");
    logger.debug("USING INDEX FOR SUMMARIZATION; index={}", getStreamName());

    final SummarizeRequest request = state.getInput();

    final StreamDesc indexDesc = getStreamDesc();
    final StreamDesc signalDesc = getStreamDesc().getParentStream();

    // An index stream always have only one corresponding view
    final ViewDesc view = indexDesc.getViews().get(0);
    final String viewStreamName = AdminUtils.makeViewStreamName(signalDesc, view);
    final StreamDesc viewDesc = signalDesc.getParent().getStream(viewStreamName);
    return dataEngine
        .getPostProcessScheduler()
        .getDigestionCoverage(signalDesc, viewDesc, state)
        .thenApply(
            (spec) ->
                ExecutionHelper.supply(
                    () -> {
                      final long firstRollupTime = spec.getDoneSince();
                      final long lastRollupTime = spec.getDoneUntil();

                      // final long interval = request.getInterval();

                      final long queryStart = Math.max(firstRollupTime, startTime);
                      final long queryEnd = Math.min(lastRollupTime, endTime) + 1;

                      final List<QueryInfo> queries =
                          makeExtractStatements(state, attributes, queryStart, queryEnd);
                      return queries;
                    }));
  }

  /**
   * Filter validater and interpreter for the index query.
   *
   * <p>The fundamental logic is the same with the super class. Differences are:
   *
   * <ul>
   *   <li>The method runs additional validation to make sure the filter matches the index
   *       configuration properly. If not, the method throws {@link FilterNotApplicableException}.
   *   <li>Interpreted filter string does not have keyword "ALLOW FILTERING"
   * </ul>
   */
  @Override
  protected String traverseFilter(List<SingleColumnRelation> relations, boolean doInterpret)
      throws InvalidFilterException, FilterNotApplicableException {
    final StringBuilder sb = doInterpret ? new StringBuilder() : null;
    final Map<String, Boolean> targetAttributes = new HashMap<>();
    validateRelations(relations, sb, targetAttributes);

    // scan the attributes to verify filter constraint
    String preceding = null;
    Boolean wasPrevEquality = Boolean.TRUE;
    for (int i = 0; i < getStreamDesc().getAttributes().size(); ++i) {
      final AttributeDesc attr = getStreamDesc().getAttributes().get(i);
      final String attrName = attr.getName();
      Boolean isEquality = targetAttributes.get(attrName);
      if (isEquality == null) {
        wasPrevEquality = null;
        preceding = attrName;
        continue;
      }
      if (wasPrevEquality == null) {
        throw new FilterNotApplicableException(
            String.format(
                "Attribute %s cannot be restricted (preceding attribute '%s' is not restricted)",
                attrName, preceding));
      }
      if (!wasPrevEquality) {
        throw new FilterNotApplicableException(
            String.format(
                "Attribute %s cannot be restricted"
                    + " (preceding attribute '%s' is restricted by a non-EQ relation)",
                attrName, preceding));
      }
      wasPrevEquality = isEquality;
      preceding = attrName;
    }

    if (doInterpret) {
      final String output = sb.toString();
      logger.debug("FILTER={}", output);
      return output;
    }
    return null;
  }
}
