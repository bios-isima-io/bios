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

import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_COUNT_ATTRIBUTE;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_MAX_SUFFIX;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_MIN_SUFFIX;
import static io.isima.bios.admin.v1.AdminConstants.ROLLUP_STREAM_SUM_SUFFIX;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_EVENT_ID;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_TIME_INDEX;

import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.filter.FilterTerm;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.NotImplementedException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Avg;
import io.isima.bios.models.DistinctCount;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.Max;
import io.isima.bios.models.Min;
import io.isima.bios.models.Range;
import io.isima.bios.models.Sort;
import io.isima.bios.models.Sum;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.View;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.query.CompiledSortRequest;
import io.isima.bios.req.ExtractRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RollupCassStream extends SignalCassStream {
  private static final Logger logger = LoggerFactory.getLogger(RollupCassStream.class);
  private static final int SPAN_IN_MILLIS = 10 * 1000;

  private static final String FORMAT_CREATE_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.%s (%s timestamp, %s timeuuid%s,"
          + " PRIMARY KEY (%s))"
          + " WITH comment = 'tenant=%s (version=%d), rollup=%s (version=%d)'"
          + " AND default_time_to_live = %d"
          + " AND gc_grace_seconds = %d"
          + " AND compaction = %s"
          + " AND bloom_filter_fp_chance = %s";

  /** Group view and aggregates that are used for convoluting rollup data. */
  final View group;

  final List<Aggregate> aggregates;

  protected RollupCassStream(CassTenant cassTenant, StreamDesc streamDesc) {
    super(cassTenant, streamDesc);

    final ViewDesc view = streamDesc.getViews().get(0);
    group = new Group(view.getGroupBy());

    aggregates = new ArrayList<>();

    if (!view.hasGenericDigestion()) {
      aggregates.add(new Sum(ROLLUP_STREAM_COUNT_ATTRIBUTE).as(ROLLUP_STREAM_COUNT_ATTRIBUTE));
      for (String attribute : view.getAttributes()) {
        aggregates.add(
            new Sum(attribute + ROLLUP_STREAM_SUM_SUFFIX).as(attribute + ROLLUP_STREAM_SUM_SUFFIX));
        aggregates.add(
            new Min(attribute + ROLLUP_STREAM_MIN_SUFFIX).as(attribute + ROLLUP_STREAM_MIN_SUFFIX));
        aggregates.add(
            new Max(attribute + ROLLUP_STREAM_MAX_SUFFIX).as(attribute + ROLLUP_STREAM_MAX_SUFFIX));
      }
    }
  }

  @Override
  protected boolean hasTable() {
    return !streamDesc.getViews().get(0).hasGenericDigestion();
  }

  @Override
  public String makeCreateTableStatement(final String keyspaceName) {

    final StringBuilder columnsBuilder = new StringBuilder();

    // add attributes to column
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
            });

    final StringJoiner primaryKeyJoiner =
        new StringJoiner(", ").add(COL_TIME_INDEX).add(COL_EVENT_ID);

    // view dimensions are used as partition keys
    if (streamDesc.getViews() != null) {
      streamDesc
          .getViews()
          .get(0)
          .getGroupBy()
          .forEach(
              dimension -> {
                final CassAttributeDesc cassAttrDesc = getAttributeDesc(dimension);
                primaryKeyJoiner.add(cassAttrDesc.getColumn());
              });
    }

    final String primaryKey = primaryKeyJoiner.toString();

    final var ttl =
        streamDesc.getType() != StreamType.CONTEXT_FEATURE
            ? TfosConfig.featureRecordsDefaultTimeToLive()
            : 0;
    return String.format(
        FORMAT_CREATE_TABLE,
        keyspaceName,
        getTableName(),
        COL_TIME_INDEX,
        COL_EVENT_ID,
        columnsBuilder.toString(), // columns
        primaryKey,
        cassTenant.getName(),
        cassTenant.getVersion(),
        streamDesc.getName(),
        streamDesc.getVersion(),
        ttl,
        TfosConfig.eventsGcGraceSeconds(),
        TfosConfig.eventsCompactionConfig(),
        TfosConfig.eventsBloomFilterFpChance());
  }

  @Override
  public List<QueryInfo> makeExtractStatements(
      QueryExecutionState state,
      final Collection<CassAttributeDesc> attributes,
      final Long startTime,
      final Long endTime)
      throws ApplicationException, TfosException {
    if (startTime == null || endTime == null) {
      throw new IllegalArgumentException("makeExtractQueries parameters may not be null");
    }
    state.addHistory("makeStatements{");
    long left = startTime;
    long right = endTime;
    if (left > right) {
      long temp = left;
      left = right;
      right = temp;
    }

    final StreamDesc rollupStream = getStreamDesc();
    if (rollupStream.getRollupHorizon() != null && rollupStream.getRollupInterval() != null) {
      left -=
          rollupStream.getRollupHorizon().getValueInMillis()
              - rollupStream.getRollupInterval().getValueInMillis();
    }
    return super.makeExtractStatements(state, attributes, left, right);
  }

  public CompletableFuture<QueryInfo> buildLastRolledupEventsQuery(
      long timestamp,
      Collection<CassAttributeDesc> latestAttributes,
      AtomicLong rollupTime,
      ExecutionState state) {
    final StreamDesc rollup = getStreamDesc();
    final long rollupInterval = rollup.getRollupInterval().getValueInMillis();
    final StreamDesc signal = getStreamDesc().getParentStream();
    return dataEngine
        .getPostProcessScheduler()
        .getDigestionCoverage(signal, rollup, state)
        .thenApply(
            (spec) -> {
              final long left = makeLeftBoundary(timestamp, spec.getDoneUntil(), rollupInterval);
              if (left < 0) {
                // the rollup has not happened at all
                return null;
              }
              rollupTime.set(spec.getDoneUntil());

              final long right = left + SPAN_IN_MILLIS * 2;
              final long lastRollupVersion = spec.getVersion();

              var correctRollup = streamDesc;

              // if version is not current version, find correct version
              while ((correctRollup != null)
                  && (correctRollup.getSchemaVersion() > lastRollupVersion)) {
                logger.debug(
                    "correct rollup ver last rollup = {} stream ver = {} schema ver = {}",
                    lastRollupVersion,
                    correctRollup.getVersion(),
                    correctRollup.getSchemaVersion());
                correctRollup = correctRollup.getPrev();
              }

              if (correctRollup == null) {
                // this should not happen
                throw new CompletionException(
                    new ApplicationException(
                        String.format(
                            "Rollup stream Version (=%d) matching last rollup entries not found",
                            lastRollupVersion)));
              }
              final String requiredColumns;
              try {
                requiredColumns = buildRequiredColumns(latestAttributes, correctRollup);
              } catch (ApplicationException e) {
                throw new CompletionException(e);
              }

              long timeIndex = makeTimeIndex(spec.getDoneUntil());
              String condition =
                  COL_TIME_INDEX
                      + " = "
                      + timeIndex
                      + " AND "
                      + COL_EVENT_ID
                      + " >= "
                      + UUIDs.startOf(left)
                      + " AND "
                      + COL_EVENT_ID
                      + " < "
                      + UUIDs.startOf(right);
              return new QueryInfo(
                  String.format(
                      "SELECT %s FROM %s.%s WHERE %s",
                      requiredColumns, keyspaceName, tableName, condition),
                  new Range(left, right),
                  this,
                  latestAttributes);
            });
  }

  public void fillLatestAttributes(List<CassAttributeDesc> attributes) {
    for (String dimension : group.getDimensions()) {
      final CassAttributeDesc attr = getAttributeDesc(dimension);
      attributes.add(attr);
    }
    for (Aggregate aggregate : aggregates) {
      attributes.add(getAttributeDesc(aggregate.getBy()));
    }
  }

  private long makeLeftBoundary(long currentTimestamp, long doneUntil, long rollupInterval) {
    long expectedRollupTime = (currentTimestamp / rollupInterval) * rollupInterval;
    long timeDiff = doneUntil - expectedRollupTime;
    if (expectedRollupTime == doneUntil
        || Math.abs(timeDiff) < rollupInterval + 2 * SPAN_IN_MILLIS) {
      return doneUntil - SPAN_IN_MILLIS;
    } else if (doneUntil >= 0) {
      // rollup has not caught up, warn in the future
      // but get the last rolled up events
      return doneUntil - SPAN_IN_MILLIS;
    } else {
      return -1;
    }
  }

  @Override
  public CompletableFuture<List<QueryInfo>> makeSummarizeStatements(
      SummarizeState state,
      Set<CassAttributeDesc> attributesIgnoreMe,
      long startTime,
      Long endTime) {

    state.addHistory("(via_rollup{");
    logger.debug("USING ROLLUP FOR SUMMARIZATION; rollup={}", getStreamName());

    final SummarizeRequest request = state.getInput();
    final var sortRequest = state.getCompiledSortRequest();

    // rebuild attributes
    final var attributes = new HashSet<CassAttributeDesc>();
    for (String dimension : request.getGroup()) {
      final CassAttributeDesc attr = getAttributeDesc(dimension);
      if (attr == null) {
        return CompletableFuture.failedFuture(
            new ApplicationException(
                String.format(
                    "Group key attribute %s is not found; rollup=%s signal=%s",
                    dimension, getStreamName(), getStreamDesc().getParentStream().getName())));
      }
      attributes.add(attr);
    }
    if (state.getCompiledFilter() != null) {
      final var onlyFilterAttributes = new HashSet<String>();
      for (String entity : state.getCompiledFilter().keySet()) {
        final var attr = getAttributeDesc(entity);
        if (attr == null) {
          return CompletableFuture.failedFuture(
              new ApplicationException(
                  String.format(
                      "Filter entity '%s' is not found in the rollup stream; rollup=%s signal=%s",
                      entity, getStreamName(), getStreamDesc().getParentStream().getName())));
        }
        if (!attributes.contains(attr)) {
          attributes.add(attr);
          onlyFilterAttributes.add(attr.getName());
        }
      }
      state.setOnlyForFilterAttributes(onlyFilterAttributes);
    }
    if (sortRequest != null) {
      final var onlyForSortAttributes = new HashSet<String>();
      if (sortRequest.isKeyAttribute()) {
        final var domain = sortRequest.getAggregate().getBy();
        final var attr = getAttributeDesc(domain);
        if (attr == null) {
          if (request.getAggregates().stream()
              .noneMatch(
                  (aggregate) -> domain.equalsIgnoreCase(aggregate.getOutputAttributeName()))) {
            return CompletableFuture.failedFuture(
                new ApplicationException(
                    String.format(
                        "Order key '%s' is not found in the rollup stream; rollup=%s signal=%s",
                        sortRequest.getAggregate().getBy(),
                        getStreamName(),
                        getStreamDesc().getParentStream().getName())));
          }
        } else {
          if (!attributes.contains(attr)) {
            onlyForSortAttributes.add(attr.getName());
            attributes.add(attr);
          }
        }
      }
    }

    final var allAggregates =
        request.getAggregates().stream()
            .map((aggregate) -> Pair.of(aggregate, Boolean.FALSE))
            .collect(Collectors.toCollection((ArrayList::new)));
    if (sortRequest != null && sortRequest.isKeyAggregate()) {
      allAggregates.add(Pair.of(sortRequest.getAggregate(), Boolean.TRUE));
    }
    for (Pair<Aggregate, Boolean> entry : allAggregates) {
      final var aggregate = entry.getLeft();
      final var isSortKey = entry.getRight();
      final String attribute = aggregate.getBy();
      switch (aggregate.getFunction()) {
        case COUNT:
          attributes.add(getAttributeDesc(ROLLUP_STREAM_COUNT_ATTRIBUTE));
          break;
        case SUM:
          attributes.add(getAttributeDesc(attribute + ROLLUP_STREAM_SUM_SUFFIX));
          break;
        case MIN:
          attributes.add(getAttributeDesc(attribute + ROLLUP_STREAM_MIN_SUFFIX));
          break;
        case MAX:
          attributes.add(getAttributeDesc(attribute + ROLLUP_STREAM_MAX_SUFFIX));
          break;
        case DISTINCTCOUNT:
          {
            final var attr = getAttributeDesc(aggregate.getBy());
            if (attr == null) {
              final var what = isSortKey ? "Order key" : "Dimension";
              return CompletableFuture.failedFuture(
                  new ApplicationException(
                      String.format(
                          "%s '%s' is not found in the rollup stream; rollup=%s signal=%s",
                          what,
                          aggregate.getBy(),
                          getStreamName(),
                          getStreamDesc().getParentStream().getName())));
            }
            attributes.add(attr);
            if (state.getOnlyForFilterAttributes() != null) {
              state.getOnlyForFilterAttributes().remove(attr.getName());
            }
            break;
          }
        case AVG:
          attributes.add(getAttributeDesc(ROLLUP_STREAM_COUNT_ATTRIBUTE));
          attributes.add(getAttributeDesc(attribute + ROLLUP_STREAM_SUM_SUFFIX));
          break;
        default:
          return CompletableFuture.failedFuture(
              new NotImplementedException(
                  String.format(
                      "Metric function %s is not supported yet with windowed query",
                      aggregate.getFunction().name())));
      }
    }

    final StreamDesc rollup = getStreamDesc();
    final long rollupInterval =
        streamDesc.getType() == StreamType.ROLLUP
            ? rollup.getRollupInterval().getValueInMillis()
            : 0;

    final StreamDesc parentStream = getStreamDesc().getParentStream();
    return dataEngine
        .getPostProcessScheduler()
        .getDigestionCoverage(parentStream, rollup, state)
        .thenApply(
            (spec) -> {
              final long firstRollupTime = spec.getDoneSince();
              final long lastRollupTime = spec.getDoneUntil();

              final long queryStart = Math.max(firstRollupTime, startTime);
              final long queryEnd = Math.min(lastRollupTime, endTime) + 1;
              if (queryStart >= queryEnd) {
                return List.of();
              }

              try {
                final List<QueryInfo> queries =
                    makeExtractStatements(
                        state, attributes, queryStart + rollupInterval, queryEnd + rollupInterval);
                return queries;
              } catch (TfosException | ApplicationException e) {
                throw new CompletionException(e);
              }
            });
  }

  @Override
  public Map<Long, List<Event>> summarizeEvents(
      final List<Event> events,
      final SummarizeRequest request,
      CompiledSortRequest sortRequest,
      final CassStream mainCassStream,
      EventFactory eventFactory,
      boolean isBiosApi)
      throws ApplicationException, TfosException {
    final long interval = request.getInterval();
    final long horizon =
        request.getHorizon() != null ? request.getHorizon() : request.getInterval();
    final long offset = request.getTimezone() != null ? request.getTimezone().getRawOffset() : 0;
    final long startTime = request.getStartTime();
    final long earliestCheckpoint =
        (startTime + offset + interval - 1) / interval * interval - offset;
    long right = earliestCheckpoint + interval;
    long left = right - horizon;
    final long shift = getStreamDesc().getRollupInterval().getValueInMillis();
    if (left + shift < 0) {
      throw new ConstraintViolationException("Time window out of range");
    }

    final var requestedAggregates = new ArrayList<>(request.getAggregates());
    if (sortRequest != null && sortRequest.isKeyAggregate()) {
      requestedAggregates.add(sortRequest.getAggregate());
    }
    final List<Aggregate> aggregates = new ArrayList<>();
    for (Aggregate aggregate : requestedAggregates) {
      final String attribute = aggregate.getBy();
      final String as = aggregate.getOutputAttributeName();
      switch (aggregate.getFunction()) {
        case COUNT:
          aggregates.add(new Sum(ROLLUP_STREAM_COUNT_ATTRIBUTE).as(as));
          break;
        case SUM:
          aggregates.add(new Sum(attribute + ROLLUP_STREAM_SUM_SUFFIX).as(as));
          break;
        case MIN:
          aggregates.add(new Min(attribute + ROLLUP_STREAM_MIN_SUFFIX).as(as));
          break;
        case MAX:
          aggregates.add(new Max(attribute + ROLLUP_STREAM_MAX_SUFFIX).as(as));
          break;
        case DISTINCTCOUNT:
          aggregates.add(new DistinctCount(attribute).as(as));
          break;
        case AVG:
          aggregates.add(
              new Avg(attribute + ROLLUP_STREAM_SUM_SUFFIX)
                  .as(as)
                  .countAttribute(ROLLUP_STREAM_COUNT_ATTRIBUTE));
          break;
        default:
          throw new ApplicationException(
              "Unknown aggregate function: " + aggregate.getFunction().name());
      }
    }

    return summarizeEventsCore(events, request, aggregates, shift, this, eventFactory, isBiosApi);
  }

  /**
   * Method used to convolute sequence of rollup intervals to handle horizon longer than intervals.
   *
   * <p>The method assumes the length of the horizon is multiplication of the interval.
   *
   * @param state Extract execution state.
   * @param events Input rollup sequence.
   * @return List of convoluted rollup sequcne.
   * @throws ConstraintViolationException When the request is invalid.
   * @throws ApplicationException When unexpected problem happens.
   */
  public List<Event> convolute(ExtractState state, List<Event> events) throws ApplicationException {
    final ExtractRequest request = state.getInput();
    final StreamDesc streamDesc = getStreamDesc();
    final long interval = streamDesc.getRollupInterval().getValueInMillis();
    final long horizon = streamDesc.getRollupHorizon().getValueInMillis();
    final long startTime = request.getStartTime();

    if (interval == horizon || events.size() <= 1) {
      // no need to convolute
      return events;
    }
    final long firstTimestamp = events.get(0).getIngestTimestamp().getTime();
    final long start = Math.max(startTime, firstTimestamp);
    long right = (start + interval - 1) / interval * interval;
    long left = right - horizon;
    List<Event> out = new ArrayList<>();
    int ileft = 0;
    int iright = 0;
    while (iright < events.size()) {
      final Event current = events.get(iright);
      while (current.getIngestTimestamp().getTime() > right) {
        combine(events, ileft, iright, right, out, state.getEventFactory());
        right += interval;
        left += interval;
        for (; ileft < iright; ++ileft) {
          final Event toRemove = events.get(ileft);
          if (toRemove.getIngestTimestamp().getTime() > left) {
            break;
          }
        }
      }
      ++iright;
    }
    combine(events, ileft, iright, right, out, state.getEventFactory());
    return out;
  }

  private void combine(
      List<Event> events,
      int ileft,
      int iright,
      long timestamp,
      List<Event> out,
      EventFactory eventFactory)
      throws ApplicationException {

    Function<List<Event>, List<Event>> groupFunction =
        ExtractView.createView(
            group, Collections.emptyList(), aggregates, this, eventFactory::create);

    List<Event> src = new ArrayList<>();
    events.subList(ileft, iright).forEach(event -> src.add(eventFactory.create(event)));
    List<Event> temp = groupFunction.apply(src);
    final Date ingestTimestamp = new Date(timestamp);
    final UUID eventId = UUIDs.startOf(timestamp);
    for (Event event : temp) {
      out.add(event.setEventId(eventId).setIngestTimestamp(ingestTimestamp));
    }
  }

  public CompletionStage<List<Map<String, Object>>> extractFeatureRecords(
      SelectContextRequest request,
      List<Aggregate> aggregates,
      Map<String, List<FilterTerm>> compiledFilter,
      Sort sortSpec,
      ContextOpState state) {

    final long startTime = 0L;
    final long endTime = Long.MAX_VALUE;

    final var summarizeRequest = new SummarizeRequest();
    summarizeRequest.setStartTime(startTime);
    summarizeRequest.setEndTime(endTime);
    summarizeRequest.setInterval(endTime - startTime);
    summarizeRequest.setAggregates(aggregates);
    summarizeRequest.setGroup(request.getGroupBy() != null ? request.getGroupBy() : List.of());
    summarizeRequest.setFilter(request.getWhere());
    final var summarizeState =
        new SummarizeState(
            "extract feature records",
            getTenantName(),
            getStreamName(),
            EventFactory.DEFAULT_FACTORY,
            state);
    summarizeState.setInput(summarizeRequest);
    summarizeState.setCompiledFilter(compiledFilter);

    // Make the final grouping function
    final var finalAggregates =
        summarizeRequest.getAggregates().stream()
            .map(
                (aggregate) -> {
                  final var alias = aggregate.getOutputAttributeName();
                  switch (aggregate.getFunction()) {
                    case COUNT:
                      return new Sum(ROLLUP_STREAM_COUNT_ATTRIBUTE).as(alias);
                    case SUM:
                      return new Sum(aggregate.getBy() + ROLLUP_STREAM_SUM_SUFFIX).as(alias);
                    case MIN:
                      return new Min(aggregate.getBy() + ROLLUP_STREAM_MIN_SUFFIX).as(alias);
                    case MAX:
                      return new Max(aggregate.getBy() + ROLLUP_STREAM_MAX_SUFFIX).as(alias);
                    case DISTINCTCOUNT:
                      return new DistinctCount(aggregate.getBy()).as(alias);
                    case AVG:
                      return new Avg(aggregate.getBy() + ROLLUP_STREAM_SUM_SUFFIX)
                          .countAttribute(ROLLUP_STREAM_COUNT_ATTRIBUTE)
                          .as(alias);
                    default:
                      // should not happen
                      throw new UnsupportedOperationException(
                          "Feature function " + aggregate.getFunction());
                  }
                })
            .collect(Collectors.toList());
    final Function<List<Event>, List<Event>> groupingFunction;
    try {
      groupingFunction =
          ExtractView.createView(
              new Group(summarizeRequest.getGroup()),
              List.of(),
              finalAggregates,
              this,
              EventJson::new);
    } catch (ApplicationException e) {
      throw new CompletionException(e);
    }

    final Function<List<Event>, List<Event>> sortFunction =
        ExtractView.createSortFunction(sortSpec, this);

    return makeSummarizeStatements(summarizeState, null, startTime, endTime)
        .thenComposeAsync(
            (queryInfos) -> {
              final var statement = queryInfos.get(0).getStatement();
              final var attributes = queryInfos.get(0).getAttributes();
              return cassandraConnection
                  .executeAsync(statement, state)
                  .thenApplyAsync(
                      (resultSet) -> {
                        final var events = new ArrayList<Event>();
                        while (!resultSet.isExhausted()) {
                          final var row = resultSet.one();
                          try {
                            final var event =
                                buildEvent(
                                    row,
                                    attributes,
                                    true,
                                    EventFactory.DEFAULT_FACTORY,
                                    compiledFilter,
                                    summarizeState.getOnlyForFilterAttributes());
                            if (event != null) {
                              events.add(event);
                            }
                          } catch (ApplicationException e) {
                            throw new CompletionException(e);
                          }
                        }
                        var outEvents = sortFunction.apply(groupingFunction.apply(events));
                        return outEvents.stream()
                            .map((event) -> event.getAttributes())
                            .collect(Collectors.toList());
                      },
                      state.getExecutor());
            });
  }

  public CompletionStage<Void> clearTimeIndex(long timestamp, ExecutionState state) {
    final long timeIndex = makeTimeIndex(timestamp);
    final var statement =
        new SimpleStatement(
            String.format(
                "DELETE FROM %s.%s where time_index = %d",
                getKeyspaceName(), getTableName(), timeIndex));
    return cassandraConnection.executeAsync(statement, state).thenAccept((result) -> {});
  }
}
