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

import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_EVENT_ID;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_TIME_INDEX;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.StreamConversion;
import io.isima.bios.admin.v1.StreamConversion.AttributeConversion;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.common.TfosUtils;
import io.isima.bios.data.filter.FilterTerm;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.data.impl.maintenance.DigestSpecifier;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.NoFeatureFoundException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.models.Event;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.Range;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.View;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.query.CompiledSortRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.cql3.SingleColumnRelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalCassStream extends CassStream {
  private static final Logger logger = LoggerFactory.getLogger(SignalCassStream.class);

  private static final String FORMAT_CREATE_SIGNAL_TABLE =
      "CREATE TABLE IF NOT EXISTS %s.%s (%s timestamp, %s timeuuid,%s"
          + " PRIMARY KEY (%s, %s)) WITH CLUSTERING ORDER BY (%s ASC)"
          + " AND comment = 'tenant=%s (version=%d), signal=%s (version=%d, schemaVersion=%s)'"
          + " AND default_time_to_live = %d"
          + " AND gc_grace_seconds = %d"
          + " AND compaction = %s"
          + " AND bloom_filter_fp_chance = %s";

  /** Time ranges will have 2 minute overlap across versions. */
  public static final long TIME_RANGE_OVERLAP_MILLIS = 10L * 1000;

  /** Time index window width in milliseconds. */
  protected final long timeIndexWindowWidth;

  public SignalCassStream(CassTenant cassTenant, StreamDesc streamDesc) {
    super(cassTenant, streamDesc);
    final Long stored = streamDesc.getIndexWindowLength();
    timeIndexWindowWidth =
        stored != null ? stored : TfosConfig.indexingDefaultIntervalSeconds() * 1000L;
  }

  @Override
  public String makeCreateTableStatement(final String keyspaceName) {

    final StringBuilder columnsBuilder = new StringBuilder();

    attributeTable.forEach(
        (attr, desc) -> {
          columnsBuilder
              .append(" ")
              .append(desc.getColumn())
              .append(" ")
              .append(valueTypeToCassDataTypeName(desc.getAttributeType()))
              .append(",");
        });

    return String.format(
        FORMAT_CREATE_SIGNAL_TABLE,
        keyspaceName,
        getTableName(),
        COL_TIME_INDEX,
        COL_EVENT_ID,
        columnsBuilder,
        COL_TIME_INDEX,
        COL_EVENT_ID,
        COL_EVENT_ID,
        cassTenant.getName(),
        cassTenant.getVersion(),
        streamDesc.getName(),
        streamDesc.getVersion(),
        streamDesc.getSchemaVersion(),
        getDefaultTimeToLive(),
        TfosConfig.eventsGcGraceSeconds(),
        TfosConfig.eventsCompactionConfig(),
        TfosConfig.eventsBloomFilterFpChance());
  }

  protected int getDefaultTimeToLive() {
    return TfosConfig.signalDefaultTimeToLive();
  }

  @Override
  protected String generateIngestQueryString(final String keyspaceName) {
    final StringBuilder statementBuilder =
        new StringBuilder(
            String.format(
                "INSERT INTO %s.%s (%s, %s",
                keyspaceName, tableName, COL_TIME_INDEX, COL_EVENT_ID));
    final StringBuilder valuesPartBuilder = new StringBuilder(") VALUES (?, ?");

    attributeTable.forEach(
        (attr, desc) -> {
          statementBuilder.append(", ").append(desc.getColumn());
          valuesPartBuilder.append(", ?");
        });

    statementBuilder.append(valuesPartBuilder).append(")");
    return statementBuilder.toString();
  }

  @Override
  public Statement makeInsertStatement(final Event event) throws ApplicationException {
    // reserved columns are 'time_index' and 'event_id'
    final int numReservedColumns = 2;
    final int numAttributes = listSize(streamDesc.getAttributes());
    final int numAdditionalAttributes = listSize(streamDesc.getAdditionalAttributes());

    final Object[] values =
        new Object[numReservedColumns + numAttributes + numAdditionalAttributes];
    int index = 0;
    final Long timeIndex;
    if (streamDesc.getType() == StreamType.CONTEXT_FEATURE) {
      timeIndex = makeTimeIndex(event.getIngestTimestamp().getTime());
    } else {
      timeIndex = makeTimeIndex(event.getEventId());
    }
    values[index++] = timeIndex;
    values[index++] = event.getEventId();
    if (numAttributes > 0) {
      index = populateIngestValues(index, event, streamDesc.getAttributes(), values);
    }
    if (numAdditionalAttributes > 0) {
      populateIngestValues(index, event, streamDesc.getAdditionalAttributes(), values);
    }

    final PreparedStatement prepared = getPreparedIngest();
    if (prepared != null) {
      return prepared.bind(values);
    } else {
      return new SimpleStatement(getIngestQueryString(), values);
    }
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

    final List<QueryInfo> queries = new ArrayList<>();

    makeExtractStatementsInternal(state, attributes, left, right, queries);
    state.addHistory("}");
    return queries;
  }

  private void makeExtractStatementsInternal(
      QueryExecutionState state,
      final Collection<CassAttributeDesc> attributes,
      long left,
      long right,
      final List<QueryInfo> queries)
      throws ApplicationException, TfosException {

    final var filter = state.getFilter();
    state.addHistory(String.format("{v%d(%d-%d)", getStreamVersion(), left, right));

    // make statements for older versions if available and necessary
    if (streamDesc.getPrev() != null) {
      final long leftBoundary = getSchemaVersion() + TIME_RANGE_OVERLAP_MILLIS;
      if (left < leftBoundary) {
        final SignalCassStream prev = cassTenant.getCassStream(streamDesc.getPrev());
        if (prev != null) {
          prev.makeExtractStatementsInternal(
              state, attributes, left, Math.min(leftBoundary, right), queries);
        }
      }

      // move left edge if necessary
      left = Math.max(left, getSchemaVersion() - TIME_RANGE_OVERLAP_MILLIS);
    }
    final String requiredColumns = buildRequiredColumns(attributes, streamDesc);

    final String filterString;
    if (filter != null) {
      filterString = interpretFilter(filter);
      if (filterString == null) {
        // filter never matches, generate no statements
        state.addHistory("}");
        return;
      }
    } else {
      filterString = null;
    }

    long timeIndex = makeTimeIndex(left);
    while (left < right) {
      StringBuilder condition = new StringBuilder(COL_TIME_INDEX).append(" = ").append(timeIndex);
      long queryLeft = timeIndex;
      if (left > timeIndex) {
        condition.append(" AND ").append(COL_EVENT_ID).append(" >= ").append(UUIDs.startOf(left));
        queryLeft = left;
      }
      timeIndex += timeIndexWindowWidth;
      long queryRight = timeIndex;
      if (right < timeIndex) {
        condition.append(" AND ").append(COL_EVENT_ID).append(" < ").append(UUIDs.startOf(right));
        queryRight = right;
      }
      left = timeIndex;
      if (filterString != null && !filterString.isEmpty()) {
        condition.append(" AND ").append(filterString).append(" ALLOW FILTERING");
      }
      queries.add(
          new QueryInfo(
              String.format(
                  "SELECT %s FROM %s.%s WHERE %s",
                  requiredColumns, keyspaceName, tableName, condition),
              new Range(queryLeft, queryRight),
              this,
              attributes));
    }
    state.addHistory("}");
  }

  String buildRequiredColumns(Collection<CassAttributeDesc> attributes, StreamDesc streamDesc)
      throws ApplicationException {
    final List<String> columns = new ArrayList<>();
    columns.add(COL_EVENT_ID);
    if (streamDesc.getStreamConversion() == null) {
      for (CassAttributeDesc attr : attributes) {
        columns.add(attr.getColumn());
      }
    } else {
      for (CassAttributeDesc attr : attributes) {
        final AttributeConversion conv =
            streamDesc.getStreamConversion().getAttributeConversion(attr.getName());
        if (conv == null) {
          throw new ApplicationException("attribute conversion is missing for " + attr.getName());
        }
        switch (conv.getConversionType()) {
          case NO_CHANGE:
          case CONVERT:
            // add the column
            columns.add(attr.getColumn());
            break;
          case ADD:
          default:
            // do nothing here. fulfill default value later
        }
      }
    }
    return String.join(",", columns);
  }

  /**
   * SignalCassStream specific method that is used for making query info entries for summarize
   * operation.
   *
   * @param state Summarize state
   * @param attributes Set of attributes to be collected. The attributes may be overwritten by the
   *     method.
   * @param startTime Query start time
   * @param endTime Query end time
   * @return List of query info entries
   * @throws ApplicationException When an unexpected error happens.
   * @throws TfosException When user error happens.
   */
  public CompletableFuture<List<QueryInfo>> makeSummarizeStatements(
      SummarizeState state, Set<CassAttributeDesc> attributes, long startTime, Long endTime) {

    return findEffectiveTable(state, startTime, endTime)
        .thenCompose(
            (effectiveCassStream) ->
                ExecutionHelper.supply(
                    () -> {
                      if (effectiveCassStream != null) {
                        return effectiveCassStream.makeSummarizeStatements(
                            state, attributes, startTime, endTime);
                      }
                      return CompletableFuture.completedFuture(
                          makeExtractStatements(state, attributes, startTime, endTime));
                    }));
  }

  private CompletableFuture<Double> getDoneCoverage(
      StreamDesc rollupStreamDesc, ExecutionState state) {
    var rollupInterval = rollupStreamDesc.getRollupInterval().getValueInMillis();
    var signal = rollupStreamDesc.getParentStream();
    return dataEngine
        .getPostProcessScheduler()
        .getDigestionCoverage(signal, rollupStreamDesc, state)
        .thenApply(
            (spec) -> {
              var endpoint = spec.getDoneUntil() + rollupInterval;
              var signalOrigin = signal.getPostProcessOrigin();
              if (endpoint == signalOrigin) {
                return 100.0;
              }
              return 100.0 * (endpoint - spec.getDoneSince()) / (endpoint - signalOrigin);
            });
  }

  /**
   * Method called by {@link #makeSummarizeStatements} to find any rollup stream usable for the
   * operation.
   *
   * <p>The method looks for a rollup stream that match following conditions:
   *
   * <ul>
   *   <li>Includes all specified group dimensions. Zero dimension group matches any rollup stream.
   *   <li>Includes all attributes required by the query
   *   <li>Rollup is done for the specified time range
   * </ul>
   *
   * <p>If multiple rollup streams match, the closest one to the query is selected.
   *
   * @param state Summarize state
   * @param startTime Beginning of the query time range
   * @param endTime End of the query time range
   * @return Tuple of rollup stream and the latest time that this method has covered. If the
   *     timestamp is earlier than query end time, the later procedure should query the rest via a
   *     regular signal table.
   * @throws ApplicationException When an unexpected error happens.
   */
  public CompletableFuture<SignalCassStream> findEffectiveTable(
      SummarizeState state, long startTime, Long endTime) {

    final StreamDesc signal = getStreamDesc();
    final var filter = state.getFilter();

    final SummarizeRequest request = state.getInput();

    final Set<String> requiredDimensions = new HashSet<>();
    request.getGroup().forEach(dimension -> requiredDimensions.add(dimension.toLowerCase()));

    final Set<String> requiredAttributes = new HashSet<>();
    request
        .getAggregates()
        .forEach(
            aggregate -> {
              final var function = aggregate.getFunction();
              if (function != MetricFunction.COUNT) {
                if (function == MetricFunction.DISTINCTCOUNT) {
                  requiredDimensions.add(aggregate.getBy().toLowerCase());
                } else {
                  requiredAttributes.add(aggregate.getBy().toLowerCase());
                }
              }
            });

    final var sortRequest = state.getCompiledSortRequest();
    if (sortRequest != null) {
      final var domain = sortRequest.getAggregate().getBy();
      if (sortRequest.isKeyAttribute()) {
        requiredDimensions.add(domain.toLowerCase());
      } else if (!domain.isBlank()) {
        requiredAttributes.add(domain.toLowerCase());
      }
    }

    final long interval = request.getInterval();
    final long horizon = request.getHorizon();

    final var candidates = new ArrayList<FindEffectiveTableState>();
    for (String subStreamName : signal.getSubStreamNames()) {
      // Check the substream type and skip if it is not of type rollup
      final CassStream subStream = cassTenant.getCassStream(subStreamName, getStreamVersion());
      if (subStream == null) {
        return CompletableFuture.failedFuture(
            new ApplicationException(
                String.format(
                    "substream not found: name=%s version=%d", subStreamName, getStreamVersion())));
      }
      final StreamDesc rollupStreamDesc = subStream.getStreamDesc();
      if (rollupStreamDesc.getType() != StreamType.ROLLUP
          || rollupStreamDesc.getViews().get(0).hasGenericDigestion()) {
        continue;
      }
      final RollupCassStream rollupCassStream = (RollupCassStream) subStream;
      // Check if the requested interval is multiple of the substream's interval
      final int streamInterval = rollupStreamDesc.getRollupInterval().getValueInMillis();
      final int streamHorizon = rollupStreamDesc.getRollupHorizon().getValueInMillis();
      if (rollupStreamDesc.getFormatVersion() < FormatVersion.REUSABLE_ROLLUP
          && streamInterval != streamHorizon
          && (interval != streamInterval || horizon != streamHorizon)) {
        continue;
      }
      if (interval % streamInterval != 0) {
        continue;
      }
      // BIOS API specific
      if (state.isBiosApi() && startTime % streamInterval != 0) {
        continue;
      }

      final long convolutionFactor =
          horizon / rollupStreamDesc.getRollupHorizon().getValueInMillis();
      final ViewDesc viewDesc = rollupCassStream.getStreamDesc().getViews().get(0);
      final List<String> providedDimensions = viewDesc.getGroupBy();
      final List<String> providedAttributes = viewDesc.getAttributes();
      if (providedDimensions.size() < requiredDimensions.size()
          || providedAttributes.size() < requiredAttributes.size()) {
        continue;
      }
      // Check whether the rollup stream covers all required attributes.
      int attributeCount = 0;
      if (!requiredAttributes.isEmpty()) {
        for (String attribute : providedAttributes) {
          if (requiredAttributes.contains(attribute.toLowerCase())
              && ++attributeCount == requiredAttributes.size()) {
            break;
          }
        }
        if (attributeCount < requiredAttributes.size()) {
          continue;
        }
      }
      // Check wither the rollup stream covers all required group dimensions.
      int dimensionsCount = 0;
      if (!requiredDimensions.isEmpty()) {
        for (String dimension : providedDimensions) {
          if (requiredDimensions.contains(dimension.toLowerCase())
              && ++dimensionsCount == requiredDimensions.size()) {
            break;
          }
        }
        if (dimensionsCount < requiredDimensions.size()) {
          continue;
        }
      }

      SignalCassStream cassIndex = null;
      if (filter != null) {
        // Try finding the best filtering approach.
        try {
          rollupCassStream.validateFilter(filter);
        } catch (TfosException e) {
          continue;
        }
      }
      candidates.add(
          new FindEffectiveTableState(
              subStreamName,
              rollupStreamDesc,
              rollupCassStream,
              cassIndex,
              convolutionFactor,
              state));
    }

    final CompletableFuture<Void> resultFuture;
    if (candidates.isEmpty()) {
      resultFuture = CompletableFuture.completedFuture(null);
    } else {
      final var allFutures = new CompletableFuture[candidates.size() * 2];
      for (int i = 0; i < candidates.size(); ++i) {
        // This rollup or index stream can be used for the request if we reach this point.
        final var childState = candidates.get(i);
        final var rollupStreamDesc = childState.getRollupStreamDesc();
        allFutures[i * 2] =
            getDoneCoverage(rollupStreamDesc, childState)
                .thenAccept((coverage) -> childState.setCoverage(coverage));
        allFutures[i * 2 + 1] =
            dataEngine
                .getPostProcessScheduler()
                .getDigestionCoverage(signal, rollupStreamDesc, childState)
                .thenAccept((spec) -> childState.setSpec(spec));
      }
      resultFuture = CompletableFuture.allOf(allFutures);
    }

    return resultFuture.thenApply(
        (none) -> {
          SignalCassStream result = null;
          long smallestMeasure = Long.MAX_VALUE;
          for (var childState : candidates) {
            final var rollupCassStream = childState.getRollupCassStream();
            final long measure =
                childState.getProvidedDimensions().size() * childState.getConvolutionFactor();
            if (measure < smallestMeasure) {
              result =
                  childState.getCassIndex() != null ? childState.getCassIndex() : rollupCassStream;
              smallestMeasure = measure;
            }
            logger.debug(
                "for stream {} spec coverage is {} {} and required time is {} {} and coverage is {}",
                rollupCassStream.getStreamName(),
                childState.getSpec().getDoneSince(),
                childState.getSpec().getDoneUntil(),
                startTime,
                endTime,
                childState.getCoverage());
            /* BIOS-1490 Initially logic was to select the feature with minimum number of attributes
             * that can satisfy this query. If someone added a new feature with just one attribute
             * even though the feature is newly added and rollup is not complete, that feature would be
             * returned, hence this optimization would not work. We want to find the feature that
             * is completely rolled up or the minimum attributes, which would complete in short while.
             */
            if (childState.getCoverage() >= 99.9 && childState.getSpec().getDoneUntil() > endTime) {
              break;
            }
          }

          if (result == null) {
            throw new CompletionException(
                new NoFeatureFoundException(
                    "No suitable features found for the specified windowed query"));
          }

          // For rollup table, we fetch all data from DB and apply the filter as post process. In
          // order
          // to do so, we compile the filter here and clear the input filter in the execution state
          // so
          // that statement builder won't append the filter to query statements at the following
          // statement building stage.
          if (result instanceof RollupCassStream && filter != null) {
            Map<String, List<FilterTerm>> compiledFilter = new HashMap<>();
            for (var relation : filter) {
              final var attribute = result.getAttributeDesc(relation.getEntity().rawText());
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
            state.setCompiledFilter(compiledFilter);
            state.setFilter(null);
          }

          logger.debug("selected stream is {}", result == null ? "none" : result.getStreamName());

          return result;
        });
  }

  /**
   * Builds an event from a Cassandra row and an attributes specifier.
   *
   * @param row Cassandra row to process
   * @param attributes List of attributes to populate.
   * @param noEventIdColumn Flat to indicate that the method should not put event ID to the event.
   * @param eventFactory Event generator.
   * @param compiledFilter Compiled filter to post process. The parameter should be null if no
   *     filter should be applied.
   * @param onlyForFilterAttributes Set of attribute names that are used only for post-process
   *     filtering. These attributes are not included in the built event.
   * @return The built event or null in case the filter is specified and it does not match the
   *     condition. The parameter is ignored when null is specified.
   * @throws ApplicationException Thrown to indicate that an unexpected error happened.
   */
  public Event buildEvent(
      Row row,
      Collection<CassAttributeDesc> attributes,
      boolean noEventIdColumn,
      EventFactory eventFactory,
      Map<String, List<FilterTerm>> compiledFilter,
      Set<String> onlyForFilterAttributes)
      throws ApplicationException {
    final Event event = eventFactory.create();
    // Index table does not have the column for event ID.
    if (!noEventIdColumn) {
      event.setEventId(row.getUUID(COL_EVENT_ID));
      event.setIngestTimestamp(new Date(UUIDs.unixTimestamp(event.getEventId())));
    }
    final StreamConversion streamc = streamDesc.getStreamConversion();
    if (streamc == null) {
      for (var desc : attributes) {
        final String name = desc.getName();
        final Object internalValue = row.getObject(desc.getColumn());
        if (internalValue == null) {
          logger.error(
              "MALFORMED DATA RETRIEVED: attribute is null;"
                  + " tenant={}, signal={}, attribute={}, keyspace={}, table={}, timestamp={},"
                  + " eventId={}, row={}",
              getTenantName(),
              getStreamName(),
              desc.getName(),
              keyspaceName,
              tableName,
              event.getIngestTimestamp().getTime(),
              event.getEventId(),
              row);
          return null;
        }
        final Object value = cassandraToDataEngine(internalValue, desc);
        if (compiledFilter != null) {
          final var terms = compiledFilter.get(name);
          if (terms != null) {
            for (FilterTerm term : terms) {
              if (!term.test(value)) {
                return null;
              }
            }
          }
        }
        if (onlyForFilterAttributes == null || !onlyForFilterAttributes.contains(name)) {
          event.set(name, value);
        }
      }
    } else {
      for (final CassAttributeDesc desc : attributes) {
        final String name = desc.getName();
        final Object value;
        final AttributeConversion conv = streamc.getAttributeConversion(name);
        switch (conv.getConversionType()) {
          case NO_CHANGE:
            {
              final CassAttributeDesc myDesc = getAttributeDesc(conv.getOldDesc().getName());
              final Object internalValue = row.getObject(myDesc.getColumn());
              value = cassandraToDataEngine(internalValue, desc);
              break;
            }
          case ADD:
            value = conv.getDefaultValue();
            break;
          case CONVERT:
            final CassAttributeDesc myDesc = getAttributeDesc(conv.getOldDesc().getName());
            value = TfosUtils.convertAttribute(row.getObject(myDesc.getColumn()), myDesc, desc);
            break;
          default:
            throw new ApplicationException("Unknown conversion");
        }
        if (compiledFilter != null) {
          final var terms = compiledFilter.get(name);
          if (terms != null) {
            for (FilterTerm term : terms) {
              if (!term.test(value)) {
                return null;
              }
            }
          }
        }
        if (onlyForFilterAttributes == null || !onlyForFilterAttributes.contains(name)) {
          event.set(name, value);
        }
      }
    }
    return event;
  }

  @Override
  public Map<Long, List<Event>> summarizeEvents(
      List<Event> events,
      SummarizeRequest request,
      CompiledSortRequest sortRequest,
      CassStream mainCassStream,
      EventFactory eventFactory,
      boolean isBiosApi)
      throws ApplicationException, TfosException {
    return summarizeEventsCore(
        events, request, request.getAggregates(), 0, mainCassStream, eventFactory, isBiosApi);
  }

  /**
   * The core part of the summarizeEvent logic.
   *
   * <p>The method is meant be called both by SignalCassStream and RollupCassStream. The logic to
   * build summary is common although rollup stream requires some more pre-process.
   *
   * @param events Input events
   * @param request Summarize request
   * @param aggregates Aggregates to collect
   * @param shift Shift in the time line. The rollup table entries have timestamp at the end of
   *     their intervals. But the an output entry of this method should have timestamp at the
   *     beginning of the interval. The method thus needs to shift timeline to handle rollup table
   *     output.
   * @param mainCassStream CassStream to be used for grouping the output.
   * @return Event summary
   * @throws ConstraintViolationException When the request violates constraint check.
   * @throws ApplicationException When an unexpected error happens.
   */
  protected Map<Long, List<Event>> summarizeEventsCore(
      List<Event> events,
      SummarizeRequest request,
      List<Aggregate> aggregates,
      long shift,
      CassStream mainCassStream,
      EventFactory eventFactory,
      boolean isBiosApi)
      throws ConstraintViolationException, ApplicationException {
    final long interval = request.getInterval();
    final long horizon =
        request.getHorizon() != null ? request.getHorizon() : request.getInterval();
    final long offset = request.getTimezone() != null ? request.getTimezone().getRawOffset() : 0;

    final long end = request.getEndTime();

    final long earliestCheckpoint;
    if (request.getSnappedStartTime() != null) {
      earliestCheckpoint = request.getSnappedStartTime();
    } else {
      earliestCheckpoint =
          (request.getStartTime() + offset + interval - 1) / interval * interval - offset;
    }
    long right = earliestCheckpoint + interval;
    long left = right - horizon;
    if (left < 0) {
      throw new ConstraintViolationException("Time window out of range");
    }

    // generate the group function
    final var groupBy = request.getGroup();
    final View group = new Group(groupBy != null ? groupBy : Collections.emptyList());
    Function<List<Event>, List<Event>> groupingFunction =
        ExtractView.createView(group, List.of(), aggregates, mainCassStream, eventFactory::create);

    // generate the sort function if enabled
    final var sortSpec = request.getSort();
    final Optional<Function<List<Event>, List<Event>>> optionalSort =
        Optional.ofNullable(
            sortSpec != null ? ExtractView.createSortFunction(sortSpec, mainCassStream) : null);

    final Map<Long, List<Event>> out = new LinkedHashMap<>();
    while (right - interval < end) {
      final int ileft = StorageDataUtils.search(events, left + shift);
      final int iright = StorageDataUtils.search(events, right + shift);

      // As rollup table entries have timestamp at the end of their intervals,
      // not start of the interval.
      final Date ts = new Date(right - interval);

      if (ileft < iright) {
        final List<Event> subarray = events.subList(ileft, iright);

        final List<Event> summary =
            aggregate(subarray, groupingFunction, optionalSort, request.getLimit());
        if (!isBiosApi) {
          summary.forEach(entry -> entry.setIngestTimestamp(ts));
        }
        out.put(ts.getTime(), summary);
      } else {
        out.put(ts.getTime(), List.of());
      }
      right += interval;
      left += interval;
    }
    return out;
  }

  protected List<Event> aggregate(
      List<Event> events,
      Function<List<Event>, List<Event>> groupingFunction,
      Optional<Function<List<Event>, List<Event>>> optionalSort,
      Integer limit)
      throws ApplicationException {
    final List<Event> out = groupingFunction.apply(events);

    optionalSort.ifPresent((sortingFunction) -> sortingFunction.apply(out));

    if (limit != null && limit < out.size()) {
      return out.subList(0, limit);
    }
    return out;
  }

  /**
   * Method to calculate time index from a version 1 UUID.
   *
   * @param eventId The source UUID. The version must be 1.
   * @return Time index
   */
  public long makeTimeIndex(final UUID eventId) {
    if (eventId == null) {
      throw new IllegalArgumentException("eventId may not be null");
    }
    if (eventId.version() != 1) {
      throw new IllegalArgumentException(
          "eventId must be version 1 UUID, but version " + eventId.version() + " was specified");
    }
    return makeTimeIndex(UUIDs.unixTimestamp(eventId));
  }

  /**
   * Method to calculate time index from a timestamp.
   *
   * @param timestamp The source timestamp. The time should be represented as milliseconds since
   *     Epoch.
   * @return Time index
   */
  public long makeTimeIndex(final long timestamp) {
    if (timestamp < 0) {
      throw new IllegalArgumentException("timestamp may not be negative");
    }
    if (streamDesc.getType() == StreamType.CONTEXT_FEATURE) {
      return timestamp;
    }
    return timestamp / timeIndexWindowWidth * timeIndexWindowWidth;
  }

  /**
   * Method to make the partition key of an event.
   *
   * @param event Source event
   * @return Partition key
   */
  public Object makePartitionKey(Event event) {
    return Long.valueOf(makeTimeIndex(event.getEventId()));
  }

  /**
   * Check the filter and return a ViewCassStream object that covers the filter
   *
   * @param filter List of relations
   * @param targetAttributes
   * @param views
   * @return ViewCassStream that covers the filter, null if no suitable views are found
   */
  public ViewCassStream findViewForFilter(
      List<SingleColumnRelation> filter,
      List<CassAttributeDesc> targetAttributes,
      List<io.isima.bios.req.View> views) {
    if (filter == null) {
      return null;
    }
    if (views != null) {
      for (var view : views) {
        if ((view.getFunction() != ViewFunction.SORT)
            && (view.getFunction() != ViewFunction.GROUP)) {
          return null;
        }
      }
    }

    final var filterDimensions = new HashSet<String>();

    // create the group dimensions from the filter
    boolean rangeQuerySupportRequired = false;
    for (var relation : filter) {
      if (relation instanceof SingleColumnRelation) {
        final String dimension = relation.getEntity().rawText();
        filterDimensions.add(dimension);
        final var operator = relation.operator();
        if (Set.of(Operator.GT, Operator.GTE, Operator.LT, Operator.LTE).contains(operator)) {
          rangeQuerySupportRequired = true;
        } else if (!Set.of(Operator.EQ, Operator.IN).contains(operator)) {
          // not for an index table, try other(s)
          return null;
        }
      } else {
        // other than SingleColumnRelation are not supported
        return null;
      }
    }

    StreamDesc candidate = null;
    long shortestInterval = Long.MAX_VALUE;
    final StreamDesc signal = getStreamDesc();

    for (String subStreamName : signal.getSubStreamNames()) {
      // Check the substream type and skip if it is not of type rollup
      final CassStream subStream = cassTenant.getCassStream(subStreamName, getStreamVersion());
      final StreamDesc viewStreamDesc = subStream.getStreamDesc();
      if (viewStreamDesc.getType() != StreamType.VIEW) {
        continue;
      }
      final var viewDesc = viewStreamDesc.getViews().get(0);
      final Boolean indexEnabledInView = viewDesc.getIndexTableEnabled();
      if (viewDesc.getIndexTableEnabled() != Boolean.TRUE
          && viewDesc.getWriteTimeIndexing() != Boolean.TRUE) {
        continue;
      }

      for (var view : viewStreamDesc.getViews()) {
        if (rangeQuerySupportRequired && view.getIndexType() != IndexType.RANGE_QUERY) {
          continue;
        }
        // check dimensions match
        final int filterDimensionsSize = filterDimensions.size();
        if (filterDimensionsSize > view.getGroupBy().size()) {
          continue;
        }
        final var viewDimensions =
            view.getGroupBy().stream().map(name -> name.toLowerCase()).collect(Collectors.toSet());
        if (!viewDimensions.equals(filterDimensions)) {
          continue;
        }

        // check attributes coverage
        boolean attributesCovered = true;
        for (var cassAttribute : targetAttributes) {
          if (viewStreamDesc.findAttribute(cassAttribute.getName()) == null) {
            attributesCovered = false;
            break;
          }
        }
        if (!attributesCovered) {
          continue;
        }

        final var interval = viewStreamDesc.getIndexingInterval();
        if (candidate == null || interval < shortestInterval) {
          candidate = viewStreamDesc;
          shortestInterval = interval;
        }
      }
    }
    if (candidate == null) {
      return null;
    }
    final var answer = (ViewCassStream) cassTenant.getCassStream(candidate);
    assert (answer != null);
    return answer;
  }

  @Getter
  private static class FindEffectiveTableState extends ExecutionState {

    private final StreamDesc rollupStreamDesc;
    private final RollupCassStream rollupCassStream;
    private final SignalCassStream cassIndex;
    private final long convolutionFactor;
    private final ViewDesc viewDesc;

    @Setter private Double coverage;
    @Setter private DigestSpecifier spec;

    public FindEffectiveTableState(
        String subStreamName,
        StreamDesc rollupStreamDesc,
        RollupCassStream rollupCassStream,
        SignalCassStream cassIndex,
        long convolutionFactor,
        ExecutionState parent) {
      super("FindEffectiveTable" + subStreamName, parent);
      this.convolutionFactor = convolutionFactor;
      this.rollupStreamDesc = rollupStreamDesc;
      this.rollupCassStream = rollupCassStream;
      this.cassIndex = cassIndex;
      this.viewDesc = rollupStreamDesc.getViews().get(0);
    }

    public List<String> getProvidedDimensions() {
      return viewDesc.getGroupBy();
    }
  }
}
