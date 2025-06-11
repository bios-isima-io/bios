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
package io.isima.bios.data;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.uuid.Generators;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.ComplexQueryState;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.QueryExecutionState;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.errors.exception.InvalidEnumException;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.QueryType;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.isql.WindowType;
import io.isima.bios.models.v1.Attributes;
import io.isima.bios.req.Aggregate;
import io.isima.bios.req.ExtractRequest;
import io.isima.bios.stats.ClockProvider;
import io.isima.bios.utils.Utils;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class implements a system signal storing queries received by bios. */
public class QueryLogger {
  private static final Logger logger = LoggerFactory.getLogger(QueryLogger.class);
  private static final Clock clock;
  private static final Map<String, Function<Item, Object>> ATTRIBUTE_PROVIDERS;
  private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

  static {
    clock = ClockProvider.getClock();
    final var temp = new LinkedHashMap<String, Function<Item, Object>>();
    temp.put("tenant", (item) -> item.tenant);
    temp.put("user", (item) -> item.user);
    temp.put("client", (item) -> item.client);
    temp.put("node", (item) -> item.node);
    temp.put(
        "queryType",
        (item) -> item.convertEnumToEngineInteger("queryType", item.queryType.stringify()));
    temp.put("select", (item) -> item.select);
    temp.put("from", (item) -> item.from);
    temp.put("where", (item) -> item.where);
    temp.put("groupBy", (item) -> item.groupBy);
    temp.put("orderBy", (item) -> item.orderBy);
    temp.put("limit", (item) -> item.limit);
    temp.put(
        "windowType",
        (item) -> item.convertEnumToEngineInteger("windowType", item.windowType.stringify()));
    temp.put("startTime", (item) -> item.startTime);
    temp.put("endTime", (item) -> item.endTime);
    temp.put("interval", (item) -> item.interval);
    temp.put("timezoneOffset", (item) -> item.timezoneOffset);
    temp.put("status", (item) -> firstOrSecond(item.status, "n/a"));
    temp.put(
        "requestReceivedTime",
        (item) -> {
          final var instant = item.requestReceivedTime;
          return instant != null ? instant.toEpochMilli() : 0L;
        });
    temp.put("responseSentTime", (item) -> item.responseSentTime);
    temp.put("queryLatency", (item) -> item.queryLatency);
    temp.put("numDbQueries", (item) -> item.numDbQueries);
    temp.put("numDbRowsRead", (item) -> item.numDbRowsRead.get());
    temp.put("dbQueryAvgLatency", (item) -> item.dbQueryAvgLatency);
    temp.put("dbTotalLatency", (item) -> item.dbTotalLatency);
    temp.put("postDbLatency", (item) -> item.postDbLatency);
    temp.put("numRecordsReturned", (item) -> item.numRecordsReturned);
    temp.put("numTimeWindowsReturned", (item) -> item.numTimeWindowsReturned);
    temp.put("details", (item) -> firstOrSecond(item.details, EMPTY_BUFFER));
    ATTRIBUTE_PROVIDERS = Collections.unmodifiableMap(temp);
  }

  private final DataEngine dataEngine;
  private final StreamDesc querySignal;

  public QueryLogger(DataEngine dataEngine, AdminInternal admin)
      throws NoSuchTenantException, NoSuchStreamException {
    this.dataEngine = dataEngine;
    this.querySignal = admin.getStream(BiosConstants.TENANT_SYSTEM, BiosConstants.QUERY_SIGNAL);
  }

  private QueryLogger() {
    dataEngine = null;
    querySignal = null;
  }

  /**
   * Builds a new query logger item from extraction state.
   *
   * @param state Extraction state
   * @return Built item
   */
  public Item newItem(ExtractState state) {
    final var request = state.getInput();
    if (request == null) {
      throw new IllegalStateException("extractState.request is not set yet");
    }
    final var tenant = state.getTenantName();
    final var nodeName = Utils.getNodeName();
    final var isRollupTask = state.isRollupTask();
    final var attributes = stringifyAttributes(request);
    final var stream = state.getStreamName();
    final var filter = firstOrSecond(request.getFilter(), "");
    List<String> dimensions = null;
    String orderBy = "";
    if (request.getViews() != null) {
      for (var view : request.getViews()) {
        if (view.getFunction() == ViewFunction.GROUP) {
          if (dimensions == null) {
            dimensions = new ArrayList<>();
          }
          dimensions.addAll(view.getDimensions());
        } else if (view.getFunction() == ViewFunction.SORT && orderBy.isBlank()) {
          orderBy = view.getBy();
        }
      }
    }
    final var groupBy = dimensions == null ? "" : dimensions.toString();
    final var limit = firstOrSecond(request.getLimit(), 0);
    final var interval = 0;
    final var timezone = 0;
    final String user =
        (state.getUserContext() == null) ? "n/a" : state.getUserContext().getUserId().toString();
    final String client =
        (state.getUserContext() == null) ? "n/a" : state.getUserContext().getAppName();
    return new Item(
        querySignal,
        tenant,
        user,
        client,
        nodeName,
        isRollupTask,
        QueryType.EXTRACT,
        attributes,
        stream,
        filter,
        groupBy,
        orderBy,
        limit,
        WindowType.GLOBAL_WINDOW,
        request.getStartTime(),
        request.getEndTime(),
        interval,
        timezone);
  }

  /**
   * Builds a new query logger item from summarizing state.
   *
   * @param state Summarizing state
   * @return Built item
   */
  public Item newItem(SummarizeState state) {
    final var request = state.getInput();
    if (request == null) {
      throw new IllegalStateException("summarizeState.request is not set yet");
    }
    final var tenant = state.getTenantName();
    final var isRollupTask = state.isRollupTask();
    final var stream = state.getStreamName();
    return newItem(request, tenant, isRollupTask, stream, QueryType.SUMMARIZE, state);
  }

  public Item newItem(ComplexQueryState state) {
    final var request = state.getInput();
    if (request == null) {
      throw new IllegalStateException("ComplexQueryState.request is not set yet");
    }
    final var tenant = state.getTenantName();
    final var stream = state.getStreamName();
    return newItem(request, tenant, false, stream, QueryType.COMPLEX_QUERY, state);
  }

  private Item newItem(
      SummarizeRequest request,
      String tenant,
      boolean isInternal,
      String stream,
      QueryType queryType,
      QueryExecutionState state) {
    final var nodeName = Utils.getNodeName();
    final var attributes = stringifyAttributes(request);
    final var filter = firstOrSecond(request.getFilter(), "");
    final var groupBy = firstOrSecond(request.getGroup(), List.of()).toString();
    final var orderBy = stringifyOrderBy(request);
    final var limit = firstOrSecond(request.getLimit(), 0);
    final var interval = firstOrSecond(request.getInterval(), 0L);
    final var timezone = request.getTimezone() != null ? request.getTimezone().getRawOffset() : 0;
    final String user =
        (state.getUserContext() == null) ? "n/a" : state.getUserContext().getUserId().toString();
    final String client =
        (state.getUserContext() == null) ? "n/a" : state.getUserContext().getAppName();
    return new Item(
        querySignal,
        tenant,
        user,
        client,
        nodeName,
        isInternal,
        queryType,
        attributes,
        stream,
        filter,
        groupBy,
        orderBy,
        limit,
        guessWindowType(request),
        request.getStartTime(),
        request.getEndTime(),
        interval,
        timezone);
  }

  /**
   * For use by a complex query that issues one or more bios queries itself. These items are meant
   * to be used to gather data for the bios queries issued by the complex query. After all queries
   * are completed, the complex query should update it's own QueryLogger.Item by calling
   * incorporateTempItems().
   */
  public static List<Item> newTempItems(int numItems) {
    var tempQueryLogger = new QueryLogger();
    var items = new LinkedList<Item>();
    for (int i = 0; i < numItems; i++) {
      items.add(tempQueryLogger.new Item());
    }
    return items;
  }

  // QueryLogger utilities /////////////////////////////////////////////////////////////////

  /**
   * Helper method that returns the first parameter if it is non-null otherwise the second.
   *
   * @param first The first parameter
   * @param second The second parameter
   * @param <T> Type of the parameter
   * @return The first or the second parameter
   */
  private static <T> T firstOrSecond(T first, T second) {
    return first != null ? first : second;
  }

  private String stringifyAttributes(ExtractRequest request) {
    final boolean hasAttributes = request.getAttributes() != null;
    final boolean hasAggregates = request.getAggregates() != null;
    if (hasAttributes || hasAggregates) {
      final var entries = new ArrayList<String>();
      if (hasAttributes) {
        entries.addAll(request.getAttributes());
      }
      if (hasAggregates) {
        entries.addAll(
            request.getAggregates().stream()
                .map((aggregate) -> stringifyAggregate(aggregate))
                .collect(Collectors.toList()));
      }
      return entries.toString();
    }
    return "";
  }

  private String stringifyAttributes(SummarizeRequest request) {
    if (request.getAggregates() == null) {
      return "";
    }
    return request.getAggregates().stream()
        .map((aggregate) -> stringifyAggregate(aggregate))
        .collect(Collectors.toList())
        .toString();
  }

  private String stringifyAggregate(Aggregate aggregate) {
    return String.format(
        "%s(%s)",
        aggregate.getFunction().name().toLowerCase(), firstOrSecond(aggregate.getBy(), ""));
  }

  private String stringifyOrderBy(SummarizeRequest request) {
    if (request.getSort() == null) {
      return "";
    }
    return request.getSort().getBy();
  }

  private WindowType guessWindowType(SummarizeRequest request) {
    if (request.getHorizon() == null || request.getHorizon().equals(request.getInterval())) {
      return WindowType.TUMBLING_WINDOW;
    } else {
      return WindowType.SLIDING_WINDOW;
    }
  }

  // End QueryLogger utilities /////////////////////////////////////////////////////////////

  /**
   * Object holding details of each query to be ingested as attributes in the system signal. It also
   * holds temporary working variables.
   */
  @Getter
  @RequiredArgsConstructor(access = AccessLevel.PACKAGE)
  public class Item {
    // Not stored in the signal; used for processing.
    private final StreamDesc querySignal;
    // Query origin metadata.
    private final String tenant;
    private final String user;
    private final String client;
    private final String node;
    private final Boolean isInternal;

    // Query parameters - from SummarizeRequest and ExtractRequest.
    private final QueryType queryType;
    private final String select; // Attributes, aggregates.
    private final String from;
    private final String where; // Filter.
    private final String groupBy;
    private final String orderBy;
    private final long limit;
    private final WindowType windowType;
    private final long startTime;
    private final long endTime;
    private final long interval;
    private final long timezoneOffset;

    // Execution results.
    private String status;
    @Setter private Instant requestReceivedTime;
    private long responseSentTime = 0;
    private long queryLatency;
    private long numDbQueries;
    private final AtomicLong numDbRowsRead = new AtomicLong();
    private final AtomicLong dbLatencySum = new AtomicLong();
    private long dbQueryAvgLatency;
    private long dbTotalLatency;
    private long postDbLatency = 0;
    private long numRecordsReturned = 0;
    private long numTimeWindowsReturned = 0;
    @Setter private ByteBuffer details;

    private Instant startDbTotalInstant = Instant.EPOCH;
    private Instant latestEndDbInstant = Instant.EPOCH;
    private ConcurrentHashMap<Integer, Instant> startDbInstants = new ConcurrentHashMap<>();
    private boolean addPostDbCalled = false;
    private boolean committed = false;

    public Item() {
      querySignal = null;
      tenant = null;
      user = null;
      client = null;
      node = null;
      isInternal = null;
      queryType = null;
      select = null;
      from = null;
      where = null;
      groupBy = null;
      orderBy = null;
      limit = 0;
      windowType = null;
      startTime = 0;
      endTime = 0;
      interval = 0;
      timezoneOffset = 0;
    }

    // Enumerated attribute getters - need to convert them to dataEngine integer values.
    public Object convertEnumToEngineInteger(String attributeName, String enumStringValue) {
      final var attribute = querySignal.findAttribute(attributeName);
      Object convertedValue = attribute.getInternalDefaultValue();
      try {
        convertedValue = Attributes.convertValue(enumStringValue, attribute);
      } catch (InvalidValueSyntaxException | InvalidEnumException e) {
        logger.warn(
            "Unexpected conversion error for internal signal ingestion; enumStringValue={}",
            enumStringValue,
            e);
      }
      return convertedValue;
    }

    // Item checkpointing methods ///////////////////////////////////////////////
    // Note: We also use setRequestReceivedTime() for checkpointing.

    /**
     * Checkpointing method that should be called before each database query starts. It sets the
     * start timestamp for this DB query. If this is the first DB query to start, it also sets the
     * start timestamp for all DB queries for this QueryLogger item.
     *
     * @param index An index for the DB Query - it identifies the query and must be passed in the
     *     corresponding {@link #endDb} call.
     */
    public void startDb(int index) {
      final var now = clock.instant();
      if (startDbTotalInstant == Instant.EPOCH) {
        startDbTotalInstant = now;
      }
      startDbInstants.put(index, now);
    }

    /**
     * Checkpointing method that should be called after each database query completes. If
     * numRowsRead is not available immediately, pass 0 for now and call {@link #addDbRowsRead} when
     * it is available.
     *
     * @param index Query index that identifies the query - returned by {@link #startDb}.
     * @param numRowsRead Number of rows read by the query.
     */
    public void endDb(int index, long numRowsRead) {
      final var now = clock.instant();
      final long latency = Duration.between(startDbInstants.get(index), now).toNanos() / 1000;
      this.dbLatencySum.addAndGet(latency);
      this.numDbRowsRead.addAndGet(numRowsRead);
      // latestEndDbInstant will keep getting overwritten and the last write will survive.
      this.latestEndDbInstant = now;
    }

    /**
     * Optional checkpointing method that can be called after a database query completes, if the
     * numRowsRead was not available immediately after the query completed.
     *
     * @param numRowsRead Number of rows read by the query.
     */
    public void addDbRowsRead(long numRowsRead) {
      this.numDbRowsRead.addAndGet(numRowsRead);
    }

    /**
     * Checkpointing method that should be called after post-processsing of a batch of DB queries is
     * done. It is OK to call this function multiple times.
     *
     * @param timeTakenMicros Number of microsecond spent in post-processing work.
     * @param numTimeWindows Number of time windows in the reply
     * @param numRecordsReturned Number of records in the reply
     */
    public void addPostDb(long timeTakenMicros, long numTimeWindows, long numRecordsReturned) {
      this.addPostDbCalled = true;
      this.postDbLatency += timeTakenMicros;
      this.numRecordsReturned += numRecordsReturned;
      this.numTimeWindowsReturned += numTimeWindows;
    }

    /**
     * Incorporate values captured by temp items into the current item. If this function is called,
     * it must be called before responseSent().
     */
    public void incorporateTempItems(List<Item> tempItems) {
      for (var tempItem : tempItems) {
        if (this.startDbTotalInstant == Instant.EPOCH) {
          this.startDbTotalInstant = tempItem.startDbTotalInstant;
        }
        if (tempItem.startDbTotalInstant.isBefore(this.startDbTotalInstant)) {
          this.startDbTotalInstant = tempItem.startDbTotalInstant;
        }
        if (tempItem.latestEndDbInstant.isAfter(this.latestEndDbInstant)) {
          this.latestEndDbInstant = tempItem.latestEndDbInstant;
        }
        this.dbLatencySum.addAndGet(tempItem.dbLatencySum.get());
        this.numDbRowsRead.addAndGet(tempItem.numDbRowsRead.get());

        this.addPostDbCalled = this.addPostDbCalled || tempItem.addPostDbCalled;
        this.postDbLatency += tempItem.postDbLatency;
        this.numRecordsReturned += tempItem.numRecordsReturned;
        this.numTimeWindowsReturned += tempItem.numTimeWindowsReturned;
      }
    }

    /**
     * Checkpointing method that should be called when the response is sent back to the client.
     *
     * @param status The status in the response
     * @param now Current timestamp
     */
    public void responseSent(String status, Instant now) {
      this.status = Objects.requireNonNull(status);
      this.queryLatency = (Duration.between(requestReceivedTime, now).toNanos()) / 1000;
      this.responseSentTime = now.toEpochMilli();
      this.numDbQueries = startDbInstants.size();
      this.dbQueryAvgLatency = numDbQueries > 0 ? dbLatencySum.get() / numDbQueries : 0L;
      dbTotalLatency = Duration.between(startDbTotalInstant, latestEndDbInstant).toNanos() / 1000;

      if (!addPostDbCalled) {
        logger.warn(
            "responseSent() called without addPostDb() being called for item ({})",
            toEvent().toString());
      }
    }

    /** Pushes the logger item to the Data Engine. */
    public void commit() {

      if (committed) {
        logger.warn("Double commit of item ({})", toEvent().toString());
      }
      if (responseSentTime == 0) {
        logger.warn(
            "Commit without responseSent() being called for item ({})", toEvent().toString());
      }
      BiosModules.getDigestor()
          .requestOneTimeTask(
              "QueryLog",
              (state) -> {
                dataEngine.insertEventIgnoreErrors(querySignal, toEvent(), state);
                return CompletableFuture.completedStage(null);
              });
      committed = true;
    }

    /**
     * Generates an event from this logger item.
     *
     * <p>The generated event implements the least set of methods that are necessary for inserting
     * to DataEngine. No setters are available. No methods for serialization are available either.
     *
     * <p>The event ID and the timestamp are immutable, but other parameters may change when the
     * underlying logger item changes its state. The timestamp is put at the time of event creation.
     *
     * @return Generated event
     */
    public Event toEvent() {
      return new Event() {
        private final UUID eventId = Generators.timeBasedGenerator().generate();

        @Override
        public UUID getEventId() {
          return eventId;
        }

        @Override
        public Event setEventId(UUID eventId) {
          throw new UnsupportedOperationException("Read-only object");
        }

        @Override
        public Date getIngestTimestamp() {
          return new Date(UUIDs.unixTimestamp(eventId));
        }

        @Override
        public Event setIngestTimestamp(Date ingestionTimestamp) {
          throw new UnsupportedOperationException("Read-only object");
        }

        @Override
        public Map<String, Object> getAttributes() {
          throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void setAttributes(Map<String, Object> attributes) {
          throw new UnsupportedOperationException("Read-only object");
        }

        @Override
        public Map<String, Object> getAny() {
          throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void set(String name, Object value) {
          throw new UnsupportedOperationException("Read-only object");
        }

        @Override
        public Object get(String name) {
          final var provider = ATTRIBUTE_PROVIDERS.get(name);
          return provider != null ? provider.apply(Item.this) : null;
        }

        @Override
        public String toString() {
          return ATTRIBUTE_PROVIDERS.keySet().stream()
              .map(
                  (key) -> {
                    final var value = get(key);
                    final String valueString = value != null ? value.toString() : "<null>";
                    final String entry = key + "=" + valueString;
                    return entry;
                  })
              .collect(Collectors.joining(", "));
        }
      };
    }
  }
}
