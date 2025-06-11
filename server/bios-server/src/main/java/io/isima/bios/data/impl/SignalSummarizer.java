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
package io.isima.bios.data.impl;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.EventFactory;
import io.isima.bios.common.SharedProperties;
import io.isima.bios.common.SummarizeState;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.data.impl.models.UseSketchPreference;
import io.isima.bios.data.impl.storage.AsyncQueryExecutor;
import io.isima.bios.data.impl.storage.CassAttributeDesc;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.QueryInfo;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.data.impl.storage.StorageDataUtils;
import io.isima.bios.data.impl.storage.ViewCassStream;
import io.isima.bios.errors.EventExtractError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.NoFeatureFoundException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.Range;
import io.isima.bios.models.SummarizeRequest;
import io.isima.bios.models.View;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Component of DataEngine that takes care of summarize method. */
public class SignalSummarizer {
  static final Logger logger = LoggerFactory.getLogger(SignalSummarizer.class);

  // constants //////////////////////////////////////////////////////////
  private static final String MAX_EXTRACT_CONCURRENCY = DataEngine.MAX_EXTRACT_CONCURRENCY;
  private static final int DEFAULT_MAX_EXTRACT_CONCURRENCY =
      DataEngine.DEFAULT_MAX_EXTRACT_CONCURRENCY;
  ///////////////////////////////////////////////////////////////////////

  private static final Set<MetricFunction> AGGREGATES_SUPPORTED_BY_ROLLUP_AND_ON_THE_FLY;

  static {
    AGGREGATES_SUPPORTED_BY_ROLLUP_AND_ON_THE_FLY =
        Set.of(
            MetricFunction.SUM,
            MetricFunction.COUNT,
            MetricFunction.MIN,
            MetricFunction.MAX,
            MetricFunction.DISTINCTCOUNT,
            MetricFunction.AVG);
  }

  private final DataEngineImpl dataEngine;
  private final Session session;
  private final SketchesExtractor sketchesExtractor;

  public SignalSummarizer(
      DataEngineImpl dataEngine, Session session, SketchesExtractor sketchesExtractor) {
    this.dataEngine = dataEngine;
    this.session = session;
    this.sketchesExtractor = sketchesExtractor;
  }

  /**
   * Method to execute summarize operation.
   *
   * @param state Summarize state tracker
   * @param cassStream CassStream where the events are retrieved
   * @param acceptor The asynchronous acceptor. The caller should assume that the The method may
   *     finish before the summary completes and result of the successful execution would come to
   *     this acceptor.
   * @param errorHandler The asynchronous error handler. The call should assume that the method may
   *     finish before the summary completes and any failure report would come to this error
   *     handler.
   * @throws TfosException When a user error occurs.
   * @throws ApplicationException When an application error happens.
   */
  public void summarize(
      SummarizeState state,
      CassStream cassStream,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler)
      throws TfosException, ApplicationException {

    BiosModules.getSharedProperties()
        .getPropertyCachedIntAsync(
            MAX_EXTRACT_CONCURRENCY,
            DEFAULT_MAX_EXTRACT_CONCURRENCY,
            state,
            (maxConcurrency) -> {
              try {
                summarize(state, cassStream, maxConcurrency, acceptor, errorHandler);
              } catch (TfosException | ApplicationException e) {
                state.markError();
                errorHandler.accept(e);
              }
            },
            (throwable) -> {
              state.markError();
              errorHandler.accept(throwable);
            });
  }

  public void summarize(
      SummarizeState state,
      CassStream cassStream,
      int maxConcurrency,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler)
      throws TfosException, ApplicationException {

    final SummarizeRequest request = state.getInput();

    if (state.isValidated()) {
      ExtractView.validateFilter(request.getFilter(), state, cassStream);
    } else {
      state.addHistory("verifyInput");
      validateSummarizeRequest(cassStream, state);
    }

    final long startTime;
    if (request.getSnappedStartTime() != null) {
      startTime = request.getSnappedStartTime();
    } else {
      final long st = request.getStartTime() + request.getInterval() - request.getHorizon();
      final long interval = request.getInterval();
      startTime = st / interval * interval;
    }

    // If we want to use sketches to satisfy this request, try that first.
    final var preference = UseSketchPreference.decide(request, state.getStreamDesc());

    final Set<CassAttributeDesc> attributes = new HashSet<>();
    if (!preference.useSketch() || request.isOnTheFly()) {
      for (Aggregate aggregate : request.getAggregates()) {
        if (!AGGREGATES_SUPPORTED_BY_ROLLUP_AND_ON_THE_FLY.contains(aggregate.getFunction())) {
          final String reason;
          if (request.isOnTheFly()) {
            reason =
                "Not supported: Aggregate "
                    + aggregate.getFunction()
                    + " is requested in on-the-fly mode";
          } else {
            // unlikely to happen
            reason =
                "Not supported: Aggregate"
                    + aggregate.getFunction()
                    + " with "
                    + preference.notPossibleReasons();
          }
          throw new InvalidRequestException(reason);
        }
        if (aggregate.getFunction() != MetricFunction.COUNT) {
          attributes.add(cassStream.getAttributeDesc(aggregate.getBy()));
        }
      }
      if (request.getGroup() != null) {
        for (String dimension : request.getGroup()) {
          attributes.add(cassStream.getAttributeDesc(dimension));
        }
      }
    }

    final CompletableFuture<QueryFuel> makeFuel;
    if (preference.useSketch()) {
      makeFuel =
          prepareSketchQuery(
                  state,
                  false,
                  request,
                  startTime,
                  maxConcurrency,
                  preference,
                  acceptor,
                  errorHandler)
              .thenCompose(
                  (lastCoverage) -> {
                    if (lastCoverage == null) {
                      return prepareRollupQuery(
                          cassStream,
                          attributes,
                          state,
                          true,
                          request,
                          startTime,
                          maxConcurrency,
                          preference,
                          acceptor,
                          errorHandler);
                    }
                    return CompletableFuture.completedFuture(lastCoverage);
                  });
    } else {
      makeFuel =
          prepareRollupQuery(
                  cassStream,
                  attributes,
                  state,
                  false,
                  request,
                  startTime,
                  maxConcurrency,
                  preference,
                  acceptor,
                  errorHandler)
              .thenCompose(
                  (lastCoverage) -> {
                    if (lastCoverage == null) {
                      return prepareSketchQuery(
                          state,
                          true,
                          request,
                          startTime,
                          maxConcurrency,
                          preference,
                          acceptor,
                          errorHandler);
                    }
                    return CompletableFuture.completedFuture(lastCoverage);
                  });
    }

    makeFuel
        .thenAccept(
            (fuel) ->
                ExecutionHelper.run(
                    () -> {
                      if (fuel.executor == null && !request.isOnTheFly()) {
                        return;
                      }

                      state.addHistory("(query{");
                      final AsyncQueryExecutor onTheFlyExecutor;
                      if (request.isOnTheFly()) {
                        state.addHistory("(prepareOnTheFly");
                        final Collection<CassAttributeDesc> tempAttributes;
                        if (state.getCompiledFilter() != null) {
                          tempAttributes = new HashSet<>(attributes);
                          for (var attributeName : state.getCompiledFilter().keySet()) {
                            tempAttributes.add(cassStream.getAttributeDesc(attributeName));
                          }
                        } else {
                          tempAttributes = attributes;
                        }
                        long lastCoverage =
                            fuel.lastCoverage != -1
                                ? fuel.lastCoverage
                                : request.getSnappedStartTime() + request.getInterval();
                        final List<QueryInfo> onTheFlyQueries =
                            cassStream.makeExtractStatements(
                                state,
                                tempAttributes,
                                lastCoverage - request.getInterval(),
                                request.getOrigEndTime());
                        final var secondAcceptor = fuel.completionHandler.makeExecutorAcceptor();
                        onTheFlyExecutor =
                            new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());
                        prepareDefaultQueries(
                            state,
                            onTheFlyQueries,
                            cassStream,
                            onTheFlyExecutor,
                            secondAcceptor,
                            errorHandler);
                        state.addHistory(")");
                      } else {
                        onTheFlyExecutor = null;
                      }

                      state.endPreProcess();
                      state.startDbAccess();

                      fuel.completionHandler.prepare();
                      fuel.executor.execute();
                      if (onTheFlyExecutor != null) {
                        onTheFlyExecutor.execute();
                      }
                    }))
        .exceptionally(
            (t) -> {
              if (t instanceof CompletionException) {
                errorHandler.accept(t.getCause());
              } else {
                errorHandler.accept(t);
              }
              return null;
            });
  }

  /**
   * Prepares data sketches queries.
   *
   * @return Future for generated QueryFuel. Null if the query cannot be covered by this approach.
   *     The member 'executor' is null if there is nothing to query and empty response is returned
   *     in the method.
   */
  private CompletableFuture<QueryFuel> prepareSketchQuery(
      SummarizeState state,
      boolean isRetry,
      SummarizeRequest request,
      long startTime,
      int maxConcurrency,
      UseSketchPreference preference,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler) {
    try {
      final var executor = new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());
      final var completionHandler = new SummarizeCompletionHandler(acceptor);
      final long lastCoverage =
          sketchesExtractor.summarize(
              state, executor, completionHandler.makeExecutorAcceptor(), errorHandler, startTime);
      return CompletableFuture.completedFuture(
          new QueryFuel(executor, completionHandler, lastCoverage));
    } catch (InvalidRequestException e) {
      logger.debug(
          "Tried to use sketch but could not, possibly because it was a summarize"
              + "without any features - going to main events table. request={}",
          request);
      if (!preference.isSketchRequired() && !isRetry) {
        // will try rollup query
        return CompletableFuture.completedFuture(null);
      }
      final String newMessage =
          "Sketch is required because: "
              + preference.requiredReasons()
              + ". However: "
              + e.getMessage();
      logger.info(
          "Unexpected use of sketch functionality: message={}, details={}", newMessage, preference);
      return CompletableFuture.failedFuture(new InvalidRequestException(newMessage));
    } catch (TfosException | ApplicationException e) {
      return CompletableFuture.failedFuture(e);
    }
  }

  /**
   * Prepares rollup queries.
   *
   * @return Future for generated QueryFuel. Null if the query cannot be covered by this approach.
   *     The member 'executor' is null if there is nothing to query and empty response is returned
   *     in the method.
   */
  private CompletableFuture<QueryFuel> prepareRollupQuery(
      CassStream cassStream,
      Set<CassAttributeDesc> attributes,
      SummarizeState state,
      boolean isRetry,
      SummarizeRequest request,
      long startTime,
      int maxConcurrency,
      UseSketchPreference preference,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler) {
    state.addHistory("makeStatement");
    return ((SignalCassStream) cassStream)
        .makeSummarizeStatements(state, attributes, startTime, request.getEndTime())
        .thenApply(
            (queries) ->
                ExecutionHelper.supply(
                    () -> {
                      long lastCoverage = -1;
                      if (request.isOnTheFly()) {
                        Range range = null;
                        for (var query : queries) {
                          if (range == null) {
                            range = query.getTimeRange();
                          } else if (query.getTimeRange().getEnd() > range.getEnd()) {
                            range = query.getTimeRange();
                          }
                        }
                        if (range != null) {
                          lastCoverage = range.getEnd() - request.getInterval() - 1;
                        }
                      }

                      // if there is nothing to return, end here.
                      if (queries.isEmpty()) {
                        state.getQueryLoggerItem().ifPresent((item) -> item.addPostDb(0, 0, 0));
                        acceptor.accept(Map.of());
                        return new QueryFuel(null, null, lastCoverage);
                      }

                      // we assume all queries are for the same table
                      final StreamDesc queryStreamDesc =
                          queries.get(0).getCassStream().getStreamDesc();
                      final var executor =
                          new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());
                      final var completionHandler = new SummarizeCompletionHandler(acceptor);
                      final var firstAcceptor = completionHandler.makeExecutorAcceptor();
                      if (queryStreamDesc.getType() != StreamType.INDEX) {
                        prepareDefaultQueries(
                            state, queries, cassStream, executor, firstAcceptor, errorHandler);
                      } else {
                        prepareIndexQueries(
                            state, queries, queryStreamDesc, executor, firstAcceptor, errorHandler);
                      }

                      return new QueryFuel(executor, completionHandler, lastCoverage);
                    }))
        .exceptionally(
            (t) -> {
              final var cause = t instanceof CompletionException ? t.getCause() : t;
              if (cause instanceof NoFeatureFoundException) {
                // Let DataSketches handle the request if possible
                if (preference.isSketchPossible() && !isRetry) {
                  return null;
                }
              }
              // rethrow
              throw new CompletionException(cause);
            });
  }

  /** Intermediate objects generated by query preparation methods. */
  @AllArgsConstructor
  private static class QueryFuel {
    // Built executor, null if done already (i.e. nothing to query)
    public AsyncQueryExecutor executor;
    // Generated completion handler
    public SummarizeCompletionHandler completionHandler;
    // The last timestamp covered by the initial queries. Used for making on-the-fly query
    public long lastCoverage;
  }

  /**
   * Method to prepare default queries.
   *
   * <p>The method puts default asynchronous execution stages to the executor based on the generated
   * statements.
   *
   * @param state Summarize execution state
   * @param queries Generated queries
   * @param cassStream Signal CassStream to query
   * @param executor Asynchronous execution executor
   * @param acceptor Result acceptor
   * @param errorHandler Error handler
   */
  private void prepareDefaultQueries(
      SummarizeState state,
      List<QueryInfo> queries,
      CassStream cassStream,
      AsyncQueryExecutor executor,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler) {
    logger.debug("Running default queries for summarizing");
    // List of event lists that accumulates the DB query results. The result handler joins the lists
    // into a single list and pass it to the method finishExecution.
    final List<Event>[] eventsList = new List[queries.size()];
    final var totalCount = new AtomicInteger(0);
    final var canceled = new AtomicBoolean(false);

    int index = 0;
    for (var query : queries) {
      executor.addStage(
          new DefaultSummarizeStage(
              state,
              query,
              cassStream,
              eventsList,
              totalCount,
              canceled,
              acceptor,
              errorHandler,
              index++));
    }
  }

  /**
   * Asynchronous execution stage for default queries.
   *
   * <p>This stage is meant to handle extractions from a rollup table. The {@link #finishExecution}
   * method bundles events for each time interval and put them into the output map of timestamp and
   * list of data points.
   */
  private static class DefaultSummarizeStage
      extends SignalQueryStage<SummarizeState, Map<Long, List<Event>>> {
    private final SummarizeRequest request;

    public DefaultSummarizeStage(
        SummarizeState state,
        QueryInfo queryInfo,
        CassStream cassStream,
        List<Event>[] eventsList,
        AtomicInteger totalDataPoints,
        AtomicBoolean canceled,
        Consumer<Map<Long, List<Event>>> acceptor,
        Consumer<Throwable> errorHandler,
        int index) {
      super(
          state,
          queryInfo,
          cassStream,
          false,
          eventsList,
          totalDataPoints,
          canceled,
          acceptor,
          errorHandler,
          index);
      request = state.getInput();
    }

    @Override
    public void beforeQuery() {
      state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
    }

    @Override
    protected void acceptRow(Row row) throws ApplicationException {
      final var compiledFilter = state.getCompiledFilter();
      final var onlyForFilterAttributes = state.getOnlyForFilterAttributes();
      final Event event =
          cassStreamForDecoding.buildEvent(
              row,
              attributes,
              noEventIdColumn,
              EventFactory.DEFAULT_FACTORY,
              compiledFilter,
              onlyForFilterAttributes);
      if (event != null) {
        events.add(event);
      }
    }

    @Override
    protected void resultReceived() {
      state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, events.size()));
    }

    @Override
    protected Map<Long, List<Event>> finishExecution(List<Event> totalEvents)
        throws ApplicationException, TfosException {
      var startPostDbTime = Instant.now();
      StorageDataUtils.convertToOutputData(
          totalEvents, attributes, state.getTenantName(), state.getStreamName());
      final var out =
          cassStreamForDecoding.summarizeEvents(
              totalEvents,
              request,
              state.getCompiledSortRequest(),
              cassStream,
              state.getEventFactory(),
              state.isBiosApi());
      final var numRecords = out.values().stream().mapToLong(List::size).sum();
      meterRawReads(numRecords);
      var timeTakenMicros = Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
      state
          .getQueryLoggerItem()
          .ifPresent((item) -> item.addPostDb(timeTakenMicros, out.size(), numRecords));
      return out;
    }
  }

  /**
   * Method to prepare index-based summarize queries.
   *
   * <p>The method puts index-based asynchronous execution stages to the executor with the generated
   * statements.
   *
   * <p>The query results would be accumulated to a queue of query statements for the view table.
   * The generated statements would be consumed at the end of this execution stages.
   *
   * @param state Summarize execution state
   * @param queries Generated queries
   * @param indexStream Index stream desc to query
   * @param executor Asynchronous execution executor
   * @param acceptor Result acceptor
   * @param errorHandler Error handler
   */
  private void prepareIndexQueries(
      SummarizeState state,
      List<QueryInfo> queries,
      StreamDesc indexStream,
      AsyncQueryExecutor executor,
      Consumer<Map<Long, List<Event>>> acceptor,
      Consumer<Throwable> errorHandler) {
    logger.debug("Using index and view for summarizing");
    final StreamDesc signalDesc = indexStream.getParentStream();
    final TenantDesc tenantDesc = signalDesc.getParent();
    // An index stream always has one corresponding view
    final ViewDesc view = indexStream.getViews().get(0);
    final StreamDesc viewDesc =
        tenantDesc.getStream(AdminUtils.makeViewStreamName(signalDesc, view), false);
    final ViewCassStream viewCassStream = (ViewCassStream) dataEngine.getCassStream(viewDesc);

    // Executor used for the next-step query stages against the view table.
    int maxConcurrency =
        SharedProperties.getCached(MAX_EXTRACT_CONCURRENCY, DEFAULT_MAX_EXTRACT_CONCURRENCY);
    final AsyncQueryExecutor nextExecutor =
        new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());
    // The query results of this stages are accumulated into this queue as next-step queries.
    final Queue<SummarizeViewQueryInfo> viewQueries = new ConcurrentLinkedQueue<>();

    // Prepare totalViewEntries entries before we run the queries to avoid race conditions
    // during result handling.
    // key: timestamp, value: map of (key: dimensions, value: retrieved event)
    final Map<Long, Map<List<Object>, Event>> totalViewEntries = new HashMap<>();
    final long interval = state.getInput().getInterval().longValue();
    final long reqStart = (((state.getInput().getStartTime() - 1) / interval) + 1) * interval;
    final long reqEnd = state.getInput().getEndTime();
    for (long start = reqStart; start < reqEnd; start += interval) {
      totalViewEntries.put(Long.valueOf(start + interval), new ConcurrentHashMap<>());
    }

    // Register execution stages to the executor
    int index = 0;
    for (var query : queries) {
      executor.addStage(
          new IndexSummarizeStage(
              state,
              query,
              viewCassStream,
              nextExecutor,
              viewQueries,
              totalViewEntries,
              acceptor,
              errorHandler,
              index++));
    }
  }

  /**
   * Method to validate summarize request. The method would modify the request object in place for
   * easier execution in later stage.
   *
   * @param cassStream CassStream where the summarizing happens.
   * @param state Summarize execution state
   * @throws TfosException Thrown when the request is invalid.
   * @throws ApplicationException When an unexpected error happens.
   */
  public static void validateSummarizeRequest(CassStream cassStream, SummarizeState state)
      throws TfosException, ApplicationException {
    final SummarizeRequest request = state.getInput();
    final StreamType streamType = cassStream.getStreamDesc().getType();
    if (streamType != StreamType.SIGNAL && streamType != StreamType.METRICS) {
      throw new ConstraintViolationException(
          String.format("Stream type %s does not support summarize operation", streamType.name()));
    }

    // simple sanity check for start time and end time
    final long startTime = request.getStartTime();
    final long endTime = request.getEndTime();

    if (startTime < 0) {
      throw new InvalidValueException("startTime may not be negative");
    }

    if (startTime > endTime) {
      throw new ConstraintViolationException("endTime may not be earlier than startTime");
    }

    // verify interval
    final long interval = request.getInterval();
    if (interval <= 0) {
      throw new InvalidValueException("interval value should be positive");
    }

    // verify time range covers at least one summarize interval
    if (request.getTimezone() == null) {
      request.setTimezone(TimeZone.getTimeZone("UTC"));
    }
    final int offset = request.getTimezone().getRawOffset();
    final long earliestCheckpoint =
        (startTime + offset + interval - 1) / interval * interval - offset;
    if (earliestCheckpoint >= endTime) {
      throw new ConstraintViolationException(
          "time range should cover at least one summary checkpoint");
    }

    // verify horizon
    if (request.getHorizon() == null) {
      request.setHorizon(request.getInterval());
    }
    final long horizon = request.getHorizon();
    if (horizon <= 0) {
      throw new InvalidValueException("horizon value should be positive");
    }
    if (horizon % interval != 0) {
      throw new ConstraintViolationException(
          String.format("horizon=%d is not multiple of interval=%d", horizon, interval));
    }
    if (earliestCheckpoint + interval - horizon < 0) {
      throw new ConstraintViolationException("The first summarize window goes out of boundary");
    }

    // check number of summarize points
    final long summarizePoints = (endTime - earliestCheckpoint + interval - 1) / interval;
    final long searchEndTime = (summarizePoints * interval) + earliestCheckpoint;
    final int hlimit = TfosConfig.summarizeHorizontalLimit();
    if (summarizePoints > hlimit) {
      throw new TfosException(
          GenericError.QUERY_SCALE_TOO_LARGE,
          "Number of summarize time points "
              + summarizePoints
              + " exceeds limitation of "
              + hlimit
              + "; interval="
              + interval
              + ".");
    }

    // update end time
    request.setEndTime(searchEndTime);

    // verify aggregates
    if (request.getAggregates() == null || request.getAggregates().isEmpty()) {
      throw new InvalidValueException("aggregates may not be null or empty");
    }
    final Set<String> eventAttributeKeys = new HashSet<>();
    ExtractView.validateAggregates(request.getAggregates(), cassStream, eventAttributeKeys);

    // verify groups
    final var normalizedDimensions = new ArrayList<String>();
    for (int i = 0; i < request.getGroup().size(); ++i) {
      final String dimension = request.getGroup().get(i);
      if (dimension == null || dimension.isEmpty()) {
        throw new InvalidValueException(
            String.format("group[%d]: group key may not be null or empty", i));
      }
      final CassAttributeDesc desc = cassStream.getAttributeDesc(dimension);
      if (desc == null) {
        throw new TfosException(EventExtractError.INVALID_ATTRIBUTE, dimension);
      }
      final String key = desc.getName();
      normalizedDimensions.add(key);
      if (eventAttributeKeys.contains(key)) {
        throw new ConstraintViolationException(
            String.format("group[%d]: duplicate attribute; %s", i, key));
      }
      eventAttributeKeys.add(key);
    }
    request.setGroup(normalizedDimensions);

    // verify sort
    if (request.getSort() != null) {
      final View sort = request.getSort();
      if (sort.getFunction() != ViewFunction.SORT) {
        throw new ConstraintViolationException("non-sort function is specified for sort parameter");
      }
      final CassAttributeDesc desc = cassStream.getAttributeDesc(sort.getBy());
      final String key = desc != null ? desc.getName() : sort.getBy();
      if (!eventAttributeKeys.contains(key)) {
        throw new ConstraintViolationException(
            String.format("sort key '%s' does not match any of output attributes", key));
      }
      if (desc != null && !desc.getAttributeType().isComparable()) {
        throw new ConstraintViolationException(
            String.format("sort key '%s' is not of comparable type", key));
      }
    }

    // verify limit
    if (request.getLimit() != null && request.getLimit() < 0) {
      throw new InvalidValueException(
          String.format("limit=%d: the value must be positive", request.getLimit()));
    }

    // validate filter
    if (request.getFilter() != null && !request.getFilter().isEmpty()) {
      state.setFilter(
          cassStream.parseFilter(request.getFilter()).stream()
              .map((relation) -> relation)
              .collect(Collectors.toList()));
      cassStream.validateFilter(state.getFilter());
    }
  }
}
