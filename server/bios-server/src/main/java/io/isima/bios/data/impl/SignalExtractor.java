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

import static io.isima.bios.common.Constants.ORDER_BY_TIMESTAMP;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_EVENT_ID;
import static io.isima.bios.data.storage.cassandra.CassandraConstants.COL_TIME_INDEX;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.UUIDs;
import io.isima.bios.admin.v1.AdminUtils;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.common.ExtractState;
import io.isima.bios.common.FormatVersion;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.impl.feature.ExtractView;
import io.isima.bios.data.impl.storage.AsyncQueryExecutor;
import io.isima.bios.data.impl.storage.AsyncQueryStage;
import io.isima.bios.data.impl.storage.CassAttributeDesc;
import io.isima.bios.data.impl.storage.CassStream;
import io.isima.bios.data.impl.storage.IndexCassStream;
import io.isima.bios.data.impl.storage.QueryInfo;
import io.isima.bios.data.impl.storage.RollupCassStream;
import io.isima.bios.data.impl.storage.SignalCassStream;
import io.isima.bios.data.impl.storage.StorageDataUtils;
import io.isima.bios.data.impl.storage.ViewCassStream;
import io.isima.bios.errors.EventExtractError;
import io.isima.bios.errors.exception.ConstraintViolationException;
import io.isima.bios.errors.exception.InvalidFilterException;
import io.isima.bios.errors.exception.InvalidValueException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.MetricFunction;
import io.isima.bios.models.Sort;
import io.isima.bios.models.ViewFunction;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Group;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.req.Aggregate;
import io.isima.bios.req.ExtractRequest;
import io.isima.bios.req.MutableView;
import io.isima.bios.req.View;
import io.isima.bios.stats.Timer;
import io.isima.bios.utils.StringUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SignalExtractor {
  static final Logger logger = LoggerFactory.getLogger(SignalExtractor.class);

  // constants //////////////////////////////////////////////////////////
  private static final String MAX_EXTRACT_CONCURRENCY = DataEngine.MAX_EXTRACT_CONCURRENCY;
  private static final int DEFAULT_MAX_EXTRACT_CONCURRENCY =
      DataEngine.DEFAULT_MAX_EXTRACT_CONCURRENCY;
  ///////////////////////////////////////////////////////////////////////

  private final DataEngineImpl dataEngine;
  private final Session session;

  public SignalExtractor(DataEngineImpl dataEngine, Session session) {
    this.dataEngine = dataEngine;
    this.session = session;
  }

  /**
   * Method to execute extract operation.
   *
   * @param state Extract state tracker
   * @param cassStream CassStream where the events are retrieved
   * @param acceptor The asynchronous acceptor. The caller should assume that the The method may
   *     finish before the extraction completes and result of the successful execution would come to
   *     this acceptor.
   * @param errorHandler The asynchronous error handler. The call should assume that the method may
   *     finish before the extraction completes and any failure report would come to this error
   *     handler.
   * @throws TfosException When a user error occurs.
   * @throws ApplicationException When an application error happens.
   */
  public void extract(
      ExtractState state,
      CassStream cassStream,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler)
      throws TfosException, ApplicationException {

    final ExtractRequest request = state.getInput();
    // TODO(Naoki): Eliminate this validation branching by consolidating the extraction code paths.
    // Currently, bios SelectServiceHandler validates in the class. BiosServer DataServiceHandler
    // (may be renamed later) lets DataEngine validate the request in the select method.
    if (state.isValidated()) {
      // only filter needs to be validated in the new path
      ExtractView.validateFilter(request.getFilter(), state, cassStream);
    } else {
      // The input verification should happen earlier ideally, but this is a convenient place since
      // cassStream has capability to find stream attributes quickly.
      state.addHistory("(verifyInput");
      String errorMessage = validateExtractRequest(state, cassStream);
      if (errorMessage != null) {
        errorHandler.accept(new ConstraintViolationException(errorMessage));
        return;
      }
      state.addHistory(")");
    }

    state.addHistory("(prepare{");

    final var view = getView(request);
    if (!state.isValidated() && (view != null && view.getFunction() == ViewFunction.DISTINCT)) {
      state.addHistory("(verifyDistinct");
      // Distinct works with only single attribute since its implemented as group by internally
      // single attribute has be explicitly specified
      // We can accept View.by being null, as we default to attribute. but nothing else
      // All other cases should work without fail
      if (request.getAttributes() == null || request.getAttributes().isEmpty()) {
        throw new TfosException(EventExtractError.ATTRIBUTES_CANNOT_BE_EMPTY);
      }
      if (request.getAttributes().size() > 1) {
        throw new TfosException(EventExtractError.MORE_THAN_ONE_ATTRIBUTE_NOT_ALLOWED);
      }
      String viewAttr = request.getAttributes().get(0);
      if (view.getBy() == null) {
        ((MutableView) view).setBy(viewAttr);
      }
      if (!viewAttr.equalsIgnoreCase(view.getBy())) {
        throw new TfosException(EventExtractError.ATTRIBUTE_SHOULD_MATCH_VIEW_SETBY);
      }
      ((MutableView) view).setFunction(ViewFunction.GROUP);
      state.addHistory(")");
    }

    state.addHistory("(selectMethodology");

    // We'll take a different extraction path when an ingest view covers the required extract view
    // TODO(TFOS-1187): We don't use indexed query for extract operation yet (only for rollups)
    final View group = getGroup(request);
    if (group != null
        && state.getFilter() == null
        && state.getStreamDesc().getType() == StreamType.SIGNAL
        && state.isIndexQueryEnabled()) {
      final List<String> attributes = request.getAttributes();
      List<String> dimensions = group.getDimensions();
      if (dimensions == null) {
        dimensions = Collections.singletonList(group.getBy());
      }
      final List<? extends Aggregate> aggregates = request.getAggregates();

      final ViewDesc viewDesc = resolveView(state, dimensions, attributes, aggregates);
      if (viewDesc != null) {
        state.addHistory("})(extractWithGroupView({");
        extractWithGroupView(
            state,
            cassStream,
            viewDesc,
            dimensions,
            attributes,
            aggregates,
            (events) -> {
              state.addHistory("})");
              acceptor.accept(events);
            },
            errorHandler);
        return;
      }
    }

    List<CassAttributeDesc> targetAttributes = collectTargetAttributes(request, cassStream);

    final ViewCassStream viewCassStream =
        ((SignalCassStream) cassStream)
            .findViewForFilter(state.getFilter(), targetAttributes, request.getViews());
    if (viewCassStream != null) {
      state.addHistory("})(viewTableExtract{");
      viewTableExtract(
          state,
          request,
          targetAttributes,
          cassStream,
          viewCassStream,
          (events) -> {
            state.addHistory("})");
            acceptor.accept(events);
          },
          errorHandler);
      return;
    }

    state.addHistory("})(regularExtract{");
    regularExtract(
        state,
        request,
        targetAttributes,
        cassStream,
        (events) -> {
          state.addHistory("})");
          acceptor.accept(events);
        },
        errorHandler);
  }

  /**
   * A wrapper around extract() that returns a future with the results instead of taking an acceptor
   * function as input.
   */
  public CompletableFuture<List<Event>> extract(ExtractState state, CassStream cassStream)
      throws TfosException, ApplicationException {
    final CompletableFuture<List<Event>> future = new CompletableFuture<>();
    extract(
        state,
        cassStream,
        (events) -> {
          future.complete(events);
        },
        future::completeExceptionally);
    return future;
  }

  private List<CassAttributeDesc> collectTargetAttributes(
      ExtractRequest request, CassStream cassStream) throws TfosException {
    final List<CassAttributeDesc> attributes = new ArrayList<>();
    boolean getAllAttributes = request.getAttributes() == null;
    if (!getAllAttributes) {
      for (String attr : request.getAttributes()) {
        CassAttributeDesc desc = cassStream.getAttributeDesc(attr);
        if (desc == null) {
          throw new TfosException(EventExtractError.INVALID_ATTRIBUTE, attr);
        }
        attributes.add(desc);
      }
      for (View view : request.getViews()) {
        List<String> viewAttrs = view.getDimensions();
        if ((viewAttrs == null) || (viewAttrs.isEmpty())) {
          viewAttrs = new ArrayList<String>();
          String by = view.getBy();
          if (by != null) {
            viewAttrs.add(by);
          }
        }
        for (var viewAttr : viewAttrs) {
          CassAttributeDesc desc = cassStream.getAttributeDesc(viewAttr);
          if (desc != null && !attributes.contains(desc)) {
            attributes.add(desc);
          }
        }
      }
      if (request.getAggregates() != null) {
        for (Aggregate aggregate : request.getAggregates()) {
          if (aggregate.getBy() != null) {
            CassAttributeDesc desc = cassStream.getAttributeDesc(aggregate.getBy());
            if (desc == null) {
              throw new TfosException(EventExtractError.INVALID_ATTRIBUTE, aggregate.toString());
            }
            if (desc != null && !attributes.contains(desc)) {
              attributes.add(desc);
            }
          }
        }
      }
    } else {
      cassStream
          .getAttributeTable()
          .forEach(
              (name, desc) -> {
                attributes.add(desc);
              });
    }
    return attributes;
  }

  private void regularExtract(
      ExtractState state,
      ExtractRequest request,
      List<CassAttributeDesc> targetAttributes,
      CassStream cassStream,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler)
      throws TfosException, ApplicationException {
    state.addHistory("(regular");
    logger.debug(
        "USING SIGNAL TABLE tenant={}, stream={}, tableName={}, startTime={}",
        cassStream.getTenantName(),
        cassStream.getStreamName(),
        cassStream.getTableName(),
        StringUtils.tsToIso8601(request.getStartTime()));

    final List<QueryInfo> queries =
        cassStream.makeExtractStatements(
            state, targetAttributes, request.getStartTime(), request.getEndTime());

    // if there is nothing to return, end here.
    if (queries.isEmpty()) {
      acceptor.accept(Collections.emptyList());
      return;
    }

    state.endPreProcess();
    state.startDbAccess();

    // Index table does not event_id column, so buildEvent this info.
    boolean noEventIdColumn = (cassStream instanceof IndexCassStream ? true : false);

    BiosModules.getSharedProperties()
        .getPropertyCachedIntAsync(
            MAX_EXTRACT_CONCURRENCY,
            DEFAULT_MAX_EXTRACT_CONCURRENCY,
            state,
            (maxConcurrency) -> {
              final AsyncQueryExecutor executor =
                  new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());
              final List<Event>[] eventsList = new List[queries.size()];
              final var totalDataPoints = new AtomicInteger(0);
              final var canceled = new AtomicBoolean(false);
              if (queries.size() > 1) {
                state.getEventFactory().setConcurrent();
              }
              int index = 0;
              for (final QueryInfo query : queries) {
                executor.addStage(
                    new ExtractStage(
                        state,
                        query,
                        cassStream,
                        targetAttributes,
                        noEventIdColumn,
                        eventsList,
                        totalDataPoints,
                        canceled,
                        acceptor,
                        errorHandler,
                        index++));
              }
              executor.execute();
            },
            errorHandler::accept);
  }

  private void viewTableExtract(
      ExtractState state,
      ExtractRequest request,
      List<CassAttributeDesc> targetAttributes,
      CassStream cassStream,
      ViewCassStream viewCassStream,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler)
      throws InvalidFilterException {

    viewCassStream
        .makeExtractFilterWithViewStatements(
            state,
            targetAttributes,
            state.getFilter(),
            request.getStartTime(),
            request.getEndTime())
        .thenAccept(
            (queries) ->
                ExecutionHelper.run(
                    () -> {
                      final List<QueryInfo> additionalQueries;
                      if (request.isOnTheFly()) {
                        long lastQueryCoverage = request.getStartTime();
                        for (final QueryInfo query : queries) {
                          if (lastQueryCoverage < 0
                              || query.getTimeRange().getEnd() > lastQueryCoverage) {
                            lastQueryCoverage = query.getTimeRange().getEnd();
                          }
                        }
                        if (lastQueryCoverage < request.getEndTime()) {
                          additionalQueries =
                              cassStream.makeExtractStatements(
                                  state, targetAttributes, lastQueryCoverage, request.getEndTime());
                        } else {
                          additionalQueries = List.of();
                        }
                      } else {
                        additionalQueries = List.of();
                      }
                      BiosModules.getSharedProperties()
                          .getPropertyCachedIntAsync(
                              MAX_EXTRACT_CONCURRENCY,
                              DEFAULT_MAX_EXTRACT_CONCURRENCY,
                              state,
                              (maxConcurrency) -> {
                                executeQueryForViewTableExtract(
                                    queries,
                                    additionalQueries,
                                    targetAttributes,
                                    cassStream,
                                    maxConcurrency,
                                    state,
                                    acceptor,
                                    errorHandler);
                              },
                              errorHandler::accept);
                    }))
        .exceptionally(
            (t) -> {
              errorHandler.accept(t);
              return null;
            });
  }

  private void executeQueryForViewTableExtract(
      List<QueryInfo> queries,
      List<QueryInfo> additionalQueries,
      List<CassAttributeDesc> targetAttributes,
      CassStream cassStream,
      int maxConcurrency,
      ExtractState state,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler) {

    final var totalDataPoints = new AtomicInteger(0);
    final int totalNumQueries = queries.size() + additionalQueries.size();

    final AsyncQueryExecutor executor =
        new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());
    final List<Event>[] eventsList = new List[totalNumQueries];
    final var canceled = new AtomicBoolean(false);
    if (queries.size() > 1 || additionalQueries.size() > 1) {
      state.getEventFactory().setConcurrent();
    }

    // Response for the view table query may not be sorted by timestamp. The result should be
    // sorted first if not specified otherwise
    final var views = state.getInput().getViews();
    if (views == null
        || !views.stream()
            .anyMatch(
                (view) ->
                    view.getFunction() == ViewFunction.SORT
                        && ORDER_BY_TIMESTAMP.equalsIgnoreCase(view.getBy()))) {
      state.setInitialViews(List.of(new Sort(ORDER_BY_TIMESTAMP)));
    }

    state.startDbAccess();
    final var eventFactory = state.getEventFactorySupplier().get();
    int queryIteration = 0;
    for (final QueryInfo query : queries) {
      final int index = queryIteration++;
      eventsList[index] = new ArrayList<>();
      final var dbTimer = new AtomicReference<Optional<Timer>>();
      executor.addStage(
          new AsyncQueryStage() {
            @Override
            public Statement getStatement() {
              return query.getStatement();
            }

            @Override
            public void beforeQuery() {
              dbTimer.set(state.startStorageAccess());
              state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
            }

            @Override
            public void handleResult(ResultSet result) {
              final var rows = result.all();
              dbTimer.get().ifPresent((timer) -> timer.commit());
              state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, rows.size()));
              for (var row : rows) {
                if (canceled.get()) {
                  return;
                }
                final Event event = eventFactory.create();
                event.setEventId(row.getUUID(COL_EVENT_ID));
                final long timestamp = UUIDs.unixTimestamp(event.getEventId());
                event.setIngestTimestamp(new Date(timestamp));
                for (var targetAttribute : targetAttributes) {
                  final String attributeName = targetAttribute.getName();
                  final String columnName = targetAttribute.getColumn();
                  final Object value =
                      ViewCassStream.cassandraToDataEngine(
                          row.getObject(columnName), targetAttribute);
                  event.set(attributeName, value);
                }
                eventsList[index].add(event);
              }
            }

            @Override
            public void handleError(Throwable t) {
              canceled.set(true);
              errorHandler.accept(DataUtils.handleExtractError(t, query.getStatement()));
            }

            @Override
            public void handleCompletion() {
              var startPostDbTime = Instant.now();
              try {
                List<Event> allEvents = new ArrayList<>();
                for (var events : eventsList) {
                  allEvents.addAll(events);
                }
                var outEvents = postProcessEvents(state, targetAttributes, cassStream, allEvents);
                var timeTakenMicros =
                    Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
                state
                    .getQueryLoggerItem()
                    .ifPresent((item) -> item.addPostDb(timeTakenMicros, 1, outEvents.size()));
                acceptor.accept(outEvents);

              } catch (ApplicationException ex) {
                canceled.set(true); // this is not really necessary but just in case
                errorHandler.accept(DataUtils.handleExtractError(ex, null));
              }
            }
          });
    }

    if (!additionalQueries.isEmpty()) {
      final AsyncQueryExecutor additionalExecutor =
          new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());
      final boolean noEventIdColumn = false;
      for (var queryInfo : additionalQueries) {
        additionalExecutor.addStage(
            new ExtractStage(
                state,
                queryInfo,
                cassStream,
                targetAttributes,
                noEventIdColumn,
                eventsList,
                totalDataPoints,
                canceled,
                (events) -> {
                  if (queries.isEmpty()) {
                    acceptor.accept(events);
                  } else {
                    executor.execute();
                  }
                },
                errorHandler,
                queryIteration++) {
              @Override
              public void beforeQuery() {
                state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
              }

              @Override
              protected void resultReceived() {
                state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, events.size()));
              }

              @Override
              protected List<Event> finishExecution(List<Event> totalEvents)
                  throws ApplicationException {
                if (queries.isEmpty()) {
                  return super.finishExecution(totalEvents);
                } else {
                  return null;
                }
              }
            });
      }
      additionalExecutor.execute();
    } else if (queries.isEmpty()) {
      acceptor.accept(Collections.emptyList());
    } else {
      executor.execute();
    }
  }

  private View getView(ExtractRequest request) {
    final var views = request.getViews();
    if (views == null) {
      return null;
    }
    return views.isEmpty() ? null : views.get(0);
  }

  private static View getGroup(ExtractRequest request) {
    for (View view : request.getViews()) {
      if (view.getFunction() == ViewFunction.GROUP) {
        return view;
      }
    }
    return null;
  }

  private static class ExtractStage extends SignalQueryStage<ExtractState, List<Event>> {

    public ExtractStage(
        ExtractState state,
        QueryInfo queryInfo,
        CassStream cassStream,
        Collection<CassAttributeDesc> attributes,
        boolean noEventIdColumn,
        List<Event>[] eventsList,
        AtomicInteger totalDataPoints,
        AtomicBoolean canceled,
        Consumer<List<Event>> acceptor,
        Consumer<Throwable> errorHandler,
        int index) {
      super(
          state,
          queryInfo,
          cassStream,
          noEventIdColumn,
          eventsList,
          totalDataPoints,
          canceled,
          acceptor,
          errorHandler,
          index);
    }

    @Override
    protected boolean isForRollup() {
      return state.isRollupTask();
    }

    @Override
    public void beforeQuery() {
      state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
    }

    @Override
    protected void resultReceived() {
      state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, events.size()));
    }

    @Override
    protected List<Event> finishExecution(List<Event> totalEvents) throws ApplicationException {
      var startPostDbTime = Instant.now();
      if (state.isRollupTask()) {
        final View view = SignalExtractor.getGroup(state.getInput());
        final var eventFactory = state.getEventFactorySupplier().get();
        final Function<List<Event>, List<Event>> viewFunction =
            ExtractView.createView(
                view,
                state.getInput().getAttributes(),
                state.getInput().getAggregates(),
                cassStream,
                eventFactory::create);
        final var out = viewFunction.apply(totalEvents);
        var timeTakenMicros = Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
        state
            .getQueryLoggerItem()
            .ifPresent((item) -> item.addPostDb(timeTakenMicros, 1, out.size()));
        return out;
      } else {
        List<Event> outEvents;
        if (cassStream.getStreamDesc().getType() == StreamType.ROLLUP
            && cassStream.getStreamDesc().getFormatVersion() >= FormatVersion.REUSABLE_ROLLUP) {
          // Rollup stream needs an additional step to convolute the past data when horizon is
          // longer than interval.
          outEvents = ((RollupCassStream) cassStream).convolute(state, totalEvents);
        } else {
          outEvents = totalEvents;
        }

        final var out = postProcessEvents(state, attributes, cassStream, outEvents);
        var timeTakenMicros = Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
        state
            .getQueryLoggerItem()
            .ifPresent((item) -> item.addPostDb(timeTakenMicros, 1, out.size()));
        meterRawReads(out == null ? 0 : out.size());
        return out;
      }
    }
  }

  /**
   * Processes the records from the database and convert them to output events.
   *
   * @param state Extract execution state
   * @param queryTargetAttributes Query target attributes
   * @param cassStream CassStream of the target signal
   * @param rawEvents Records from the database
   * @return Processed reply-ready events
   * @throws ApplicationException thrown to indicate that an unexpected error happened
   */
  static List<Event> postProcessEvents(
      ExtractState state,
      Collection<CassAttributeDesc> queryTargetAttributes,
      CassStream cassStream,
      List<Event> rawEvents)
      throws ApplicationException {
    final var requestedAttributes = state.getInput().getAttributes();
    final var aggregates = state.getInput().getAggregates();

    List<Event> currentEvents = rawEvents;

    // check views
    final var views = new ArrayList<View>(state.getInitialViews());
    views.addAll(state.getInput().getViews());
    // if aggregates are requested but group dimensions are not set, it's "global" as a group
    if (aggregates != null && !aggregates.isEmpty()) {
      if (!views.stream().anyMatch((view) -> view.getFunction() == ViewFunction.GROUP)) {
        views.add(new Group());
      }
    }

    // change order of views if necessary; sort must come at the last
    for (int i = 1; i < views.size(); ++i) {
      var prev = views.get(i - 1);
      if (prev.getFunction() == ViewFunction.SORT) {
        views.set(i - 1, views.get(i));
        views.set(i, prev);
      }
    }

    // apply views
    final var eventFactory = state.getEventFactorySupplier().get();
    for (View view : views) {
      final Function<List<Event>, List<Event>> viewFunction =
          ExtractView.createView(
              view,
              state.getInput().getAttributes(),
              state.getInput().getAggregates(),
              cassStream,
              eventFactory::create);
      currentEvents = viewFunction.apply(currentEvents);
    }

    // limit number of rows if specified
    final var limit = state.getInput().getLimit();
    if (limit != null && currentEvents.size() > limit) {
      currentEvents = currentEvents.subList(0, limit);
    }

    // Return events after converting values from internal format to output format.
    StorageDataUtils.convertToOutputData(
        currentEvents, queryTargetAttributes, state.getTenantName(), state.getStreamName());
    return currentEvents;
  }

  /**
   * Method that validates an extract request.
   *
   * <p>The method runs following validations:
   *
   * <dl>
   *   <li>Whether the view is specified properly if any
   *   <li>Whether the specified attribute names are correct
   *   <li>Whether aggregates are specified properly
   *   <li>Whether filter is properly set if any
   * </dl>
   *
   * @param cassStream Cassandra stream info to be used for validation.
   * @return The method returns null when the given view is valid. Error message otherwise.
   * @throws TfosException For invalid request
   * @throws ApplicationException For server error
   */
  protected static String validateExtractRequest(ExtractState state, CassStream cassStream)
      throws TfosException, ApplicationException {
    final ExtractRequest request = state.getInput();
    if (request.getViews() != null) {
      for (View view : request.getViews()) {
        String err = ExtractView.validateView(view, cassStream);
        if (err != null) {
          return err;
        }
      }
    }
    if (request.getAttributes() != null) {
      for (String attribute : request.getAttributes()) {
        if (cassStream.getAttributeDesc(attribute) == null) {
          return String.format("Attribute '%s' is not in stream attributes", attribute);
        }
      }
    }
    final Set<String> eventAttributeKeys = new HashSet<>();
    String err =
        ExtractView.validateAggregates(request.getAggregates(), cassStream, eventAttributeKeys);
    if (err != null) {
      return err;
    }

    // verify limit
    if (request.getLimit() != null && request.getLimit() < 0) {
      throw new InvalidValueException(
          String.format("limit=%d: the value must be positive", request.getLimit()));
    }

    ExtractView.validateFilter(request.getFilter(), state, cassStream);
    return null;
  }

  /**
   * Method to resolve view description by specified dimensions, attributes, and aggregates.
   *
   * @param dimensions Set of attributes to be used for view.
   * @param attributes Specified attributes
   * @param aggregates Specified aggregates
   * @return Resolved view description if there is any match, null otherwise.
   */
  private ViewDesc resolveView(
      ExtractState state,
      List<String> dimensions,
      List<String> attributes,
      List<? extends Aggregate> aggregates) {
    StreamDesc signal = state.getStreamDesc();
    // no chance if the signal does not have view config
    if (signal.getViews() == null) {
      return null;
    }

    // These sets are to avoid O(n^2) and to take care of case insensitivity
    final Set<String> requiredDimensions = new HashSet<>();
    // TODO(TFOS-1080): Better approach is to resolve ViewCassStream when dimensions match and
    // to check attributes coverage by using ViewCassStream.getAttributeDesc(). That approach does
    // not require set requiredAttributes. In the same manner, we may want to add dimension resolver
    // to ViewCassStream for eliminating requiredDimensions.
    final Set<String> requiredAttributes = new HashSet<>();
    // Populate the sets
    dimensions.forEach(dimension -> requiredDimensions.add(dimension.toLowerCase()));
    if (attributes != null) {
      attributes.forEach(attribute -> requiredAttributes.add(attribute.toLowerCase()));
    } else {
      signal
          .getAttributes()
          .forEach(attrDesc -> requiredAttributes.add(attrDesc.getName().toLowerCase()));
    }
    if (aggregates != null) {
      for (Aggregate aggregate : aggregates) {
        if (aggregate.getFunction() != MetricFunction.COUNT) {
          requiredAttributes.add(aggregate.getBy().toLowerCase());
        }
      }
    }

    // find a view that meets the requirement
    ViewDesc match = null;
    int tableLength = Integer.MAX_VALUE;
    for (ViewDesc current : signal.getViews()) {
      final List<String> providedDimensions = current.getGroupBy();
      final List<String> providedAttributes = current.getAttributes();
      // no chance if the view is smaller than the request
      if (providedDimensions.size() < requiredDimensions.size()) {
        continue;
      }
      if (providedAttributes.size() < requiredAttributes.size()) {
        continue;
      }
      int count = 0;
      int index = 0;
      while (count < requiredDimensions.size() && index < providedDimensions.size()) {
        if (requiredDimensions.contains(providedDimensions.get(index++).toLowerCase())) {
          ++count;
        }
      }
      if (count < requiredDimensions.size()) {
        continue;
      }
      count = 0;
      index = 0;
      while (count < requiredAttributes.size() && index < providedAttributes.size()) {
        if (requiredAttributes.contains(providedAttributes.get(index++).toLowerCase())) {
          ++count;
        }
      }
      if (count == requiredAttributes.size()) {
        int length = providedDimensions.size() + providedAttributes.size();
        if (length < tableLength) {
          match = current;
          tableLength = length;
        }
      }
    }
    return match;
  }

  private void extractWithGroupView(
      ExtractState state,
      CassStream cassStream,
      ViewDesc viewDesc,
      List<String> dimensions,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler)
      throws ApplicationException, TfosException {

    state.addHistory("viewExtract");
    logger.debug(
        "USING VIEW TABLE tenant={}, stream={}, tableName={}, view={}",
        cassStream.getTenantName(),
        cassStream.getStreamName(),
        cassStream.getTableName(),
        viewDesc.getName());

    // prepare necessary objects
    final StreamDesc signalDesc = state.getStreamDesc();
    final TenantDesc tenantDesc = signalDesc.getParent();
    final String viewConfigName = AdminUtils.makeViewStreamName(signalDesc, viewDesc);
    StreamDesc viewConfig = tenantDesc.getStream(viewConfigName, false);
    if (viewConfig == null) {
      // This should not happen by design
      throw new ApplicationException("view stream not found");
    }
    final String indexConfigName =
        AdminUtils.makeIndexStreamName(signalDesc.getName(), viewDesc.getName());
    StreamDesc indexConfig = tenantDesc.getStream(indexConfigName, false);
    if (indexConfig == null) {
      // This should not happen by design
      throw new ApplicationException("index stream not found");
    }
    CassStream indexCassStream = dataEngine.getCassStream(indexConfig);

    // Search index first
    state.addHistory("makeIndexQueryStatements");

    final ExtractRequest request = state.getInput();
    final long start = request.getStartTime();
    final long end = request.getEndTime();
    final List<QueryInfo> queries = indexCassStream.makeExtractStatements(state, null, start, end);

    logger.debug(
        "Queries count # {} to fetch from index={} range[{} - {}]",
        queries.size(),
        indexConfigName,
        StringUtils.tsToIso8601(start),
        StringUtils.tsToIso8601(end));

    // if there is nothing to return, end here.
    if (queries.isEmpty()) {
      acceptor.accept(Collections.emptyList());
      return;
    }

    ViewCassStream viewCassStream = (ViewCassStream) dataEngine.getCassStream(viewConfig);

    state.addHistory("executeIndexQueries");
    state.startDbAccess();

    BiosModules.getSharedProperties()
        .getPropertyCachedIntAsync(
            MAX_EXTRACT_CONCURRENCY,
            DEFAULT_MAX_EXTRACT_CONCURRENCY,
            state,
            (maxConcurrency) ->
                executeQueryWithGroupView(
                    queries,
                    start,
                    end,
                    cassStream,
                    indexCassStream,
                    viewCassStream,
                    dimensions,
                    attributes,
                    aggregates,
                    maxConcurrency,
                    state,
                    acceptor,
                    errorHandler),
            errorHandler::accept);
  }

  private void executeQueryWithGroupView(
      List<QueryInfo> queries,
      long start,
      long end,
      CassStream cassStream,
      CassStream indexCassStream,
      ViewCassStream viewCassStream,
      List<String> dimensions,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      int maxConcurrency,
      ExtractState state,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler) {
    final AsyncQueryExecutor executor =
        new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());

    final Queue<Statement> iviewStatements = new ConcurrentLinkedQueue<>();

    int queryIteration = 0;
    for (final QueryInfo query : queries) {
      final int index = queryIteration++;
      executor.addStage(
          new AsyncQueryStage() {
            @Override
            public Statement getStatement() {
              return query.getStatement();
            }

            @Override
            public void beforeQuery() {
              state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
            }

            @Override
            public void handleResult(ResultSet result) {
              state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, 0));
              long numDbRows =
                  handleIndexQueryResult(
                      state,
                      result,
                      attributes,
                      aggregates,
                      start,
                      end,
                      acceptor,
                      errorHandler,
                      indexCassStream,
                      viewCassStream,
                      iviewStatements);
              state.getQueryLoggerItem().ifPresent((item) -> item.addDbRowsRead(numDbRows));
            }

            @Override
            public void handleError(Throwable t) {
              errorHandler.accept(DataUtils.handleExtractError(t, query.getStatement()));
            }

            @Override
            public void handleCompletion() {
              if (iviewStatements.isEmpty()) {
                acceptor.accept(Collections.emptyList());
              } else {
                readIviewTable(
                    state,
                    cassStream,
                    iviewStatements,
                    viewCassStream,
                    dimensions,
                    attributes,
                    aggregates,
                    acceptor,
                    errorHandler);
              }
            }
          });
    }
    state.endPreProcess();
    executor.execute();
  }

  private long handleIndexQueryResult(
      ExtractState state,
      ResultSet result,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      long start,
      long end,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler,
      CassStream indexCassStream,
      ViewCassStream viewCassStream,
      Queue<Statement> iviewStatements) {
    state.addHistory("makeViewQueryStatements");

    List<String> partitionKeyColumns = new ArrayList<>();
    indexCassStream
        .getAttributeTable()
        .entrySet()
        .forEach(entry -> partitionKeyColumns.add(entry.getValue().getColumn()));

    long numRows = 0;
    while (!result.isExhausted()) {
      ++numRows;
      Row row = result.one();
      // build partition keys for ingest view table
      List<Object> partitionKeys = new ArrayList<>();
      partitionKeys.add(row.getObject(COL_TIME_INDEX));
      partitionKeyColumns.forEach(name -> partitionKeys.add(row.getObject(name)));
      final Statement statement =
          viewCassStream.makeGroupViewExtractStatement(
              partitionKeyColumns, partitionKeys, attributes, aggregates, start, end);
      if (statement != null) {
        iviewStatements.offer(statement);
      }
    }

    logger.debug(
        "From index={} rows={} fetched, range[{} - {}]",
        indexCassStream.getStreamName(),
        numRows,
        StringUtils.tsToIso8601(start),
        StringUtils.tsToIso8601(end));
    return numRows;
  }

  private void readIviewTable(
      ExtractState state,
      CassStream cassStream,
      Collection<Statement> iviewStatements,
      ViewCassStream viewCassStream,
      List<String> dimensions,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler) {

    state.addHistory("executeViewQueries{(prep");
    BiosModules.getSharedProperties()
        .getPropertyCachedIntAsync(
            MAX_EXTRACT_CONCURRENCY,
            DEFAULT_MAX_EXTRACT_CONCURRENCY,
            state,
            (maxConcurrency) ->
                readIviewTableCore(
                    state,
                    cassStream,
                    iviewStatements,
                    viewCassStream,
                    dimensions,
                    attributes,
                    aggregates,
                    maxConcurrency,
                    acceptor,
                    errorHandler),
            errorHandler::accept);
  }

  private void readIviewTableCore(
      ExtractState state,
      CassStream cassStream,
      Collection<Statement> iviewStatements,
      ViewCassStream viewCassStream,
      List<String> dimensions,
      List<String> attributes,
      List<? extends Aggregate> aggregates,
      int maxConcurrency,
      Consumer<List<Event>> acceptor,
      Consumer<Throwable> errorHandler) {

    state.addHistory(")(query");
    // key: view scope -- values of view dimensions, value: events
    final Map<List<Object>, Event> viewEntries = new ConcurrentHashMap<>();

    final AsyncQueryExecutor executor =
        new AsyncQueryExecutor(session, maxConcurrency, state.getExecutor());

    // Don't start from 0 to avoid conflict with previously used numbers.
    int queryIteration = 100000000;
    for (Statement statement : iviewStatements) {
      final int index = queryIteration++;
      executor.addStage(
          new AsyncQueryStage() {
            @Override
            public Statement getStatement() {
              return statement;
            }

            @Override
            public void beforeQuery() {
              state.getQueryLoggerItem().ifPresent((item) -> item.startDb(index));
            }

            @Override
            public void handleResult(ResultSet result) {
              state.getQueryLoggerItem().ifPresent((item) -> item.endDb(index, 0));
              long numDbRows = 0;
              while (!result.isExhausted()) {
                final Row row = result.one();
                numDbRows++;
                viewCassStream.handleGroupViewQueryRow(
                    viewEntries, dimensions, row, attributes, aggregates, state, true);
              }
              final long finalNumDbRows = numDbRows;
              state.getQueryLoggerItem().ifPresent((item) -> item.addDbRowsRead(finalNumDbRows));
            }

            @Override
            public void handleCompletion() {
              state.endDbAccess();
              var startPostDbTime = Instant.now();
              final ExtractRequest request = state.getInput();
              final List<Event> events = new ArrayList<>();
              viewEntries.entrySet().forEach(entry -> events.add(entry.getValue()));
              if (!state.isRollupTask()) {
                final var group = SignalExtractor.getGroup(request);
                if (group != null) {
                  Comparator<Event> comparator =
                      ExtractView.generateComparator(group, viewCassStream);
                  Collections.sort(events, comparator);
                }
                List<AttributeDesc> attrDescs = new ArrayList<>();
                if (group != null && group.getDimensions() != null) {
                  for (String attr : group.getDimensions()) {
                    attrDescs.add(cassStream.getAttributeDesc(attr));
                  }
                } else {
                  if (request.getViews() != null) {
                    request
                        .getViews()
                        .forEach(
                            (view) -> attrDescs.add(cassStream.getAttributeDesc(view.getBy())));
                  }
                }
                if (request.getAttributes() != null) {
                  for (String attr : request.getAttributes()) {
                    attrDescs.add(cassStream.getAttributeDesc(attr));
                  }
                }
                StorageDataUtils.convertToOutputData(
                    events, attrDescs, cassStream.getTenantName(), cassStream.getStreamName());
              }
              int rowsToReturn = events.size();
              if (request.getLimit() != null) {
                rowsToReturn = Math.min(events.size(), request.getLimit());
              }
              var timeTakenMicros =
                  Duration.between(startPostDbTime, Instant.now()).toNanos() / 1000;
              final int finalRowsToReturn = rowsToReturn;
              state
                  .getQueryLoggerItem()
                  .ifPresent((item) -> item.addPostDb(timeTakenMicros, 1, finalRowsToReturn));
              acceptor.accept(events.subList(0, rowsToReturn));
            }

            @Override
            public void handleError(Throwable t) {
              errorHandler.accept(t);
            }
          });
    }

    logger.debug(
        "Queries count # {} to fetch from view={} range[{} - {}]",
        iviewStatements.size(),
        viewCassStream.getStreamName(),
        StringUtils.tsToIso8601(state.getInput().getStartTime()),
        StringUtils.tsToIso8601(state.getInput().getEndTime()));

    executor.execute();
  }
}
