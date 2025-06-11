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
package io.isima.bios.server.handlers;

import static io.isima.bios.common.BiosConstants.STREAM_ALL_CLIENT_METRICS;
import static io.isima.bios.common.BiosConstants.STREAM_CLIENT_METRICS;
import static io.isima.bios.common.BiosConstants.TENANT_SYSTEM;
import static io.isima.bios.service.Keywords.X_BIOS_STREAM_VERSION;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.codec.proto.messages.UuidMessageConverter;
import io.isima.bios.codec.proto.wrappers.ProtoSelectQuery;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.Constants;
import io.isima.bios.common.TfosConfig;
import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.data.DataEngine;
import io.isima.bios.data.ReplaceContextAttributesSpec;
import io.isima.bios.data.UpdateContextEntrySpec;
import io.isima.bios.data.impl.DataEngineImpl;
import io.isima.bios.data.impl.models.ContextOpOption;
import io.isima.bios.data.payload.CsvParser;
import io.isima.bios.dto.AllContextSynopses;
import io.isima.bios.dto.ContextSynopsis;
import io.isima.bios.dto.GetContextEntriesResponse;
import io.isima.bios.dto.MultiContextEntriesSpec;
import io.isima.bios.dto.MultiGetResponse;
import io.isima.bios.dto.MultiGetSpec;
import io.isima.bios.dto.PutContextEntriesRequest;
import io.isima.bios.dto.ReplaceContextAttributesRequest;
import io.isima.bios.dto.SelectContextEntriesResponse;
import io.isima.bios.dto.SelectContextRequest;
import io.isima.bios.dto.SynopsisRequest;
import io.isima.bios.dto.UpdateContextEntryRequest;
import io.isima.bios.dto.bulk.InsertBulkErrorResponse;
import io.isima.bios.errors.BiosError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.DataValidationError;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.exceptions.InsertBulkFailedServerException;
import io.isima.bios.execution.AsyncExecutionStage;
import io.isima.bios.execution.ConcurrentExecutionController;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.AtomicOperationSpec;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.ContextEntryRecord;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.FeatureRefreshRequest;
import io.isima.bios.models.FeatureStatusRequest;
import io.isima.bios.models.FeatureStatusResponse;
import io.isima.bios.models.InsertBulkEachResult;
import io.isima.bios.models.InsertResponseRecord;
import io.isima.bios.models.NodeType;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SelectResponseRecords;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.isql.SelectStatement;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.recorder.ContextRequestType;
import io.isima.bios.recorder.SignalRequestType;
import io.isima.bios.service.handler.DataServiceUtils;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.stats.ClockProvider;
import io.isima.bios.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataServiceHandler extends Bios2ServiceHandler {
  static final Logger logger = LoggerFactory.getLogger(DataServiceHandler.class);
  private static final int INSERTION_TIMEOUT_SECONDS = 30; // TODO(Naoki): Make this configurable
  private static final int SELECT_TIMEOUT_SECONDS = 300; // TODO(Naoki): Make this configurable

  private final OperationMetrics metrics;
  private final SharedConfig sharedConfig;

  public DataServiceHandler(
      DataEngine dataEngine,
      AdminInternal admin,
      OperationMetrics metrics,
      SharedConfig sharedConfig) {
    super(dataEngine, admin);
    this.metrics = metrics;
    this.sharedConfig = sharedConfig;
  }

  public DataEngine getDataEngine() {
    return dataEngine;
  }

  /**
   * Handles an Insert request.
   *
   * @param sessionToken The session token, a DATA_WRITE role is required.
   * @param atomicOperationSpec Optional atomic operation spec
   * @param state The insertion execution state
   * @return Future for the insert response record
   */
  public CompletableFuture<InsertResponseRecord> insertSignal(
      SessionToken sessionToken, AtomicOperationSpec atomicOperationSpec, InsertState state) {
    final var tenantName = state.getTenantName();
    return validateTokenAllowDelegation(sessionToken, tenantName, AllowedRoles.DATA_WRITE, state)
        .thenCompose(this::prepareInsertion)
        .thenApply((st) -> setUpAtomicOperation(atomicOperationSpec, st))
        .thenComposeAsync(dataEngine::insert, state.getExecutor())
        .thenComposeAsync(
            (resp) ->
                completeAtomicOperationIfEnabled(state)
                    .thenApply(
                        (none) -> {
                          state.startPostProcess();
                          return resp;
                        }),
            state.getExecutor())
        .orTimeout(INSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .exceptionally(
            (t) -> {
              abortAtomicOperationIfEnabled(state);
              return handleAsyncError(t, state);
            });
  }

  private CompletionStage<InsertState> prepareInsertion(InsertState state) {
    state.addHistory("(validateReq");
    state.setRequestType(SignalRequestType.INSERT);
    final var request = verifyNotNull(state.getRequest(), "Request was not provided");
    if (request.getContentRep() != DataProto.ContentRepresentation.CSV) {
      throw new CompletionException(
          new InvalidRequestException(
              "ContentRepresentation " + request.getContentRep().name() + " is not supported"));
    }
    try {
      findStreamDescAndSetUpMetricsRecorder(state, true, StreamType.SIGNAL);
    } catch (InvalidRequestException e) {
      return CompletableFuture.failedStage(e);
    }
    state.addHistory(")");

    final var recordSrc = request.getRecord();
    final var eventId = state.getEventId();

    return prepareInsertionCore(recordSrc, eventId, state);
  }

  /**
   * Handles an InsertBulk request.
   *
   * @param sessionToken The session token, a DATA_WRITE role is required
   * @param state InsertBulk execution state
   * @return Future for InsertBulk responses
   */
  public CompletableFuture<List<InsertBulkEachResult>> insertBulk(
      SessionToken sessionToken, AtomicOperationSpec atomicOperationSpec, InsertBulkState state) {
    return validateTokenAllowDelegation(
            sessionToken, state.getTenantName(), AllowedRoles.DATA_WRITE, state)
        .thenApply(this::prepareBulkInsertion)
        .thenApply((st) -> setUpAtomicOperation(atomicOperationSpec, st))
        .thenComposeAsync(this::insertBulkCore, state.getExecutor())
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  /**
   * Handles an InsertBulk request.
   *
   * @param sessionToken The session token, a DATA_WRITE role is required
   * @param state InsertBulk execution state
   * @return Future for InsertBulk responses
   */
  public CompletableFuture<List<InsertBulkEachResult>> insertClientMetrics(
      SessionToken sessionToken, InsertBulkState state) {
    return validateTokenAsync(sessionToken, state.getTenantName(), AllowedRoles.ANY, state)
        .thenCompose(
            (state1) -> {
              state.addHistory("(sanityCheck");
              final DataProto.InsertBulkRequest request =
                  verifyNotNull(state.getRequest(), "Request was not provided");
              final String tenantName =
                  verifyNotNull(state.getTenantName(), "Path parameter tenantName is required");
              verifyNotNull(request.getSignal(), "Request parameter signalName is required");

              final var origRequest = state.getRequest();

              final var allMetricsRequestBuilder = DataProto.InsertBulkRequest.newBuilder();
              allMetricsRequestBuilder
                  .setSignal(STREAM_ALL_CLIENT_METRICS)
                  .setContentRep(origRequest.getContentRep());
              final DataProto.InsertBulkRequest.Builder metricsRequestBuilder;
              if (TENANT_SYSTEM.equalsIgnoreCase(tenantName)) {
                metricsRequestBuilder = null;
              } else {
                metricsRequestBuilder = DataProto.InsertBulkRequest.newBuilder();
                metricsRequestBuilder
                    .setSignal(STREAM_CLIENT_METRICS)
                    .setContentRep(origRequest.getContentRep());
              }
              origRequest.getRecordList().stream()
                  .filter((x) -> x.getEventId().size() > 0 && x.getStringValuesCount() > 0)
                  .forEach(
                      (record) -> {
                        String origCsv = record.getStringValues(0);
                        final var origElements = origCsv.split(",");
                        final var allMetrics = new StringJoiner(",").add(tenantName);
                        final var metrics = new StringJoiner(",");
                        // If the client is new and has sent the 3 qosRetry* numbers, remove them -
                        // they
                        // are for system tenant only.
                        // It is safe to assume that all comma characters are the CSV separators
                        // (and not part of a string) since we do not allow commas in any of the
                        // names.
                        int numAttributes =
                            Math.min(
                                origElements.length,
                                BiosConstants.CLIENT_METRICS_NUM_ATTRIBUTES_BEFORE_QOS);
                        int i;
                        for (i = 0; i < numAttributes; ++i) {
                          allMetrics.add(origElements[i]);
                          metrics.add(origElements[i]);
                        }
                        numAttributes =
                            Math.max(
                                origElements.length,
                                BiosConstants.CLIENT_METRICS_NUM_ATTRIBUTES_BEFORE_QOS + 3);
                        for (; i < numAttributes; ++i) {
                          // If the client is old and has not sent the 3 qosRetry* numbers
                          // we need to add 3 0s at the end for STREAM_ALL_CLIENT_METRICS.
                          allMetrics.add(i < origElements.length ? origElements[i] : "0");
                        }
                        allMetricsRequestBuilder.addRecord(
                            DataProto.Record.newBuilder()
                                .setEventId(record.getEventId())
                                .addStringValues(allMetrics.toString()));
                        if (metricsRequestBuilder != null) {
                          metricsRequestBuilder.addRecord(
                              DataProto.Record.newBuilder()
                                  .setEventId(record.getEventId())
                                  .addStringValues(metrics.toString()));
                        }
                      });

              final var allMetricsState =
                  new InsertBulkState(TENANT_SYSTEM, allMetricsRequestBuilder.build(), state);
              final var fut1 =
                  CompletableFuture.completedFuture(allMetricsState)
                      .thenApply(this::prepareBulkInsertion)
                      .thenComposeAsync(this::insertBulkCore, state.getExecutor());

              if (metricsRequestBuilder != null) {
                final var metricsState =
                    new InsertBulkState(tenantName, metricsRequestBuilder.build(), state);
                final var fut2 =
                    CompletableFuture.completedFuture(metricsState)
                        .thenApply(this::prepareBulkInsertion)
                        .thenComposeAsync(this::insertBulkCore, state.getExecutor());
                return CompletableFuture.allOf(fut1, fut2);
              }
              return CompletableFuture.allOf(fut1);
            })
        .thenApply((none) -> List.of());
  }

  private InsertBulkState prepareBulkInsertion(InsertBulkState bulkState) {
    // verify the request
    bulkState.addHistory("(prepare");
    bulkState.addHistory("(sanityCheck");
    bulkState.setRequestType(SignalRequestType.INSERT_BULK);
    final DataProto.InsertBulkRequest request =
        verifyNotNull(bulkState.getRequest(), "Request was not provided");
    verifyNotNull(bulkState.getTenantName(), "Path parameter tenantName is required");
    final String signalName =
        verifyNotNull(request.getSignal(), "Request parameter signalName is required");
    bulkState.setStreamName(signalName);
    if (request.getContentRep() != DataProto.ContentRepresentation.CSV) {
      throw new CompletionException(
          new InvalidRequestException(
              "ContentRepresentation " + request.getContentRep().name() + " is not supported"));
    }
    logger.debug("SIGNAL={}", signalName);
    bulkState.addHistory(")(getSignal");
    try {
      findStreamDescAndSetUpMetricsRecorder(bulkState, true, StreamType.SIGNAL);
    } catch (InvalidRequestException e) {
      throw new CompletionException(e);
    }
    bulkState.addHistory(")");
    bulkState.setSkipCountingErrors(true); // errors are count individually
    return bulkState;
  }

  private CompletableFuture<List<InsertBulkEachResult>> insertBulkCore(InsertBulkState bulkState) {
    bulkState.addHistory("(insertBulk");
    final var request = bulkState.getRequest();
    final var tenantName = bulkState.getTenantName();
    final var signalName = bulkState.getStreamName();
    final var signalDesc = bulkState.getSignalDesc();

    final var concurrentExecutionBuilder =
        ConcurrentExecutionController.newBuilder()
            .concurrency(Bios2Config.insertBulkMaxConcurrency());

    for (int i = 0; i < request.getRecordList().size(); ++i) {
      final var eachState =
          new InsertState(tenantName, signalName, signalDesc, dataEngine, bulkState);
      eachState.setAtomicOperationContext(bulkState.getAtomicOperationContext());

      // result placeholder; the value would be filled on completion
      final var result = new InsertBulkEachResult();
      bulkState.addResult(result);

      // This async execution stage does individual insertion
      final int index = i;
      final var stage =
          new AsyncExecutionStage<>(eachState) {
            @Override
            public CompletionStage<Void> runAsync() {
              final var recordSrc = request.getRecordList().get(index);
              return prepareEachInsertion(recordSrc, eachState)
                  .thenCompose(dataEngine::insert)
                  .thenAccept(result::setSuccess)
                  .exceptionally(
                      (t) -> {
                        state.markError();
                        bulkState.markPartialError();
                        final var cause = (t instanceof CompletionException) ? t.getCause() : t;
                        insertOperationFailureEvents(eachState, cause);
                        if (cause instanceof TfosException) {
                          result.setError(t.getCause());
                          state.addError(cause instanceof DataValidationError);
                          final var ex = (TfosException) cause;
                          ex.replaceMessage(ex.getMessage() + state.makeErrorContext());
                          return null;
                        }
                        throw (t instanceof CompletionException)
                            ? (CompletionException) t
                            : new CompletionException(t);
                      });
            }

            /**
             * Method to parse an insert request record and convert it to a DefaultRecord.The
             * generated record is put into the state of this stage.
             *
             * @return The state of this stage
             */
            private CompletionStage<InsertState> prepareEachInsertion(
                DataProto.Record recordSrc, InsertState state) {
              final var eventId =
                  UuidMessageConverter.fromProtoUuid(
                      verifyNotNull(
                          recordSrc.getEventId(), "Required property eventId is missing"));
              return prepareInsertionCore(recordSrc, eventId, state);
            }
          };

      concurrentExecutionBuilder.addStage(stage);
    }

    return concurrentExecutionBuilder
        .build()
        .execute()
        .thenComposeAsync(
            (none) -> {
              if (!bulkState.partialErrorHappened()) {
                return completeAtomicOperationIfEnabled(bulkState);
              }
              return CompletableFuture.completedFuture(null);
            },
            bulkState.getExecutor())
        .thenApplyAsync(
            (none) -> {
              bulkState.addHistory(")");
              if (bulkState.partialErrorHappened()) {
                DataEngineImpl.throwInsertBulkError(bulkState.getResults());
              }
              return bulkState.getResults();
            },
            bulkState.getExecutor())
        .exceptionally(
            (t) -> {
              abortAtomicOperationIfEnabled(bulkState);
              final var cause = (t instanceof CompletionException) ? t.getCause() : t;
              throw new CompletionException(cause);
            });
  }

  /**
   * Error handler of InsertBulk operation.
   *
   * <p>The success/failure results are accumulated in bulkState.results at the end of each
   * insertion stage, rather than aborting entire operation on error. This method iterates the
   * results list, collects errors, and determines error type and message to be returned to the
   * client.
   *
   * <p>The result would always end up with InsertBulkFailedServerException. Error type may vary
   * with following rule:
   *
   * <ul>
   *   <li>If one of the collected exception is not an instance of TfosException, overall error
   *       would be APPLICATION_ERROR
   *   <li>Otherwise the first error type would be picked up and returned to the client
   * </ul>
   *
   * @param results BulkInsert execution results
   */
  private void throwInsertBulkError(List<InsertBulkEachResult> results) {
    BiosError overall = null;
    int indexCount = 0;
    final var indexSb = new StringBuilder();
    String delimiter = "";
    for (int i = 0; i < results.size(); ++i) {
      final var result = results.get(i);
      if (!result.isError()) {
        continue;
      }
      if (indexCount < 5) {
        indexSb.append(delimiter).append(i);
        delimiter = ", ";
      } else if (indexCount == 5) {
        indexSb.append(", ...");
      }
      // Update the overall error, takes the first one, but a non-TFOS error flips it to 500
      if (!result.isTfosError()) {
        overall = GenericError.APPLICATION_ERROR;
      } else if (overall == null) {
        overall = ((TfosException) result.getError());
      }
      ++indexCount;
    }
    if (overall == null) {
      // the results are empty; likely to be some serious bug
      overall = GenericError.APPLICATION_ERROR;
    }

    // make the exception and throw
    final var message =
        String.format(
            "Failed to insert %d/%d records (%s). For details, check property 'resultsWithError'",
            indexCount, results.size(), indexSb);
    final InsertBulkErrorResponse response = new InsertBulkErrorResponse(overall, message, results);
    final var e = new InsertBulkFailedServerException(overall, message, response, true);
    throw new CompletionException(e);
  }

  private CompletionStage<InsertState> prepareInsertionCore(
      DataProto.Record recordSrc, UUID eventId, InsertState state) {
    state.startPreProcess();
    state.addHistory("(parse");
    final var signalDesc = state.getSignalDesc();
    final var event = new EventJson().setEventId(eventId);
    if (recordSrc.getStringValuesCount() < 1) {
      return CompletableFuture.failedStage(
          new InvalidRequestException(String.format("Request does not contain the event text")));
    }
    try {
      CsvParser.parse(recordSrc.getStringValues(0), signalDesc.getBiosAttributes(), event);
      DataServiceUtils.validateIncomingEvent(signalDesc, event);
    } catch (TfosException e) {
      state.addRecordsWritten(1);
      return CompletableFuture.failedStage(e);
    }
    long timestamp = event.getIngestTimestamp() != null ? event.getIngestTimestamp().getTime() : 0;
    if (timestamp == 0) {
      if (event.getEventId().version() == 1) {
        timestamp = Utils.uuidV1TimestampInMillis(event.getEventId());
      } else {
        timestamp = System.currentTimeMillis();
      }
      event.setIngestTimestamp(new Date(timestamp));
    }
    state.addHistory(")");

    // client metrics data needs special cleansing
    if (signalDesc.isClientMetrics()) {
      state.addHistory("(cleanseData");
      DataServiceUtils.cleanseClientMetricsData(signalDesc, admin, event);
      state.addHistory(")");
    }

    state.setEvent(event);
    var stage = CompletableFuture.completedFuture(state);
    var preprocessors = state.getSignalDesc().getPreprocessStages();
    if (preprocessors != null) {
      state.addHistory("{preprocesses[");
      for (var processor : preprocessors) {
        stage = stage.thenCompose(processor.getProcess()::applyAsync);
      }
    }
    return stage.thenApply(
        (state1) -> {
          state.addHistory("]");
          // preprocess will be ended later in the DataEngine
          // state.endPreProcess();
          return state1;
        });
  }

  /**
   * Method to handle MultiSelect operation request.
   *
   * @param sessionToken The session token
   * @param multiState The execution state of the operation
   * @return Future for list of select response records
   */
  public CompletableFuture<List<SelectResponseRecords>> multiSelect(
      SessionToken sessionToken, MultiSelectState multiState) {
    final var multiSelectRequest = multiState.getRequest();
    final int numQueries = multiSelectRequest.getQueriesCount();
    final var forkedStates = new SelectState[numQueries];
    multiState.clearBytesWritten();
    return validateTokenAllowDelegation(
            sessionToken, multiState.getTenantName(), AllowedRoles.SIGNAL_DATA_READ, multiState)
        .thenCompose(
            (state1) ->
                ExecutionHelper.supply(
                    () -> {
                      state1.addHistory("(multiSelect");
                      final var tenantName = multiState.getTenantName();
                      DataServiceUtils.validateNumMultiSelectQueries(tenantName, numQueries);
                      final var builder = ConcurrentExecutionController.newBuilder();
                      for (int index = 0; index < numQueries; ++index) {
                        final String executionName = "Select[" + index + "]";
                        final SelectStatement query =
                            new ProtoSelectQuery(state1.getRequest().getQueries(index));
                        // final var signalDesc = admin.getStream(tenantName, query.getFrom());
                        final var forkedState =
                            new SelectState(
                                executionName,
                                tenantName,
                                query,
                                index,
                                state1.getClientVersion(),
                                state1);
                        if (multiState.getMetricsTracer() != null) {
                          forkedState.setMetricsTracer(new OperationMetricsTracer(0));
                          multiState
                              .getMetricsTracer()
                              .addSubTracer(forkedState.getMetricsTracer());
                        }
                        findStreamDescAndSetUpMetricsRecorder(
                            forkedState, false, StreamType.SIGNAL);

                        forkedStates[index] = forkedState;
                        builder.addStage(
                            new AsyncExecutionStage<>(forkedState) {
                              @Override
                              public CompletionStage<Void> runAsync() {
                                return dataEngine
                                    .select(state)
                                    .exceptionally((t) -> handleAsyncError(t, state));
                              }
                            });
                      }
                      return builder
                          .build()
                          .execute()
                          .thenApplyAsync(
                              (none) -> {
                                final var results = collectSelectResults(multiState, forkedStates);
                                final var now = ClockProvider.getClock().instant();
                                for (var forkedState : forkedStates) {
                                  forkedState
                                      .getQueryLoggerItem()
                                      .ifPresent(
                                          (item) -> {
                                            item.responseSent("Ok", now);
                                            item.commit();
                                          });
                                  forkedState.endPostProcess();
                                }
                                return results;
                              },
                              multiState.getExecutor());
                    }))
        .orTimeout(SELECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private List<SelectResponseRecords> collectSelectResults(
      MultiSelectState state, SelectState[] forkedStates) {
    state.addHistory(")"); // terminates the previous stage
    state.addHistory("(collect");
    final var results = new ArrayList<SelectResponseRecords>(forkedStates.length);
    for (var forkedState : forkedStates) {
      results.addAll(forkedState.getResponses());
    }
    state.addHistory(")");
    return results;
  }

  /**
   * Handles an UpsertContext request.
   *
   * @param sessionToken The session token, a DATA_WRITE role is required
   * @param state UpsertContext execution state
   * @param fanRouter The fan router for the operation
   * @return Future for the completion
   */
  public CompletableFuture<Void> upsertContextEntries(
      SessionToken sessionToken,
      AtomicOperationSpec atomicOperationSpec,
      ContextWriteOpState<PutContextEntriesRequest, List<Event>> state,
      FanRouter<PutContextEntriesRequest, Void> fanRouter) {

    state.startPreProcess();
    var future =
        validateTokenAllowDelegation(
                sessionToken, state.getTenantName(), AllowedRoles.DATA_WRITE, state)
            .thenApply((st) -> setUpAtomicOperation(atomicOperationSpec, st))
            .thenApply((st) -> prepareUpserting(st, fanRouter))
            .thenCompose(dataEngine::putContextEntriesAsync)
            .thenComposeAsync(
                (none) -> completeAtomicOperationIfEnabled(state), state.getExecutor())
            .exceptionally(
                (t) -> {
                  abortAtomicOperationIfEnabled(state);
                  final var cause = (t instanceof CompletionException) ? t.getCause() : t;
                  throw new CompletionException(cause);
                });

    final var secondPhase =
        makeFinalPhase(
            dataEngine::putContextEntriesAsync,
            state.getTimestamp(),
            fanRouter,
            state.getRequest(),
            state);
    if (secondPhase != null) {
      future = future.thenCompose(secondPhase);
    }

    return future
        .orTimeout(INSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  private ContextWriteOpState<PutContextEntriesRequest, List<Event>> prepareUpserting(
      ContextWriteOpState<PutContextEntriesRequest, List<Event>> state,
      FanRouter<PutContextEntriesRequest, Void> fanRouter) {
    return ExecutionHelper.supply(
        () -> {
          state.addHistory("(prepare");
          state.setRequestType(ContextRequestType.UPSERT);
          // resolve the context config first
          final StreamDesc contextDesc = resolveContextDesc(state, fanRouter);
          validateStreamType(contextDesc, StreamType.CONTEXT);
          if (state.getPhase() == RequestPhase.INITIAL) {
            findStreamDescAndSetUpMetricsRecorder(state, true, StreamType.CONTEXT);
            state.addRecordsWritten(state.getRequest().getEntries().size());
          } else if (contextDesc == null) {
            // it's unlikely happen but still possible in a race condition
            throw new NoSuchStreamException(state.getTenantName(), state.getStreamName());
          }
          state.setInputData(
              DataServiceUtils.buildContextEntries(
                  contextDesc, state.getRequest(), state.getTimestamp()));
          return state;
        });
  }

  public CompletableFuture<GetContextEntriesResponse> getContextEntries(
      SessionToken sessionToken, MultiContextEntriesSpec request, ContextOpState state) {

    state.startPreProcess();
    state.addHistory("(prepare{");
    state.clearBytesWritten();

    final var future = new CompletableFuture<GetContextEntriesResponse>();
    validateToken(
        sessionToken,
        state.getTenantName(),
        AllowedRoles.CONTEXT_DATA_READ,
        state,
        (userContext) -> {
          try {
            state.addHistory("(findContextDesc");
            state.setRequestType(ContextRequestType.SELECT_CONTEXT);
            findStreamDescAndSetUpMetricsRecorder(state, false, StreamType.CONTEXT);
            final StreamDesc contextDesc = state.getContextDesc();
            state.addHistory(")})");

            callDataEngineForGetContextEntries(
                request,
                contextDesc,
                state,
                (attrs, records) -> {
                  state.startPostProcess();
                  state.addHistory("})(postProcess");
                  final List<String> primaryKey = contextDesc.getPrimaryKey();
                  final var resp = buildGetContextEntriesResponse(primaryKey, attrs, records);
                  state.addHistory(")");
                  state.markDone();
                  future.complete(resp);
                },
                future::completeExceptionally);

          } catch (Throwable t) {
            future.completeExceptionally(t);
          }
        },
        future::completeExceptionally);

    return future
        .orTimeout(SELECT_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  /**
   * @return whether it's a count query
   */
  private boolean validateContextAttributes(
      StreamDesc contextConfig, List<String> attributes, List<List<Object>> primaryKeys)
      throws InvalidRequestException {
    // There are 3 possibilities:
    // 1. It is a count request, i.e. select("count()") without where clause.
    // 2. It is a list context keys request, i.e. select("primaryKey") without where clause.
    // 3. It is a get context entries request, i.e. select("attr1", "attr2", ...) where keys=...
    if (primaryKeys == null || primaryKeys.isEmpty()) {
      if ((attributes == null) || (attributes.size() == 0)) {
        throw new InvalidRequestException(
            String.format("Neither list of keys nor attribute provided in context select request"));
      }
      if (attributes.size() == 1) {
        final String attr = attributes.get(0);
        if (Constants.COUNT_METRIC.equalsIgnoreCase(attr)) {
          return true;
        }
      }
      final var allKeyElements =
          contextConfig.getPrimaryKey().stream()
              .map((element) -> element.toLowerCase())
              .collect(Collectors.toSet());
      if (attributes.size() == allKeyElements.size()
          && attributes.stream().allMatch((name) -> allKeyElements.contains(name.toLowerCase()))) {
        return false;
      }
      throw new InvalidRequestException(
          String.format(
              "Only primary key `%s` or count() allowed as select attribute",
              contextConfig.getPrimaryKey()));
    }
    // This is the 3rd case, i.e. get context entries request.
    // Verify that the attributes (if specified) are all valid.
    if ((attributes != null) && (attributes.size() > 0)) {
      final var validAttributes =
          contextConfig.getAllBiosAttributes().stream()
              .map(AttributeConfig::getName)
              .collect(Collectors.toSet());
      for (final var attr : attributes) {
        if (!validAttributes.contains(attr)) {
          throw new InvalidRequestException(
              String.format(
                  "Attribute `%s` is not defined in context `%s`", attr, contextConfig.getName()));
        }
      }
    }
    return false;
  }

  private void callDataEngineForGetContextEntries(
      MultiContextEntriesSpec request,
      StreamDesc contextDesc,
      ContextOpState state,
      BiConsumer<List<AttributeConfig>, List<ContextEntryRecord>> acceptor,
      Consumer<Throwable> errorHandler)
      throws TfosException {
    // Workaround a bug in SDK -- attributes may come as a single string delimited by comma, such
    // as "attrA,attrB,..."
    if (request.getAttributes() != null
        && request.getAttributes().size() == 1
        && request.getAttributes().get(0) != null
        && request.getAttributes().get(0).contains(",")) {
      final var elements = request.getAttributes().get(0).split(",");
      request.setAttributes(Arrays.asList(elements));
    }
    final List<AttributeConfig> attributes;
    final boolean countQuery =
        validateContextAttributes(contextDesc, request.getAttributes(), request.getPrimaryKeys());
    if (countQuery) {
      state.addHistory("(countContextPrimaryKeys{");
      attributes =
          List.of(
              new AttributeConfig(
                  Constants.COUNT_METRIC, io.isima.bios.models.AttributeType.INTEGER));
      dataEngine.countContextPrimaryKeysAsync(
          contextDesc,
          state,
          (count) -> acceptor.accept(attributes, DataServiceUtils.countToContextRecord(count)),
          errorHandler::accept);
    } else if ((request.getPrimaryKeys() == null) || (request.getPrimaryKeys().size() == 0)) {
      state.addHistory("(listContextPrimaryKeys{");
      attributes =
          contextDesc.getPrimaryKeyAttributes().stream()
              .map(
                  (attr) ->
                      new AttributeConfig(
                          attr.getName(), attr.getAttributeType().getBiosAttributeType()))
              .collect(Collectors.toList());
      dataEngine.listContextPrimaryKeysAsync(
          contextDesc,
          state,
          (keys) -> acceptor.accept(attributes, DataServiceUtils.keysToContextRecords(keys)),
          errorHandler::accept);
    } else {
      state.addHistory("(getContextEntries{");
      attributes = new ArrayList<>(contextDesc.getAllBiosAttributes());

      final List<List<Object>> convertedKeys =
          DataServiceUtils.buildContextPrimaryKeys(contextDesc, request);
      if (request.getOnTheFly() == Boolean.TRUE) {
        final var options = new HashSet<>(state.getOptions());
        options.add(ContextOpOption.ON_THE_FLY);
        state.setOptions(options);
      }
      dataEngine.getContextEntriesAsync(
          convertedKeys,
          state,
          (entries) ->
              acceptor.accept(
                  attributes, DataServiceUtils.entriesToContextRecords(contextDesc, entries)),
          errorHandler::accept);
    }
  }

  private GetContextEntriesResponse buildGetContextEntriesResponse(
      List<String> primaryKey,
      List<AttributeConfig> attributeConfigs,
      List<ContextEntryRecord> records) {
    final var response = new GetContextEntriesResponse();
    response.setContentRepresentation(ContentRepresentation.UNTYPED);
    response.setDefinitions(
        attributeConfigs.stream()
            .map((entry) -> new AttributeConfig(entry.getName(), entry.getType()))
            .collect(Collectors.toList()));
    response.setPrimaryKey(primaryKey);
    response.setEntries(records);
    return response;
  }

  public CompletableFuture<SelectContextEntriesResponse> selectContextEntries(
      SessionToken sessionToken, SelectContextRequest request, ContextOpState state) {
    return validateTokenAsync(
            sessionToken, state.getTenantName(), AllowedRoles.CONTEXT_DATA_READ, state)
        .thenCompose(
            (userContext) ->
                ExecutionHelper.supply(
                    () -> {
                      state.setRequestType(ContextRequestType.SELECT_CONTEXT);
                      state.addHistory("(findContextDesc");
                      findStreamDescAndSetUpMetricsRecorder(state, false, StreamType.CONTEXT);
                      state.addHistory(")})(selectContextEntries{");
                      return dataEngine
                          .selectContextEntriesAsync(request, state)
                          .thenApply(
                              (result) -> {
                                state.addHistory("})");
                                state.markDone();
                                return result;
                              })
                          .exceptionally((t) -> handleAsyncError(t, state));
                    }));
  }

  public CompletableFuture<MultiGetResponse> multiGetContextEntries(
      SessionToken sessionToken, MultiGetSpec multiGetSpec, ContextOpState state) {

    if ((multiGetSpec.getContextNames() == null)
        || (multiGetSpec.getQueries() == null)
        || (multiGetSpec.getQueries().size() == 0)) {
      return CompletableFuture.failedFuture(
          new InvalidRequestException(
              "multiGetSpec.contextNames and queries should not be null or empty. SDK error"));
    }

    int numQueries = multiGetSpec.getQueries().size();
    if (numQueries != multiGetSpec.getContextNames().size()) {
      return CompletableFuture.failedFuture(
          new InvalidRequestException(
              "Sizes of contextNames and queries are different: %d, %d. SDK error",
              multiGetSpec.getContextNames().size(), numQueries));
    }

    final CompletableFuture<String> initialFuture;

    if (TfosConfig.isTestMode()) {
      initialFuture =
          sharedConfig
              .getSharedProperties()
              .getPropertyCachedAsync("prop.test.analysis.MultiGetDelayContextName", null, state)
              .thenCompose(
                  (value) -> {
                    if (StringUtils.isBlank(value)) {
                      return CompletableFuture.completedFuture(null);
                    }
                    return sharedConfig
                        .nodeNameToEndpointAsync(Utils.getNodeName(), state)
                        .thenApply(
                            (endpoint) -> {
                              if (endpoint == null) {
                                return null;
                              }
                              return endpoint.getLeft() == NodeType.ANALYSIS ? value : null;
                            });
                  })
              .toCompletableFuture();
    } else {
      initialFuture = CompletableFuture.completedFuture(null);
    }

    return initialFuture.thenCompose(
        (contextToDelay) ->
            multiGetContextEntriesCore(sessionToken, multiGetSpec, contextToDelay, state));
  }

  @SuppressWarnings("unchecked")
  private CompletableFuture<MultiGetResponse> multiGetContextEntriesCore(
      SessionToken sessionToken,
      MultiGetSpec multiGetSpec,
      String contextToDelay,
      ContextOpState state) {
    final var tenantName = Objects.requireNonNull(state.getTenantName());
    final int numQueries = multiGetSpec.getQueries().size();

    CompletableFuture<GetContextEntriesResponse>[] futures = new CompletableFuture[numQueries];
    for (int i = 0; i < numQueries; i++) {
      var contextName = multiGetSpec.getContextNames().get(i);
      final var eachState = new ContextOpState(String.format("getContextEntries[%d]", i), state);
      eachState.setTenantName(tenantName);
      eachState.setStreamName(contextName);
      if (state.getMetricsTracer() != null) {
        eachState.setMetricsTracer(new OperationMetricsTracer(0));
        state.getMetricsTracer().addSubTracer(eachState.getMetricsTracer());
      }
      var querySpec = multiGetSpec.getQueries().get(i);
      futures[i] = getContextEntries(sessionToken, querySpec, eachState);
      if (contextName.equals(contextToDelay)) {
        futures[i] =
            futures[i].thenApplyAsync(
                (response) -> {
                  logger.info("Delaying multiGet request for context {} for 1s", contextName);
                  return response;
                },
                CompletableFuture.delayedExecutor(1000, TimeUnit.MILLISECONDS));
      }
    }

    return CompletableFuture.allOf(futures)
        .thenApply(
            (none) -> {
              final var multiGetResponse = new MultiGetResponse();
              final var entryLists =
                  Arrays.stream(futures)
                      .map(CompletableFuture::join)
                      .collect(Collectors.<GetContextEntriesResponse>toList());
              multiGetResponse.setEntryLists(entryLists);
              state.markDone();
              return multiGetResponse;
            })
        .exceptionally(
            (t) -> {
              // Find the number of the query that failed and add it to the exception message.
              if (t.getCause() instanceof TfosException) {
                final var sb = new StringBuilder();
                sb.append("Error in 0-indexed query number ");
                String delimiter = "";
                for (int i = 0; i < numQueries; i++) {
                  if (futures[i].isCompletedExceptionally()) {
                    sb.append(delimiter);
                    sb.append(i);
                    delimiter = ", ";
                  }
                }
                sb.append(": ");
                TfosException ex = (TfosException) (t.getCause());
                final var message = ex.getMessage();
                ex.replaceMessage(sb.append(message).toString());
                throw new CompletionException(ex);
              }
              throw new CompletionException(t.getCause());
            });
  }

  public CompletableFuture<Void> deleteContextEntries(
      SessionToken sessionToken,
      AtomicOperationSpec atomicOperationSpec,
      ContextWriteOpState<MultiContextEntriesSpec, List<List<Object>>> state,
      FanRouter<MultiContextEntriesSpec, Void> fanRouter) {
    final var tenantName = state.getTenantName();

    var future =
        validateTokenAsync(sessionToken, tenantName, AllowedRoles.DATA_WRITE, state)
            .thenApply((st) -> setUpAtomicOperation(atomicOperationSpec, st))
            .thenApply(this::prepareContextEntryDeletion)
            .thenCompose(dataEngine::deleteContextEntriesAsync)
            .thenComposeAsync(
                (none) -> completeAtomicOperationIfEnabled(state), state.getExecutor());

    final var secondPhase =
        makeFinalPhase(
            dataEngine::deleteContextEntriesAsync,
            state.getTimestamp(),
            fanRouter,
            state.getRequest(),
            state);
    if (secondPhase != null) {
      future = future.thenCompose(secondPhase);
    }

    return future
        .orTimeout(INSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  private ContextWriteOpState<MultiContextEntriesSpec, List<List<Object>>>
      prepareContextEntryDeletion(
          ContextWriteOpState<MultiContextEntriesSpec, List<List<Object>>> state) {
    return ExecutionHelper.supply(
        () -> {
          // resolve the context config first
          state.addHistory("(prepare");
          state.setRequestType(ContextRequestType.DELETE);
          if (state.getMetricsTracer() != null) {
            state.getMetricsTracer().clearBytesWritten();
          }

          if (state.getPhase() == RequestPhase.INITIAL) {
            findStreamDescAndSetUpMetricsRecorder(state, true, StreamType.CONTEXT);
          } else {
            final var contextDesc = admin.getStream(state.getTenantName(), state.getStreamName());
            validateStreamType(contextDesc, StreamType.CONTEXT);
            state.setStreamDesc(contextDesc);
          }
          state.setInputData(
              DataServiceUtils.buildContextPrimaryKeys(state.getContextDesc(), state.getRequest()));
          return state;
        });
  }

  public CompletableFuture<Void> updateContextEntry(
      SessionToken sessionToken,
      AtomicOperationSpec atomicOperationSpec,
      ContextWriteOpState<UpdateContextEntryRequest, UpdateContextEntrySpec> state,
      FanRouter<UpdateContextEntryRequest, Void> fanRouter) {
    var future =
        validateTokenAsync(sessionToken, state.getTenantName(), AllowedRoles.DATA_WRITE, state)
            .thenApply((st) -> setUpAtomicOperation(atomicOperationSpec, st))
            .thenApply((st) -> prepareContextEntryUpdate(st, fanRouter))
            .thenCompose(dataEngine::updateContextEntryAsync)
            .thenComposeAsync(
                (none) -> completeAtomicOperationIfEnabled(state), state.getExecutor());

    final var secondPhase =
        makeFinalPhase(
            dataEngine::updateContextEntryAsync,
            state.getTimestamp(),
            fanRouter,
            state.getRequest(),
            state);
    if (secondPhase != null) {
      future = future.thenCompose(secondPhase);
    }

    return future
        .orTimeout(INSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  private ContextWriteOpState<UpdateContextEntryRequest, UpdateContextEntrySpec>
      prepareContextEntryUpdate(
          ContextWriteOpState<UpdateContextEntryRequest, UpdateContextEntrySpec> state,
          FanRouter<UpdateContextEntryRequest, Void> fanRouter) {
    return ExecutionHelper.supply(
        () -> {
          // resolve the context config first
          state.addHistory("(prepare");
          state.setRequestType(ContextRequestType.UPDATE);
          final StreamDesc contextDesc = resolveContextDesc(state, fanRouter);
          if (state.getPhase() == RequestPhase.INITIAL) {
            findStreamDescAndSetUpMetricsRecorder(state, true, StreamType.CONTEXT);
          } else if (contextDesc == null) {
            // it's unlikely happen but still possible in a race condition
            throw new NoSuchStreamException(state.getTenantName(), state.getStreamName());
          } else {
            state.setMetricsRecorder(null);
          }
          state.setInputData(
              DataServiceUtils.buildContextEntryUpdateSpec(contextDesc, state.getRequest()));
          return state;
        });
  }

  public CompletableFuture<Void> replaceContextAttributes(
      SessionToken sessionToken,
      ContextWriteOpState<ReplaceContextAttributesRequest, ReplaceContextAttributesSpec> state,
      FanRouter<ReplaceContextAttributesRequest, Void> fanRouter) {
    var future =
        validateTokenAsync(sessionToken, state.getTenantName(), AllowedRoles.DATA_WRITE, state)
            .thenApply((st) -> prepareContextAttributesReplacement(st, fanRouter))
            .thenCompose(dataEngine::replaceContextAttributesAsync);

    final var secondPhase =
        makeFinalPhase(
            dataEngine::replaceContextAttributesAsync,
            state.getTimestamp(),
            fanRouter,
            state.getRequest(),
            state);
    if (secondPhase != null) {
      future = future.thenCompose(secondPhase);
    }

    return future
        .orTimeout(INSERTION_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  private ContextWriteOpState<ReplaceContextAttributesRequest, ReplaceContextAttributesSpec>
      prepareContextAttributesReplacement(
          ContextWriteOpState<ReplaceContextAttributesRequest, ReplaceContextAttributesSpec> state,
          FanRouter<ReplaceContextAttributesRequest, Void> fanRouter) {
    return ExecutionHelper.supply(
        () -> {
          state.addHistory("(prepare");
          state.setRequestType(ContextRequestType.REPLACE_CONTEXT_ATTRIBUTES);
          final StreamDesc contextDesc = resolveContextDesc(state, fanRouter);
          if (state.getPhase() == RequestPhase.INITIAL) {
            findStreamDescAndSetUpMetricsRecorder(state, true, StreamType.CONTEXT);
          } else if (contextDesc == null) {
            // it's unlikely happen but still possible in a race condition
            throw new NoSuchStreamException(state.getTenantName(), state.getStreamName());
          } else {
            state.setMetricsRecorder(null);
          }
          state.setInputData(
              DataServiceUtils.buildContextAttributeReplacementSpec(
                  contextDesc, state.getRequest()));
          return state;
        });
  }

  public CompletableFuture<FeatureStatusResponse> featureStatus(
      SessionToken sessionToken,
      FeatureStatusRequest featureStatusRequest,
      GenericExecutionState state) {
    return validateTokenAsync(sessionToken, state.getTenantName(), AllowedRoles.SIGNAL_READ, state)
        .thenComposeAsync(
            (s) -> dataEngine.featureStatus(s, featureStatusRequest), state.getExecutor())
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  public CompletableFuture<Void> featureRefresh(
      SessionToken sessionToken,
      FeatureRefreshRequest featureRefreshRequest,
      GenericExecutionState state) {
    return validateTokenAsync(sessionToken, state.getTenantName(), AllowedRoles.DATA_WRITE, state)
        .thenComposeAsync(
            (s) -> dataEngine.featureRefresh(state, featureRefreshRequest), state.getExecutor())
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  public CompletableFuture<AllContextSynopses> getAllContextSynopses(
      SessionToken sessionToken, SynopsisRequest request, GenericExecutionState state) {
    return validateTokenAsync(sessionToken, state.getTenantName(), AllowedRoles.SIGNAL_READ, state)
        .thenComposeAsync(
            (st) -> dataEngine.getAllContextSynopses(request, st), state.getExecutor())
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  public CompletableFuture<ContextSynopsis> getContextSynopsis(
      SessionToken sessionToken, SynopsisRequest request, GenericExecutionState state) {
    return validateTokenAsync(sessionToken, state.getTenantName(), AllowedRoles.SIGNAL_READ, state)
        .thenComposeAsync((st) -> dataEngine.getContextSynopsis(request, st), state.getExecutor())
        .exceptionally((t) -> handleAsyncError(t, state));
  }

  // Utilities /////////////////////////////////////////////////////////////

  private void findStreamDescAndSetUpMetricsRecorder(
      ExecutionState state, boolean isWriteOperation, StreamType streamType)
      throws InvalidRequestException {
    StreamDesc streamDesc = state.getStreamDesc();
    TfosException ex = null;
    if (streamDesc == null) {
      try {
        streamDesc = admin.getStream(state.getTenantName(), state.getStreamName());
        state.setStreamDesc(streamDesc);
      } catch (TfosException e) {
        // we'll handle this error later
        ex = e;
      }
    }
    validateStreamType(streamDesc, streamType);

    final var metricsTracer = state.getMetricsTracer();
    if (metricsTracer != null) {
      var realTenantName = state.getTenantName();
      var realStreamName = state.getStreamName();
      if (streamDesc != null) {
        realTenantName = streamDesc.getParent().getName();
        realStreamName = streamDesc.getName();
      }

      metricsTracer.setWriteOperation(isWriteOperation);
      final var userContext = state.getUserContext();
      final var recorder =
          metrics.getRecorder(
              realTenantName,
              realStreamName,
              userContext.getAppName(),
              userContext.getAppType(),
              state.getRequestType());
      if (recorder != null) {
        metricsTracer.attachRecorder(recorder);
        state.startPreProcess();
      }
      final var operationRecorder = metricsTracer.getLocalRecorder(realStreamName);
      if (operationRecorder != null) {
        state.setMetricsRecorder(operationRecorder);
      }
    }

    if (ex != null) {
      throw new CompletionException(ex);
    }
  }

  private <ReqT, DataT, RetT> Function<RetT, CompletionStage<RetT>> makeFinalPhase(
      Function<ContextWriteOpState<ReqT, DataT>, CompletionStage<RetT>> method,
      Long timestamp,
      FanRouter<ReqT, RetT> fanRouter,
      ReqT request,
      ContextWriteOpState<ReqT, DataT> state) {
    // no-op if fan router is not present
    if (fanRouter == null) {
      return null;
    }

    return (res) -> {
      state.startPostProcess();
      return fanRouter
          .fanRouteAsync(request, timestamp, state)
          .thenCompose(
              (remoteResponses) -> {
                if (remoteResponses == null) {
                  state.endPostProcess();
                  throw new CompletionException(new ApplicationException("Fan routing failed"));
                }
                if (!remoteResponses.isEmpty()) {
                  // state.endPostProcess();
                  return CompletableFuture.completedStage(remoteResponses.get(0));
                }
                state.setPhase(RequestPhase.FINAL);
                state.setMetricsRecorder(null);
                return method
                    .apply(state)
                    .thenApply(
                        (result) -> {
                          state.endPostProcess();
                          return result;
                        });
              });
    };
  }

  /**
   * Resolves the context description to be used for the operation.
   *
   * <p>The method also sets the resolved context and context version to the state. The version of
   * the resolved context is set to fanRouter, too, if the version is not specified in the state,
   * assuming it is an initial phase operation
   *
   * <p>The method returns null if the target stream is not found.
   */
  private <T, U> StreamDesc resolveContextDesc(
      ContextWriteOpState<?, ?> state, FanRouter<T, U> fanRouter) {
    StreamDesc contextDesc = null;
    try {
      if (state.getContextVersion() == null) {
        contextDesc = admin.getStream(state.getTenantName(), state.getStreamName());
        final var contextVersion = contextDesc.getVersion();
        state.setContextDesc(contextDesc);
        state.setContextVersion(contextVersion);
        if (fanRouter != null) {
          fanRouter.addParam(X_BIOS_STREAM_VERSION, contextVersion.toString());
        }
      } else {
        contextDesc =
            admin.getStream(
                state.getTenantName(), state.getStreamName(), state.getContextVersion());
        state.setContextDesc(contextDesc);
      }
    } catch (TfosException e) {
      // we'll handle this error later
    }
    return contextDesc;
  }

  private void validateStreamType(StreamDesc streamDesc, StreamType streamType)
      throws InvalidRequestException {
    Objects.requireNonNull(streamType);
    if (streamDesc == null) {
      return;
    }
    if (streamDesc.getType() != streamType) {
      throw new InvalidRequestException(
          String.format(
              "%s data operation may not be done against a %s",
              StringUtils.capitalize(streamType.name().toLowerCase()),
              streamDesc.getType().name().toLowerCase()));
    }
  }
}
