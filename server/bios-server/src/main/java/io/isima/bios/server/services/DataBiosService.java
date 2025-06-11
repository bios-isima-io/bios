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
package io.isima.bios.server.services;

import static io.netty.handler.codec.http.HttpMethod.PATCH;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import io.isima.bios.data.DataEngine;
import io.isima.bios.data.ReplaceContextAttributesSpec;
import io.isima.bios.data.UpdateContextEntrySpec;
import io.isima.bios.data.payload.PayloadUtils;
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
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.exceptions.InvalidRequestSyntaxException;
import io.isima.bios.execution.ContextOpState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.AtomicOperationSpec;
import io.isima.bios.models.BiosVersion;
import io.isima.bios.models.Event;
import io.isima.bios.models.FeatureRefreshRequest;
import io.isima.bios.models.FeatureStatusRequest;
import io.isima.bios.models.FeatureStatusResponse;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.proto.DataProto;
import io.isima.bios.models.proto.DataProto.InsertBulkRequest;
import io.isima.bios.models.proto.DataProto.InsertBulkSuccessResponse;
import io.isima.bios.server.MediaType;
import io.isima.bios.server.ServiceRouter;
import io.isima.bios.server.handlers.ContextWriteOpState;
import io.isima.bios.server.handlers.DataServiceHandler;
import io.isima.bios.server.handlers.InsertBulkState;
import io.isima.bios.server.handlers.InsertState;
import io.isima.bios.server.handlers.MultiSelectState;
import io.isima.bios.service.Keywords;
import io.isima.bios.service.handler.FanRouter;
import io.netty.handler.codec.Headers;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataBiosService extends BiosService {
  private static final Logger logger = LoggerFactory.getLogger(DataBiosService.class);
  private final DataEngine dataEngine;
  private final DataServiceHandler handler;

  public DataBiosService(DataServiceHandler handler) {
    this.dataEngine = BiosModules.getDataEngine();
    this.handler = handler;
  }

  @Override
  public void register(ServiceRouter serviceRouter) {
    serviceRouter.addHandler(
        new RestHandler<>(
            "Insert",
            PUT,
            BiosServicePath.PATH_EVENT,
            OK,
            MediaType.X_PROTOBUF,
            MediaType.X_PROTOBUF,
            DataProto.InsertRequest.class,
            DataProto.InsertSuccessResponse.class,
            InsertState.class) {

          @Override
          public InsertState createState(Executor executor) {
            return new InsertState(executor, dataEngine);
          }

          @Override
          protected CompletableFuture<DataProto.InsertSuccessResponse> handle(
              DataProto.InsertRequest insertRequest,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              InsertState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var signalName = pathParams.get(Keywords.SIGNAL_NAME);
            final UUID eventId;
            try {
              eventId = UUID.fromString(pathParams.get(Keywords.EVENT_ID));
            } catch (IllegalArgumentException e) {
              return CompletableFuture.failedFuture(
                  new InvalidRequestSyntaxException("Malformed eventId format"));
            }
            state.setInitialParams(tenantName, signalName, eventId, insertRequest);
            state.setDelegate(getBoolean(queryParams, "delegate"));

            var atomicOperationSpec = getAtomicOperationOption(queryParams);
            // Disable the atomic op spec if number of bundled requests is 1, meaning it's only
            // this op; a single insertion is always atomic.
            if (atomicOperationSpec != null && atomicOperationSpec.getNumBundledRequests() == 1) {
              atomicOperationSpec = null;
            }

            return handler
                .insertSignal(sessionToken, atomicOperationSpec, state)
                .thenApplyAsync(PayloadUtils::toProtoInsertResponse, state.getExecutor());
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "InsertBulk",
            POST,
            BiosServicePath.PATH_INSERT_BULK,
            OK,
            MediaType.X_PROTOBUF,
            MediaType.X_PROTOBUF,
            InsertBulkRequest.class,
            InsertBulkSuccessResponse.class,
            InsertBulkState.class) {

          @Override
          public InsertBulkState createState(Executor executor) {
            return new InsertBulkState(executor);
          }

          @Override
          protected CompletableFuture<InsertBulkSuccessResponse> handle(
              InsertBulkRequest insertRequest,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              InsertBulkState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var internalRequest = headers.get(Keywords.X_BIOS_INTERNAL_REQUEST);

            // Any user would send client metrics information with special header
            // x-bios-internal-request. For those requests, the request handler gives permission
            // to run the operation regardless the user's role.
            final boolean isMetricsLogging =
                internalRequest != null && Keywords.METRICS.contentEquals(internalRequest);

            state.setInitialParams(tenantName, insertRequest);
            // TODO(Naoki): Define "delegate" in Keywords
            state.setDelegate(getBoolean(queryParams, "delegate"));
            if (isMetricsLogging) {
              return handler
                  .insertClientMetrics(sessionToken, state)
                  .thenApplyAsync(PayloadUtils::toProtoInsertBulkResponse, state.getExecutor());
            }
            final var atomicOperationSpec = getAtomicOperationOption(queryParams);
            return handler
                .insertBulk(sessionToken, atomicOperationSpec, state)
                .thenApplyAsync(PayloadUtils::toProtoInsertBulkResponse, state.getExecutor());
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "MultiSelect",
            POST,
            BiosServicePath.PATH_SELECT,
            OK,
            MediaType.X_PROTOBUF,
            MediaType.X_PROTOBUF,
            DataProto.SelectRequest.class,
            DataProto.SelectResponse.class,
            MultiSelectState.class) {

          @Override
          public MultiSelectState createState(Executor executor) {
            return new MultiSelectState(executor);
          }

          @Override
          protected CompletableFuture<DataProto.SelectResponse> handle(
              DataProto.SelectRequest selectRequest,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              MultiSelectState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var clientVersion =
                new BiosVersion(getString(headers, Keywords.X_BIOS_CLIENT_VERSION, null));
            state.setInitialParams(tenantName, selectRequest, clientVersion);
            return handler
                .multiSelect(sessionToken, state)
                .thenApplyAsync(PayloadUtils::toProtoMultiSelectResponse, state.getExecutor());
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "PutContextEntries",
            POST,
            BiosServicePath.PATH_CONTEXT_ENTRIES,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            PutContextEntriesRequest.class,
            Void.class,
            ContextWriteOpState.class) {

          @Override
          public ContextWriteOpState<PutContextEntriesRequest, List<Event>> createState(
              Executor executor) {
            final var state =
                new ContextWriteOpState<PutContextEntriesRequest, List<Event>>(
                    operationName, executor);
            state.setExecutionId(UUID.randomUUID());
            return state;
          }

          @Override
          @SuppressWarnings("unchecked") // Due to generic state type
          protected CompletableFuture<Void> handle(
              PutContextEntriesRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              ContextWriteOpState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);
            final Long contextVersion;
            try {
              contextVersion = getLong(headers, Keywords.X_BIOS_STREAM_VERSION, null);
            } catch (InvalidRequestException e) {
              return CompletableFuture.failedFuture(e);
            }
            if (headers.contains(Keywords.X_BIOS_EXECUTION_ID)) {
              logger.debug("{}: {}", this.operationName, headers.get(Keywords.X_BIOS_EXECUTION_ID));
            }

            final Long timestamp = determineTimestamp(headers);
            final var phase = getRequestPhase(headers);
            final Optional<FanRouter<PutContextEntriesRequest, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);

            state.setInitialParams(
                tenantName, contextName, contextVersion, request, phase, timestamp);

            final var atomicOperationSpec =
                phase == RequestPhase.INITIAL ? makeAtomicOperationOption() : null;
            state.setDelegate(getBoolean(queryParams, "delegate"));

            return handler.upsertContextEntries(
                sessionToken, atomicOperationSpec, state, fanRouter.orElse(null));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetContextEntries",
            POST,
            BiosServicePath.PATH_CONTEXT_ENTRIES_FETCH,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            MultiContextEntriesSpec.class,
            GetContextEntriesResponse.class,
            ContextOpState.class) {

          @Override
          public ContextOpState createState(Executor executor) {
            return new ContextOpState(operationName, executor);
          }

          @Override
          protected CompletableFuture<GetContextEntriesResponse> handle(
              MultiContextEntriesSpec request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              ContextOpState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);

            state.setTenantName(tenantName);
            state.setStreamName(contextName);

            return handler.getContextEntries(sessionToken, request, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "SelectContextEntries",
            POST,
            BiosServicePath.PATH_CONTEXT_ENTRIES_SELECT,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            SelectContextRequest.class,
            SelectContextEntriesResponse.class,
            ContextOpState.class) {

          @Override
          public ContextOpState createState(Executor executor) {
            return new ContextOpState(operationName, executor);
          }

          @Override
          protected CompletableFuture<SelectContextEntriesResponse> handle(
              SelectContextRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              ContextOpState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);

            state.setTenantName(tenantName);
            state.setStreamName(contextName);

            return handler.selectContextEntries(sessionToken, request, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "MultiGetContextEntries",
            POST,
            BiosServicePath.PATH_MULTI_GET,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            MultiGetSpec.class,
            MultiGetResponse.class,
            ContextOpState.class) {

          @Override
          public ContextOpState createState(Executor executor) {
            return new ContextOpState(operationName, executor);
          }

          @Override
          protected CompletableFuture<MultiGetResponse> handle(
              MultiGetSpec request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              ContextOpState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);

            state.setTenantName(tenantName);
            state.setStreamName(contextName);

            return handler.multiGetContextEntries(sessionToken, request, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "DeleteContextEntries",
            POST,
            BiosServicePath.PATH_CONTEXT_ENTRIES_DELETE,
            NO_CONTENT,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            MultiContextEntriesSpec.class,
            Void.class,
            ContextWriteOpState.class) {

          @Override
          public ContextWriteOpState<MultiContextEntriesSpec, List<Object>> createState(
              Executor executor) {
            return new ContextWriteOpState<>(operationName, executor);
          }

          @Override
          @SuppressWarnings("unchecked") // due to the generic state type
          protected CompletableFuture<Void> handle(
              MultiContextEntriesSpec request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              ContextWriteOpState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);
            final Long contextVersion;
            try {
              contextVersion = getLong(headers, Keywords.X_BIOS_STREAM_VERSION, null);
            } catch (InvalidRequestException e) {
              return CompletableFuture.failedFuture(e);
            }

            final Long timestamp = determineTimestamp(headers);
            final var phase = getRequestPhase(headers);
            final Optional<FanRouter<MultiContextEntriesSpec, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);

            state.setInitialParams(
                tenantName, contextName, contextVersion, request, phase, timestamp);

            final var atomicOperationSpec =
                phase == RequestPhase.INITIAL ? makeAtomicOperationOption() : null;

            return handler.deleteContextEntries(
                sessionToken, atomicOperationSpec, state, fanRouter.orElse(null));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "UpdateContextEntry",
            PATCH,
            BiosServicePath.PATH_CONTEXT_ENTRIES,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            UpdateContextEntryRequest.class,
            Void.class,
            ContextWriteOpState.class) {

          @Override
          public ContextWriteOpState<UpdateContextEntryRequest, UpdateContextEntrySpec> createState(
              Executor executor) {
            return new ContextWriteOpState<>(operationName, executor);
          }

          @Override
          @SuppressWarnings("unchecked") // due to the generic state type
          protected CompletableFuture<Void> handle(
              UpdateContextEntryRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              ContextWriteOpState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);
            final Long contextVersion;
            try {
              contextVersion = getLong(headers, Keywords.X_BIOS_STREAM_VERSION, null);
            } catch (InvalidRequestException e) {
              return CompletableFuture.failedFuture(e);
            }

            final Long timestamp = determineTimestamp(headers);
            final var phase = getRequestPhase(headers);
            final Optional<FanRouter<UpdateContextEntryRequest, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);

            state.setInitialParams(
                tenantName, contextName, contextVersion, request, phase, timestamp);

            final var atomicOperationSpec =
                phase == RequestPhase.INITIAL ? makeAtomicOperationOption() : null;

            return handler.updateContextEntry(
                sessionToken, atomicOperationSpec, state, fanRouter.orElse(null));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "ReplaceContextAttributes",
            POST,
            BiosServicePath.PATH_CONTEXT_ENTRIES_REPLACE,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            ReplaceContextAttributesRequest.class,
            Void.class,
            ContextWriteOpState.class) {

          @Override
          public ContextWriteOpState<ReplaceContextAttributesRequest, ReplaceContextAttributesSpec>
              createState(Executor executor) {
            return new ContextWriteOpState<>(operationName, executor);
          }

          @Override
          @SuppressWarnings("unchecked") // due to the generic state type
          protected CompletableFuture<Void> handle(
              ReplaceContextAttributesRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              ContextWriteOpState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);
            final Long contextVersion;
            try {
              contextVersion = getLong(headers, Keywords.X_BIOS_STREAM_VERSION, null);
            } catch (InvalidRequestException e) {
              return CompletableFuture.failedFuture(e);
            }

            final Long timestamp = determineTimestamp(headers);
            final var phase = getRequestPhase(headers);
            final Optional<FanRouter<ReplaceContextAttributesRequest, Void>> fanRouter =
                createFanRouter(headers, phase, sessionToken, Void.class);

            state.setInitialParams(
                tenantName, contextName, contextVersion, request, phase, timestamp);

            return handler.replaceContextAttributes(sessionToken, state, fanRouter.orElse(null));
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "FeatureStatus",
            POST,
            BiosServicePath.PATH_FEATURE_STATUS,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.APPLICATION_JSON,
            FeatureStatusRequest.class,
            FeatureStatusResponse.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<FeatureStatusResponse> handle(
              FeatureStatusRequest featureStatusRequest,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            state.setTenantName(pathParams.get("tenantName"));

            return handler.featureStatus(sessionToken, featureStatusRequest, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "FeatureRefresh",
            POST,
            BiosServicePath.PATH_FEATURE_REFRESH,
            OK,
            MediaType.APPLICATION_JSON,
            MediaType.NONE,
            FeatureRefreshRequest.class,
            Void.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<Void> handle(
              FeatureRefreshRequest featureRefreshRequest,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            state.setTenantName(pathParams.get("tenantName"));

            return handler.featureRefresh(sessionToken, featureRefreshRequest, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetAllContextSynopses",
            POST,
            BiosServicePath.PATH_CONTEXTS_SYNOPSES,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            SynopsisRequest.class,
            AllContextSynopses.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<AllContextSynopses> handle(
              SynopsisRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);

            state.setTenantName(tenantName);

            return handler.getAllContextSynopses(sessionToken, request, state);
          }
        });

    serviceRouter.addHandler(
        new RestHandler<>(
            "GetContextSynopsis",
            POST,
            BiosServicePath.PATH_CONTEXT_SYNOPSIS,
            OK,
            MediaType.APPLICATION_JSON_NO_VALIDATION,
            MediaType.APPLICATION_JSON,
            SynopsisRequest.class,
            ContextSynopsis.class,
            GenericExecutionState.class) {

          @Override
          public GenericExecutionState createState(Executor executor) {
            return createDefaultState(executor);
          }

          @Override
          protected CompletableFuture<ContextSynopsis> handle(
              SynopsisRequest request,
              Headers<CharSequence, CharSequence, ?> headers,
              Map<String, String> pathParams,
              Map<String, String> queryParams,
              Headers<CharSequence, CharSequence, ?> responseHeaders,
              GenericExecutionState state) {
            final var sessionToken = getSessionToken(headers);
            final var tenantName = pathParams.get(Keywords.TENANT_NAME);
            final var contextName = pathParams.get(Keywords.CONTEXT_NAME);

            state.setTenantName(tenantName);
            state.setStreamName(contextName);

            return handler.getContextSynopsis(sessionToken, request, state);
          }
        });
  }

  /**
   * Parse request header to retrieve atomic operation option if set.
   *
   * @param queryParams Query parameters
   * @return Atomic operation spec if the option is set, null otherwise.
   * @throws InvalidRequestException wrapped by CompletionException to indicate that the atomic
   *     operation specification is invalid.
   */
  private AtomicOperationSpec getAtomicOperationOption(Map<String, String> queryParams) {
    final var atomicOperationIdSrc = getString(queryParams, Keywords.ATOMIC_OP_ID, null);
    if (atomicOperationIdSrc == null) {
      return null;
    }
    final UUID atomicOperationId;
    try {
      atomicOperationId = UUID.fromString(atomicOperationIdSrc.toString());
    } catch (IllegalArgumentException e) {
      throw new CompletionException(
          new InvalidRequestException(
              "Query parameter %s must be a UUID but value '%s' is given",
              Keywords.ATOMIC_OP_ID, atomicOperationIdSrc.toString()));
    }

    final var numBundledRequestsSrc = getString(queryParams, Keywords.ATOMIC_OP_COUNT, null);
    if (numBundledRequestsSrc == null) {
      throw new CompletionException(
          new InvalidRequestException(
              "Query parameter %s is required when %s is set",
              Keywords.ATOMIC_OP_COUNT, Keywords.ATOMIC_OP_ID));
    }
    final int numBundledRequests;
    try {
      numBundledRequests = Integer.parseInt(numBundledRequestsSrc.toString());
    } catch (NumberFormatException e) {
      throw new CompletionException(
          new InvalidRequestException(
              "Query parameter %s must be an integer but %s is given",
              Keywords.ATOMIC_OP_COUNT, numBundledRequestsSrc.toString()));
    }
    return new AtomicOperationSpec(atomicOperationId, numBundledRequests);
  }

  private AtomicOperationSpec makeAtomicOperationOption() {
    return new AtomicOperationSpec(UUID.randomUUID(), 1);
  }
}
