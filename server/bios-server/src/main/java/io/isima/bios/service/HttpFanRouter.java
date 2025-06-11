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
package io.isima.bios.service;

import com.datastax.driver.core.utils.UUIDs;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.SharedConfig;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.BuildVersion;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.OkHttp3ErrorHandler;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.Events;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.sdk.errors.BiosClientError;
import io.isima.bios.sdk.errors.BiosClientErrorHandler;
import io.isima.bios.sdk.exceptions.BiosClientException;
import io.isima.bios.service.handler.FanRouter;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.Utils;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FanRouter for HTTP implemented by using Java SDK.
 *
 * @param <RequestT> Request object type
 * @param <ResponseT> Response object type
 */
// TODO(Naoki): Implement this by using C-SDK which is significantly faster than OkHttp.
public class HttpFanRouter<RequestT, ResponseT> implements FanRouter<RequestT, ResponseT> {
  // static members
  private static final Logger logger = LoggerFactory.getLogger(HttpFanRouter.class);
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  // external modules
  private final SharedConfig sharedConfig = BiosModules.getSharedConfig();
  private final AdminInternal tfosAdmin = BiosModules.getAdminInternal();
  private final DataEngine dataEngine = BiosModules.getDataEngine();
  private final HttpClientManager httpClientManager;

  // Call parameters
  private final String method;
  private final String pathPrefix;
  private final Supplier<String> pathSupplier;
  private final Map<String, String> headers;
  private String sessionToken;
  private final Class<ResponseT> responseClass;

  @Getter
  @AllArgsConstructor
  private static class FailureInfo {
    private String endpoint;
    private String failureREason;
  }

  /**
   * The constructor.
   *
   * @param method HTTP method
   * @param pathPrefix HTTP path prefix for the operation
   * @param pathSupplier HTTP path supplier for the operation
   * @param sessionToken Session token
   * @param httpClientManager HTTP client manager
   */
  public HttpFanRouter(
      String method,
      String pathPrefix,
      Supplier<String> pathSupplier,
      String sessionToken,
      HttpClientManager httpClientManager,
      Class<ResponseT> responseClass) {
    this.httpClientManager = httpClientManager;

    this.method = method;
    this.pathPrefix = pathPrefix;
    this.pathSupplier = pathSupplier;
    this.sessionToken = sessionToken;
    this.headers = new HashMap<>();
    this.responseClass = responseClass;
  }

  /**
   * Method to add an operation parameter.
   *
   * @param name Parameter name
   * @param value Parameter value
   */
  @Override
  public void addParam(String name, String value) {
    headers.put(name, value);
  }

  /**
   * Sets the session token.
   *
   * <p>The method replaces the existing token if it is set already.
   *
   * @param sessionToken The new session token
   */
  @Override
  public void setSessionToken(String sessionToken) {
    this.sessionToken = sessionToken;
  }

  @Override
  public List<ResponseT> fanRoute(RequestT request, Long timestamp)
      throws TfosException, ApplicationException {
    Objects.requireNonNull(timestamp);

    addParam(Keywords.X_BIOS_REQUEST_PHASE, RequestPhase.FINAL.name());
    addParam(Keywords.X_BIOS_TIMESTAMP, timestamp.toString());

    final RequestBody requestBody = makeRequestBody(request);

    final List<String> endpoints = sharedConfig.getEndpoints();
    logger.trace("endpoints={}", endpoints);
    if (endpoints.isEmpty()) {
      // No endpoints are registered. Unable to continue.
      return List.of();
    }

    final Queue<HttpRemoteCallState<RequestT, ResponseT>> callStates = new LinkedList<>();
    for (String endpoint : endpoints) {
      if (endpoint.startsWith(BiosConstants.PREFIX_FAILED_NODE_NAME)) {
        continue;
      }
      callStates.add(executeAsync(endpoint, requestBody));
    }

    final List<ResponseT> successfulResponses = new ArrayList<>();
    final var failures = new HashSet<FailureInfo>();
    // BiosClientException error = null;
    final var error = new AtomicReference<BiosClientException>();
    while (!callStates.isEmpty()) {
      final var state = callStates.remove();
      try {
        successfulResponses.add(state.getFuture().get());
      } catch (Throwable t) {
        final var failureInfo = handleError(t, state, error);
        if (failureInfo != null) {
          failures.add(failureInfo);
        }
      }
    }

    reportFailures(timestamp, failures);

    final int quorum = endpoints.size() / 2 + 1;
    if (successfulResponses.size() < quorum) {
      final var sb = new StringBuilder();
      sb.append(
          String.format(
              "successfulResponses %d < quorum %d. ", successfulResponses.size(), quorum));
      if ((error.get() != null) && (error.get().getMessage() != null)) {
        sb.append(error.get().getMessage());
      }
      throw new TfosException(sb.toString());
    }
    return successfulResponses;
  }

  @Override
  public CompletionStage<List<ResponseT>> fanRouteAsync(
      RequestT request, Long timestamp, ExecutionState state) {
    Objects.requireNonNull(timestamp);

    state.addHistory("fanRouting{(prep");

    addParam(Keywords.X_BIOS_REQUEST_PHASE, RequestPhase.FINAL.name());
    addParam(Keywords.X_BIOS_TIMESTAMP, timestamp.toString());
    if (state.getExecutionId() != null) {
      addParam(Keywords.X_BIOS_EXECUTION_ID, state.getExecutionId().toString());
    }

    final var error = new AtomicReference<BiosClientException>();
    final Set<FailureInfo> failures = ConcurrentHashMap.newKeySet();
    final var results = new AtomicReference<List<Object>>();

    state.addHistory(")(getEndpoints");
    final var successCount = new AtomicInteger();
    final var getEndpointState = new GenericExecutionState("getEndpoint", state);
    return sharedConfig
        .getEndpointsAsync(getEndpointState)
        .thenCompose(
            (endpoints) -> {
              state.addHistory(")");
              logger.trace("endpoints={}", endpoints);
              if (endpoints.isEmpty()) {
                // No endpoints are registered. Unable to continue.
                return CompletableFuture.completedFuture(null);
              }

              state.addHistory("(makeRequestBody");
              final RequestBody requestBody;
              try {
                requestBody = makeRequestBody(request);
              } catch (ApplicationException e) {
                return CompletableFuture.failedFuture(e);
              }

              state.addHistory(")");
              final List<HttpRemoteCallState<RequestT, ResponseT>> callStates = new ArrayList<>();
              for (String endpoint : endpoints) {
                if (endpoint.startsWith(BiosConstants.PREFIX_FAILED_NODE_NAME)) {
                  continue;
                }
                callStates.add(
                    executeAsync(
                        endpoint, requestBody, new GenericExecutionState(endpoint, state)));
              }

              final var futures = new CompletableFuture[callStates.size()];
              final var responses = new Object[callStates.size()];
              for (int i = 0; i < futures.length; ++i) {
                final var index = i;
                final HttpRemoteCallState<RequestT, ResponseT> callState = callStates.get(i);
                futures[i] =
                    callState
                        .getFuture()
                        .thenAccept(
                            ((response) -> {
                              responses[index] = response;
                              successCount.incrementAndGet();
                              callState.getState().ifPresent((st) -> st.markDone());
                            }))
                        .exceptionally(
                            (t) -> {
                              final var failureInfo = handleError(t, callState, error);
                              if (failureInfo != null) {
                                failures.add(failureInfo);
                              }
                              return null;
                            });
              }
              results.set(Arrays.asList(responses));
              return CompletableFuture.allOf(futures);
            })
        .thenApplyAsync(
            (none) -> {
              if (error.get() == null) {
                if (results.get() == null) {
                  return List.of();
                }
                return results.get().stream()
                    .map((entry) -> (ResponseT) entry)
                    .collect(Collectors.toList());
              }

              dataEngine.getExecutor().execute(() -> reportFailures(timestamp, failures));

              if (results.get() == null) {
                return null;
              }

              final var successfulResponses = new ArrayList<ResponseT>();
              for (Object resp : results.get()) {
                if (resp != null) {
                  successfulResponses.add((ResponseT) resp);
                }
              }
              final int quorum = results.get().size() / 2 + 1;
              if (successCount.get() < quorum) {
                final var sb = new StringBuilder();
                sb.append(
                    String.format(
                        "successfulResponses %d < quorum %d. ",
                        successfulResponses.size(), quorum));
                if ((error != null) && (error.get().getMessage() != null)) {
                  sb.append(error.get().getMessage());
                }
                throw new CompletionException(new TfosException(sb.toString()));
              }
              state.addHistory("}");
              return successfulResponses;
            },
            state.getExecutor());
  }

  private RequestBody makeRequestBody(RequestT request) throws ApplicationException {
    if (request == null) {
      return null;
    }
    try {
      return RequestBody.create(
          MediaType.parse(BiosConstants.APPLICATION_JSON), mapper.writeValueAsBytes(request));
    } catch (JsonProcessingException e) {
      throw new ApplicationException("Failed to convert request to JSON; error=" + e.getMessage());
    }
  }

  private HttpRemoteCallState<RequestT, ResponseT> executeAsync(
      String endpointSrc, RequestBody requestBody) {
    return executeAsync(endpointSrc, requestBody, null);
  }

  private HttpRemoteCallState<RequestT, ResponseT> executeAsync(
      String endpointSrc, RequestBody requestBody, ExecutionState state) {

    Objects.requireNonNull(endpointSrc);
    // TODO(Naoki): Do better trimming (if necessary) at the end of the authority.
    var endpoint =
        endpointSrc.endsWith("/")
            ? endpointSrc.substring(0, endpointSrc.length() - 1)
            : endpointSrc;
    var cli = httpClientManager.getClient(endpoint);
    final var remoteCallState = new HttpRemoteCallState<>(this, endpoint, cli, state);
    final String url = endpoint + pathPrefix + pathSupplier.get();
    return sendHttpRequest(method, url, headers, sessionToken, requestBody, remoteCallState);
  }

  private HttpRemoteCallState<RequestT, ResponseT> sendHttpRequest(
      final String method,
      final String url,
      final Map<String, String> headers,
      final String authToken,
      final RequestBody body,
      final HttpRemoteCallState<RequestT, ResponseT> remoteCallState) {
    remoteCallState.getState().ifPresent((state) -> state.addHistory("(sendRequest"));
    if (method == null) {
      throw new IllegalArgumentException("parameter 'method' may not be null");
    }
    if (url == null) {
      throw new IllegalArgumentException("parameter 'url' may not be null");
    }

    final Request.Builder builder =
        new Request.Builder()
            .url(url)
            .method(method, body)
            .addHeader(BiosConstants.SCHEME, BiosConstants.HTTPS)
            .addHeader(BiosConstants.ACCEPT_ENCODING, BiosConstants.GZIP)
            .addHeader(BiosConstants.ACCEPT_ENCODING, BiosConstants.DEFLATE)
            .addHeader(BiosConstants.CONTENT_TYPE, BiosConstants.APPLICATION_JSON)
            .addHeader(BiosConstants.SCHEME, BiosConstants.HTTPS);

    if (authToken != null) {
      builder.addHeader(BiosConstants.AUTH_HEADER, BiosConstants.TOKEN_SEPARATOR + authToken);
    }
    if (headers != null) {
      for (var entry : headers.entrySet()) {
        builder.addHeader(entry.getKey(), entry.getValue());
      }
    }

    final Request request = builder.build();

    final Call call = remoteCallState.getClient().newCall(request);
    call.enqueue(remoteCallState);
    remoteCallState.getState().ifPresent((state) -> state.addHistory(")(wait"));

    return remoteCallState;
  }

  public ResponseT handleWebResponse(Response webResponse) throws BiosClientException, IOException {
    try {
      OkHttp3ErrorHandler.checkError(webResponse);
      if (responseClass != Void.class) {
        if (webResponse.body() != null) {
          final String body = webResponse.body().string();
          if (body != null && !body.isEmpty()) {
            return mapper.readValue(body, responseClass);
          }
        }
      }
      return null;
    } finally {
      if (webResponse != null && webResponse.body() != null) {
        webResponse.body().close();
      }
    }
  }

  private FailureInfo handleError(
      Throwable t,
      HttpRemoteCallState<?, ?> callState,
      AtomicReference<BiosClientException> error) {
    final var endpoint = callState.getEndpoint();
    if (t instanceof InterruptedException) {
      // TODO(Naoki): Elaborate more
      logger.error("Final-phase operation was interrupted unexpectedly; endpoint={}", endpoint);
      return null;
    }
    Throwable cause = t.getCause();
    if (cause instanceof ConnectException) {
      cause = new BiosClientException(BiosClientError.SERVER_CONNECTION_FAILURE, endpoint);
    } else if (cause instanceof IOException) {
      cause = new BiosClientException(BiosClientError.SERVER_CHANNEL_ERROR, cause.getMessage());
    }
    if (cause instanceof BiosClientException) {
      final var ex = (BiosClientException) cause;
      if (ex.getCode() == BiosClientError.SERVER_IN_MAINTENANCE) {
        return null;
      }
      if (BiosClientErrorHandler.isTemporaryServerFailure(ex.getCode())) {
        final String reason =
            String.format(
                "error_code=%s, message=%s", ex.getCode().getErrorCode(), ex.getMessage());
        error.set(ex);
        return new FailureInfo(endpoint, reason);
      }
    }
    logger.error("Failed in fan routing; state={}", callState, cause);
    return null;
  }

  private void reportFailures(Long timestamp, Collection<FailureInfo> failures) {

    Objects.requireNonNull(timestamp);
    Objects.requireNonNull(failures);

    if (failures.isEmpty()) {
      return;
    }

    final String operation = method + " " + pathPrefix + pathSupplier.get();
    String nodeName = Utils.getNodeName();
    final String reporter =
        String.format(
            "{\"type\":\"server\",\"version\":\"%s\",\"node\":\"%s\"",
            BuildVersion.VERSION, nodeName);

    // TODO(Naoki): Determine if recording the payload is useful.
    final String payload = "";

    final var failedEndpoints = sharedConfig.getFailedEndpoints();

    final var errors = new ArrayList<Event>(failures.size());
    for (final var failure : failures) {
      final String endpoint = failure.getEndpoint();
      if (failedEndpoints.contains(endpoint)) {
        // already reported
        continue;
      }

      final String failedNodeName = sharedConfig.getNodeName(endpoint);

      // Make a failure report event
      final Event error = Events.createEvent(UUIDs.timeBased());
      error.set(BiosConstants.ATTR_TIMESTAMP, timestamp);
      error.set(BiosConstants.ATTR_ENDPOINT, endpoint);
      error.set(BiosConstants.ATTR_REASON, failure.getFailureREason());
      error.set(BiosConstants.ATTR_NODE_ID, failedNodeName);
      error.set(BiosConstants.ATTR_OPERATION, operation);
      error.set(BiosConstants.ATTR_PAYLOAD, payload);
      error.set(BiosConstants.ATTR_REPORTER, reporter);
      logger.error("Failure report; {}", error);
      errors.add(error);

      // Mark the failed endpoint
      try {
        sharedConfig.addFailedEndpoint(endpoint);
      } catch (ApplicationException e) {
        logger.error("Setting failed endpoint {} failed: {}", endpoint, e.getMessage());
      }
    }

    // Ingest failure reports
    StreamDesc systemSignal;
    try {
      systemSignal =
          tfosAdmin.getStream(BiosConstants.TENANT_SYSTEM, BiosConstants.STREAM_FAILURE_REPORT);
      // Assuming there are only up to a few error events, we just throw the events into data
      // engine without throttling
      BiosModules.getDigestor()
          .requestOneTimeTask(
              "FailureReport",
              (state) -> {
                for (var errorEvent : errors) {
                  dataEngine.insertEventIgnoreErrors(systemSignal, errorEvent, state);
                }
                return CompletableFuture.completedStage(null);
              });
    } catch (TfosException e) {
      logger.error("Failed to report failure reports", e);
    }
  }

  @Override
  public String toString() {
    final var sb =
        new StringBuilder("{")
            .append("method=")
            .append(method)
            .append(", path=")
            .append(pathPrefix + pathSupplier.get());
    if (headers != null) {
      sb.append(", headers=").append(headers);
    }
    return sb.append("}").toString();
  }
}
