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
package io.isima.bios.service.handler;

import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.audit.AuditConstants;
import io.isima.bios.audit.AuditInfo;
import io.isima.bios.audit.AuditManager;
import io.isima.bios.audit.AuditOperation;
import io.isima.bios.auth.Auth;
import io.isima.bios.data.DataEngine;
import io.isima.bios.errors.BiosError;
import io.isima.bios.errors.EventIngestError;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.PermissionDeniedException;
import io.isima.bios.errors.exception.ServiceException;
import io.isima.bios.errors.exception.StreamVersionMismatchException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutionHelper;
import io.isima.bios.execution.ExecutionState;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.execution.GenericExecutionState;
import io.isima.bios.metrics.MetricsOperation;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.RequestPhase;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.recorder.RequestType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceHandler {
  private static final Logger logger = LoggerFactory.getLogger(ServiceHandler.class);

  protected final AuditManager auditManager;
  protected final Auth auth;
  protected final AdminInternal tfosAdmin;
  protected final DataEngine dataEngine;
  protected final OperationMetrics metrics;
  @Getter @Setter protected boolean skipOperationFailure;

  protected ServiceHandler(
      AuditManager auditManager,
      Auth auth,
      AdminInternal admin,
      DataEngine dataEngine,
      OperationMetrics metrics) {
    this.auditManager = auditManager;
    this.auth = auth;
    this.tfosAdmin = admin;
    this.dataEngine = dataEngine;
    this.metrics = metrics;
    skipOperationFailure = false;
  }

  protected void initExecutionState(
      AdminInternal admin,
      DataEngine dataEngine,
      String tenant,
      String stream,
      ExecutionState state,
      Long version)
      throws TfosException {

    // state.setDataEngine(dataEngine);

    TfosException ex = null;
    BiosError errorInfo = null;
    if (tenant == null || tenant.isEmpty()) {
      errorInfo = EventIngestError.NO_TENANT;
    } else if (stream == null | stream.isEmpty()) {
      errorInfo = EventIngestError.NO_STREAM;
    } else {
      // setup execution state
      try {
        StreamDesc streamConfig = admin.getStream(tenant, stream);
        // verify version
        if (version != null && !streamConfig.getVersion().equals(version)) {
          throw new StreamVersionMismatchException(
              tenant,
              stream,
              String.format("given=%d existing=%d", version, streamConfig.getVersion()));
        }
        if (streamConfig.getType() != StreamType.SIGNAL
            && streamConfig.getType() != StreamType.ROLLUP
            && streamConfig.getType() != StreamType.METRICS) {
          throw new TfosException(
              GenericError.INVALID_REQUEST, "invalid stream type: " + streamConfig.getType());
        }
        state.setStreamDesc(streamConfig);
      } catch (TfosException e) {
        errorInfo = e;
        ex = e;
      }
    }
    if (errorInfo != null) {
      if (!"_clientLog".equalsIgnoreCase(stream)) {
        logger.warn(
            "{} sanity check failed. error={} status={}\n{}",
            state.getExecutionName(),
            (ex != null ? ex : errorInfo).toString(),
            errorInfo.getStatus().getStatusCode(),
            state);
      }
      throw new TfosException(errorInfo);
    }
  }

  /**
   * Method to validate an session token.
   *
   * <p>The method checks whether the session token is valid. If the sessionToken is valid but is
   * close to expiry, a new session token is created. It is returned via UserContext by the property
   * newSessionToken.
   *
   * <p>This method is meant to be called within a CompletableFuture async execution.
   *
   * @param sessionToken Session token
   * @param tenantName If specified, the method checks whether the tenant name matches the user's
   *     tenant. If the parameter is omitted (i.e., null is specified), the method skips the tenant
   *     validation.
   * @param permittedRoles List of permitted roles for the operation.
   * @throws UserAccessControlException thrown to indicate that the token validation failed
   */
  protected UserContext validateTokenCore(
      SessionToken sessionToken,
      String tenantName,
      List<Permission> permittedRoles,
      ExecutionState state)
      throws UserAccessControlException {
    final var userContext = auth.validateSessionToken(sessionToken, tenantName, permittedRoles);
    if (userContext.getIsAppMaster() == Boolean.TRUE || (state != null && state.isDelegate())) {
      final var tenant = tfosAdmin.getTenantOrNull(tenantName);
      if (tenant == null || !userContext.getTenant().equalsIgnoreCase(tenant.getAppMaster())) {
        throw new PermissionDeniedException(
            userContext,
            String.format(
                "AppMaster received a delegation request for non-app tenant %s", tenantName));
      }
    }
    return userContext;
  }

  /**
   * Execute an async operation in a sideline (handler-provided) thread.
   *
   * <p>NOTE: Never use this method for a performance-critical operation.
   *
   * @param parentState Parent execution state, likely to be in i/o thread
   * @param operation Supplier that provides the future for the result of the async operation. The
   *     operation may throw {@link TfosException} or {@link ApplicationException}. In the case of
   *     exception, returned future would complete exceptionally.
   * @returns Completable future of the operation result.
   */
  public <T> CompletableFuture<T> executeInSideline(
      ExecutionState parentState, ExecutionHelper.BiosAsyncTask<T> operation) {
    return CompletableFuture.completedFuture(null)
        .thenComposeAsync(
            (none) -> ExecutionHelper.supply(() -> operation.executeAsync()), getExecutor());
  }

  /**
   * Helper class to handle service asynchronously.
   *
   * @param <RespT> Return type of the API method.
   * @param <HandlerT> Handler type.
   */
  protected abstract class HandlerBase<RespT, HandlerT extends HandlerBase<RespT, HandlerT>> {
    private final SessionToken sessionToken;
    protected final OperationMetricsTracer metricsTracer;
    protected int numRecords = 1;
    protected final String authRealmTenant; // name of the tenant where the request is invoked
    protected boolean suppressAuditLogging = false; // set true for final-phase operations
    @Getter protected final String streamName;
    protected final RequestType requestType;
    private final List<Permission> allowedRoles;

    // for audit
    private AuditInfo auditInfo;
    private boolean putToLogger = false;

    @Getter @Setter private ExecutionState state;

    @Getter
    @Setter
    @Accessors(chain = true)
    private ExecutionState v2State;

    public HandlerBase(
        SessionToken sessionToken,
        OperationMetricsTracer metricsTracer,
        String tenantName,
        String streamName,
        RequestType requestType,
        List<Permission> allowedRoles,
        ExecutionState state) {

      Objects.requireNonNull(allowedRoles);

      this.sessionToken = sessionToken;
      this.metricsTracer = metricsTracer;
      this.authRealmTenant = tenantName;
      this.streamName = streamName;
      this.requestType = requestType;
      this.allowedRoles = allowedRoles;
      this.v2State = state;
    }

    public String getRequestName() {
      if (requestType != null) {
        return requestType.getRequestName();
      }
      return null;
    }

    public HandlerT enableAuditLog(
        AuditOperation operation, String streamName, Object request, boolean putToLogger) {
      final var requestString = request != null ? request.toString() : AuditConstants.NA;
      auditInfo = auditManager.begin(authRealmTenant, streamName, operation, requestString);
      this.putToLogger = putToLogger;
      return (HandlerT) this;
    }

    public HandlerT setNumRecords(int numRecords) {
      this.numRecords = numRecords;
      return (HandlerT) this;
    }

    /**
     * The service handling implementation.
     *
     * <p>This method is called by {@link #executeAsync} after the session token is validated. The
     * implementation must return the value to serve. When a user-oriented error happens, such as
     * "signal not found", "invalid request", and so on, the implementation should throw a
     * TfosException. When the implementation detects any application error, throw an
     * ApplicationException.
     *
     * @param userContext Information of the validated user
     * @return The value to serve.
     * @throws TfosException Throw this exception when a user error happens. The exception is logged
     *     as a warning. The service layer translates the exception to a proper return code.
     * @throws ApplicationException Throw this exception when an application error happens. The
     *     exception is logged as an error. The service layer translates this error as an Internal
     *     Server Error.
     */
    protected abstract RespT handleService(UserContext userContext)
        throws TfosException, ApplicationException;

    public final CompletableFuture<RespT> executeAsync() {
      final UserContext userContext;
      try {
        userContext = validateTokenCore(sessionToken, authRealmTenant, allowedRoles, this.v2State);
      } catch (UserAccessControlException e) {
        return CompletableFuture.failedFuture(e);
      }
      if (v2State != null) {
        v2State.setUserContext(userContext);
      }
      if (auditInfo != null) {
        auditInfo.setUserContext(userContext);
      }
      var realStreamName = streamName;
      if (realStreamName != null && !realStreamName.isBlank()) {
        try {
          final var streamDesc = tfosAdmin.getStream(userContext.getTenant(), streamName);
          realStreamName = streamDesc.getName();
        } catch (NoSuchTenantException | NoSuchStreamException e) {
          // we'll handle the error later
        }
      }
      if (metricsTracer != null) {
        final var recorder =
            metrics.getRecorder(
                userContext.getTenant(),
                realStreamName,
                userContext.getAppName(),
                userContext.getAppType(),
                requestType);
        if (recorder != null) {
          metricsTracer.attachRecorder(recorder);
        }
      }
      String requestName;
      if (requestType != null) {
        requestName = requestType.getRequestName();
      } else {
        requestName = "";
      }

      String finalRealStreamName = realStreamName;
      final var fut =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return handleService(userContext);
                } catch (TfosException e) {
                  e.setUserContext(userContext);
                  var state = v2State;
                  if (state == null) {
                    state =
                        new GenericExecutionState(
                            "InsertFailureEvent", ExecutorManager.getSidelineExecutor());
                  }
                  insertOperationFailureEvent(
                      userContext.getTenant(),
                      finalRealStreamName,
                      requestName,
                      userContext.getAppType().name(),
                      userContext.getAppName(),
                      e,
                      state);
                  addErrorContext(state, e);
                  throw new CompletionException(e);
                } catch (ApplicationException e) {
                  insertOperationFailureEvent(
                      userContext.getTenant(),
                      finalRealStreamName,
                      requestName,
                      userContext.getAppType().name(),
                      userContext.getAppName(),
                      e,
                      state);
                  throw new CompletionException(e);
                } catch (Throwable t) {
                  insertOperationFailureEvent(
                      userContext.getTenant(),
                      finalRealStreamName,
                      requestName,
                      userContext.getAppType().name(),
                      userContext.getAppName(),
                      t,
                      state);
                  throw new CompletionException(t);
                }
              },
              getExecutor());

      if (auditInfo == null) {
        return fut;
      }

      return fut.thenApplyAsync(
              (response) -> {
                if (!suppressAuditLogging) {
                  auditManager.logSuccess(auditInfo, response);
                }
                if (putToLogger) {
                  logAudit(auditInfo);
                }
                return response;
              },
              getExecutor())
          .exceptionally(
              t -> {
                if (t instanceof CompletionException) {
                  if (!suppressAuditLogging) {
                    auditManager.logFailure(auditInfo, t.getCause());
                  }
                  if (putToLogger) {
                    logAudit(auditInfo);
                  }
                  throw (CompletionException) t;
                } else {
                  if (!suppressAuditLogging) {
                    auditManager.logFailure(auditInfo, t);
                  }
                  if (putToLogger) {
                    logAudit(auditInfo);
                  }
                  throw new CompletionException(t);
                }
              });
    }

    protected void updateRecordsRead(String recorderName, long recRead) {
      if (metricsTracer != null) {
        final var recorder = metricsTracer.getLocalRecorder(recorderName);
        if (recorder != null) {
          recorder.getNumReads().add(recRead);
        }
      }
    }

    protected void updateRecordsWritten(String recorderName, long recWritten) {
      if (metricsTracer != null) {
        final var recorder = metricsTracer.getLocalRecorder(recorderName);
        if (recorder != null) {
          recorder.getNumWrites().add(recWritten);
        }
      }
    }
  }

  private void logAudit(AuditInfo auditInfo) {
    logger.info("Audit: {}", auditInfo.toString().replaceAll("\\p{C}", "?"));
  }

  protected abstract class AsyncServiceHandler<RequestT>
      extends HandlerBase<RequestT, AsyncServiceHandler<RequestT>> {
    public AsyncServiceHandler(
        SessionToken sessionToken,
        OperationMetricsTracer metricsTracer,
        String tenantName,
        String streamName,
        RequestType requestType,
        List<Permission> allowedRoles,
        ExecutionState state) {
      super(sessionToken, metricsTracer, tenantName, streamName, requestType, allowedRoles, state);
    }
  }

  protected abstract class FanRoutingServiceHandler<RequestT, ResponseT>
      extends HandlerBase<ResponseT, FanRoutingServiceHandler<RequestT, ResponseT>> {

    private final RequestT request;
    private final RequestPhase requestedPhase;
    private final Long timestamp;
    private final Optional<FanRouter<RequestT, ResponseT>> fanRouter;

    // for metrics
    private MetricsOperation initialOperation;
    private MetricsOperation finalOperation;
    private String streamName;

    public FanRoutingServiceHandler(
        SessionToken sessionToken,
        OperationMetricsTracer metricsTracer,
        String tenantName,
        String streamName,
        RequestType requestType,
        List<Permission> allowedRoles,
        RequestT request,
        RequestPhase requestedPhase,
        Long requestedTimestamp,
        Optional<FanRouter<RequestT, ResponseT>> fanRouter,
        ExecutionState state) {
      super(sessionToken, metricsTracer, tenantName, streamName, requestType, allowedRoles, state);

      Objects.requireNonNull(requestedPhase, "FanRoutingServiceHandler.requestedPhase");
      Objects.requireNonNull(requestedTimestamp, "FanRoutingServiceHandler.requestedTimestamp");
      Objects.requireNonNull(fanRouter, "FanRoutingServiceHandler.fanRouter");

      this.request = request;
      this.requestedPhase = requestedPhase;
      this.timestamp = requestedTimestamp;
      this.fanRouter = fanRouter;
      this.suppressAuditLogging = requestedPhase == RequestPhase.FINAL;
    }

    protected abstract ResponseT handleService(
        UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
        throws TfosException, ApplicationException;

    @Override
    protected final ResponseT handleService(UserContext userContext)
        throws TfosException, ApplicationException {

      ResponseT resp = handleServiceOuter(userContext, requestedPhase, timestamp);
      if (fanRouter.isPresent()) {
        final List<ResponseT> remoteResponses = fanRouter.get().fanRoute(request, timestamp);
        // Replace the response by the one from the final phase
        if (!remoteResponses.isEmpty()) {
          resp = remoteResponses.get(0);
        } else {
          // No endpoints are registered. We assume that this is a single node server.
          resp = handleServiceOuter(userContext, RequestPhase.FINAL, timestamp);
        }
      }
      return resp;
    }

    private ResponseT handleServiceOuter(
        UserContext userContext, RequestPhase currentPhase, Long currentTimestamp)
        throws TfosException, ApplicationException {
      return handleService(userContext, currentPhase, currentTimestamp);
    }
  }

  protected static <T> Consumer<T> createAcceptor(
      ExecutionState state, CompletableFuture<T> future) {
    return output -> {
      state.addHistory("}");
      logger.trace("{} was done successfully.\n{}", state.getExecutionName(), state);
      state.markDone();
      future.complete(output);
    };
  }

  protected Consumer<Throwable> createErrorHandler(
      ExecutionState state,
      CompletableFuture<?> future,
      OperationMetrics metrics,
      UserContext userContext,
      String streamName,
      RequestType requestType) {
    return new Consumer<>() {
      @Override
      public void accept(Throwable t) {
        if ((t instanceof RuntimeException && t.getCause() != null)
            || t instanceof CompletionException) {
          accept(t.getCause());
          return;
        }

        if ((t != null) && (!skipOperationFailure)) {
          var realStreamName = streamName;
          if (realStreamName != null && !realStreamName.isBlank()) {
            try {
              final var streamDesc = tfosAdmin.getStream(userContext.getTenant(), streamName);
              realStreamName = streamDesc.getName();
            } catch (NoSuchTenantException | NoSuchStreamException e) {
              // we'll handle the error later
            }
          }
          String requestName = "";
          if (requestType != null) {
            requestName = requestType.getRequestName();
          }
          insertOperationFailureEvent(
              userContext.getTenant(),
              realStreamName,
              requestName,
              userContext.getAppType() != null ? userContext.getAppType().name() : "",
              userContext.getAppName() != null ? userContext.getAppName() : "",
              t,
              state);
        }

        if (t instanceof TimeoutException) {
          TfosException ex = new TfosException(GenericError.TIMEOUT);
          logger.error(
              "{} timed out. error={} status={}\n{}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state);
          future.completeExceptionally(ex);
        } else if (t instanceof TfosException) {
          TfosException ex = (TfosException) t;
          addErrorContext(state, ex);
          logger.warn(
              "{} failed. error={} status={}\n{}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state);
          future.completeExceptionally(ex);
        } else if (t instanceof ServiceException) {
          ServiceException ex = (ServiceException) t;
          logger.error(
              "{} failed. error={} status={}\n{}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getResponse().getStatus(),
              state);
          future.completeExceptionally(ex);
        } else if (t != null) {
          // This causes Internal Server Error
          logger.error(
              "{} error. error={} status=500\n{}",
              state.getExecutionName(),
              t.getMessage(),
              state,
              t);
          future.completeExceptionally(
              new TfosException(GenericError.APPLICATION_ERROR, t.getMessage()));
        }
      }
    };
  }

  private static void addErrorContext(ExecutionState state, TfosException ex) {
    final var streamDesc = state != null ? state.getStreamDesc() : null;
    if (streamDesc != null) {
      final var message = ex.getMessage();
      final var keyword = streamDesc.getType() == StreamType.CONTEXT ? "context=" : "signal=";
      if (!message.contains("stream=") && !message.contains(keyword)) {
        final var sb = new StringBuilder();
        if (!message.contains(";")) {
          sb.append("; ");
        } else {
          sb.append(", ");
        }
        final String parent =
            streamDesc.getParent() == null ? null : streamDesc.getParent().getName();
        sb.append("tenant=")
            .append(parent)
            .append(", ")
            .append(keyword)
            .append(streamDesc.getName());
        ex.replaceMessage(message + sb.toString());
      }
    }
  }

  private void insertOperationFailureEvent(
      String tenant,
      String stream,
      String request,
      String appType,
      String appName,
      Throwable t,
      ExecutionState state) {
    if (!skipOperationFailure) {
      DataServiceUtils.insertOperationFailureEvents(
          tenant, stream, request, appName, appType, t, dataEngine, tfosAdmin, state);
    }
  }

  /**
   * Wrapper for a service handler method call.
   *
   * <p>This method is used when a service handler calls another service handler synchronously. A
   * service handler method returns a completable future of the response type. This method blocks
   * waiting until the future completes.
   *
   * <p>The service handler may completes exceptionally. This method catches such a case and throw
   * the caught exception.
   *
   * @param future Future to wait on
   * @param <RetT> The response type of the service handler
   * @return The service handler response
   * @throws TfosException thrown when the handler fails with this error type
   * @throws ApplicationException thrown when the handler fails with the reason other than
   *     TfosException
   */
  protected <RetT> RetT callHandlerMethod(CompletableFuture<RetT> future)
      throws TfosException, ApplicationException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new TfosException(GenericError.OPERATION_CANCELED);
    } catch (ExecutionException e) {
      final var cause = e.getCause();
      if (cause instanceof TfosException) {
        throw (TfosException) cause;
      }
      if (cause instanceof ApplicationException) {
        throw (ApplicationException) cause;
      }
      throw new ApplicationException("Unexpected error", cause);
    }
  }

  // TODO(Naoki): Make this an instance method
  public static Executor getExecutor() {
    return ExecutorManager.getSidelineExecutor();
  }
}
