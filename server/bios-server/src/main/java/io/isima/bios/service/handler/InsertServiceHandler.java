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
import io.isima.bios.audit.AuditManager;
import io.isima.bios.auth.AllowedRoles;
import io.isima.bios.auth.Auth;
import io.isima.bios.common.IngestState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.NoSuchStreamException;
import io.isima.bios.errors.exception.NoSuchTenantException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.UserAccessControlException;
import io.isima.bios.metrics.OperationMetrics;
import io.isima.bios.metrics.OperationMetricsTracer;
import io.isima.bios.models.IngestRequest;
import io.isima.bios.models.ProcessStage;
import io.isima.bios.models.SessionToken;
import io.isima.bios.models.UserContext;
import io.isima.bios.preprocess.Utils;
import io.isima.bios.recorder.OperationMetricsRecorder;
import io.isima.bios.recorder.SignalRequestType;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InsertServiceHandler extends ServiceHandler {
  private static final Logger logger = LoggerFactory.getLogger(InsertServiceHandler.class);

  private static final int MAX_PARALLELISM = 64;

  /** Constructor. */
  public InsertServiceHandler(
      AuditManager auditManager,
      Auth auth,
      AdminInternal admin,
      DataEngine dataEngine,
      OperationMetrics metrics) {
    super(auditManager, auth, admin, dataEngine, metrics);
  }

  /**
   * Inserts a single record.
   *
   * @param sessionToken Session token
   * @param tenantName Tenant Name
   * @param signalName Signal Name
   * @param request Ingest Request (still old style as engine uses old style)
   * @return a completable future
   */
  public CompletableFuture<IngestResponse> insertEvent(
      SessionToken sessionToken,
      String tenantName,
      String signalName,
      IngestRequest request,
      OperationMetricsTracer metricsTracer) {

    final UserContext userContext;
    try {
      userContext = auth.validateSessionToken(sessionToken, tenantName, AllowedRoles.DATA_WRITE);
    } catch (UserAccessControlException e) {
      return CompletableFuture.failedFuture(e);
    }

    if (metricsTracer != null) {
      var realTenantName = tenantName;
      var realSignalName = signalName;
      if (signalName != null && !signalName.isBlank()) {
        try {
          final var signalDesc = tfosAdmin.getStream(tenantName, signalName);
          realTenantName = signalDesc.getParent().getName();
          realSignalName = signalDesc.getName();
        } catch (NoSuchTenantException | NoSuchStreamException e) {
          // we'll handle the error later
        }
      }
      metricsTracer.setWriteOperation(true);
      final var recorder =
          metrics.getRecorder(
              realTenantName,
              realSignalName,
              userContext.getAppName(),
              userContext.getAppType(),
              SignalRequestType.INSERT);
      if (recorder != null) {
        metricsTracer.attachRecorder(recorder);
      }
      final var localRecorder = metricsTracer.getLocalRecorder(realSignalName);
      if (localRecorder != null) {
        localRecorder.getNumWrites().increment();
      }
    }
    try {
      return insertEventInternal(
          userContext,
          tenantName,
          signalName,
          request,
          metricsTracer != null ? metricsTracer.getLocalRecorder(signalName) : null);
    } catch (TfosException e) {
      e.setUserContext(userContext);
      return CompletableFuture.failedFuture(e);
    }
  }

  private CompletableFuture<IngestResponse> insertEventInternal(
      UserContext userContext,
      String tenantName,
      String signalName,
      IngestRequest input,
      OperationMetricsRecorder localRecorder)
      throws TfosException {

    // create and initialize an ingest state
    IngestState state =
        new IngestState(
            localRecorder, tenantName, signalName, dataEngine.getExecutor(), dataEngine);
    state.addHistory("initIngest");
    state.setInput(input);
    state.startPreProcess();

    initExecutionState(
        tfosAdmin, dataEngine, tenantName, signalName, state, input.getStreamVersion());
    if (input.getEventId() == null) {
      throw new TfosException(GenericError.INVALID_REQUEST, "eventId may not be null");
    }
    if (input.getEventId().version() != 1) {
      throw new TfosException(GenericError.INVALID_REQUEST, "UUID version of eventId must be 1");
    }

    // get preprocessors
    List<ProcessStage> preProcesses;
    preProcesses = tfosAdmin.getPreProcesses(tenantName, signalName);

    CompletableFuture<IngestResponse> completableFuture = new CompletableFuture<>();
    final Consumer<IngestResponse> acceptor = createAcceptor(state, completableFuture);
    final Consumer<Throwable> errorHandler =
        createErrorHandler(
            state, completableFuture, metrics, userContext, signalName, SignalRequestType.INSERT);

    Utils.preprocessIngestion(state, preProcesses, false)
        .thenRun(
            () -> {
              // state.endPreProcess();
              // create acceptors
              state.addHistory("}");
              dataEngine.ingestEvent(state, acceptor, errorHandler);
            })
        .exceptionally(
            ex -> {
              errorHandler.accept(ex);
              return null;
            });
    return completableFuture;
  }
}
