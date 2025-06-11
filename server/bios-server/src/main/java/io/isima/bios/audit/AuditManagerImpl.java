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
package io.isima.bios.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.uuid.Generators;
import io.isima.bios.admin.v1.AdminInternal;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.common.IngestState;
import io.isima.bios.data.DataEngine;
import io.isima.bios.dto.IngestResponse;
import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.ServiceException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.models.Event;
import io.isima.bios.models.Events;
import io.isima.bios.preprocess.Utils;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.lang.reflect.Constructor;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AuditManager to begin and commit audit logs.
 *
 * @author sourav
 */
public class AuditManagerImpl extends AuditManager {
  private static final Logger logger = LoggerFactory.getLogger(AuditManagerImpl.class);
  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();

  private DataEngine dataEngine;

  /** {@link Constructor} */
  public AuditManagerImpl(DataEngine dataEngine) {
    this.dataEngine = dataEngine;
  }

  @Override
  public void commit(final AuditInfo auditInfo, final Throwable t) {
    auditInfo.setOperationFinishedAt(System.currentTimeMillis());
    auditInfo.setResponse(t.getMessage());
    if (t instanceof TfosException) {
      final var e = (TfosException) t;
      auditInfo.setStatus(e.getStatus().name());
      auditInfo.setUserContext(e.getUserContext());
    } else {
      auditInfo.setStatus(Response.Status.INTERNAL_SERVER_ERROR.name());
    }
    ingestAuditInfo(auditInfo);
  }

  /**
   * Method to commit audit info to db.
   *
   * @param auditInfo audit info to commit
   * @param status operation status
   * @param response operation response
   */
  @Override
  public void commit(AuditInfo auditInfo, String status, String response) {
    auditInfo.setResponse(response);
    auditInfo.setOperationFinishedAt(System.currentTimeMillis());
    auditInfo.setStatus(status);
    ingestAuditInfo(auditInfo);
  }

  /**
   * Method to commit audit info to database.
   *
   * @param auditInfo audit info to commit
   * @param response operation response
   */
  @Override
  public void commit(AuditInfo auditInfo, Response response) {
    auditInfo.setResponse(response.toString());
    auditInfo.setOperationFinishedAt(System.currentTimeMillis());
    if (null != response.getStatusInfo()) {
      auditInfo.setStatus(response.getStatusInfo().toEnum().name());
    } else {
      auditInfo.setStatus(String.valueOf(response.getStatus()));
    }
    ingestAuditInfo(auditInfo);
  }

  /**
   * Method to ingest audit info to database.
   *
   * @param auditInfo to commit
   */
  private void ingestAuditInfo(final AuditInfo auditInfo) {
    Event auditEvent = Events.createEvent(Generators.timeBasedGenerator().generate());
    auditEvent.setAttributes(auditInfo.getAuditAtrributes());
    try {
      IngestState ingestState =
          new IngestState(
              auditEvent,
              null,
              BiosConstants.TENANT_SYSTEM,
              BiosConstants.STREAM_AUDIT_LOG,
              dataEngine.getExecutor(),
              dataEngine);
      final StreamDesc streamDesc =
          getAdmin().getStream(BiosConstants.TENANT_SYSTEM, BiosConstants.STREAM_AUDIT_LOG);
      ingestState.setStreamDesc(streamDesc);

      // defence against harmful data
      Utils.validateEvent(streamDesc, auditEvent);

      final var completion = new CompletableFuture<Void>();
      Consumer<IngestResponse> acceptor = createAcceptor(ingestState);
      Consumer<Throwable> errorHandler = createErrorHandler(ingestState);
      ingestState.addHistory("}");
      dataEngine.ingestEvent(ingestState, acceptor, errorHandler);
    } catch (Throwable th) {
      logger.warn("Error while ingesting audit info", th);
    }
  }

  private AdminInternal getAdmin() {
    return BiosModules.getAdminInternal();
  }

  private <T> Consumer<T> createAcceptor(IngestState state) {
    return new Consumer<>() {
      @Override
      public void accept(T output) {
        state.addHistory("}");
        logger.trace(
            "Audit log ingestion was done successfully; tenant={}, stream={}, event={}",
            state.getTenantName(),
            state.getStreamName(),
            state.getEvent());
        state.markDone();
      }
    };
  }

  private Consumer<Throwable> createErrorHandler(IngestState state) {
    return new Consumer<>() {
      @Override
      public void accept(Throwable t) {
        if (t instanceof TimeoutException) {
          TfosException ex = new TfosException(GenericError.TIMEOUT);
          logger.warn(
              "{} failed. error={} status={}\n{}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state);
        } else if (t instanceof TfosException) {
          TfosException ex = (TfosException) t;
          logger.warn(
              "{} failed. error={} status={}\n{}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state);
        } else if (t.getCause() instanceof TfosException) {
          TfosException ex = (TfosException) t.getCause();
          logger.error(
              "{} failed. error={} status={}\n{}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getStatus(),
              state);
        } else if (t.getCause() instanceof ServiceException) {
          ServiceException ex = (ServiceException) t.getCause();
          logger.warn(
              "{} failed. error={} status={}\n{}",
              state.getExecutionName(),
              ex.getMessage(),
              ex.getResponse().getStatus(),
              state);
        } else {
          // This causes Internal Server Error
          logger.error(
              "{} error. error={} status=500\n{}",
              state.getExecutionName(),
              t.getMessage(),
              state,
              t);
        }
      }
    };
  }
}
