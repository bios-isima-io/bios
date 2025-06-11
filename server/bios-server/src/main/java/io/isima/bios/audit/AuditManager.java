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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.AdminWriteRequest;
import io.isima.bios.models.Operation;
import io.isima.bios.utils.TfosObjectMapperProvider;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AuditManager implements IAuditManager {
  private static final Logger logger = LoggerFactory.getLogger(AuditManager.class);
  private static final ObjectMapper mapper = TfosObjectMapperProvider.get();

  /**
   * Instantiate a AuditInfo object with method parameter.
   *
   * @param stream on which operation is performed
   * @param operation what is done
   * @param request client request
   * @return AuditInfo
   */
  public final AuditInfo begin(
      final String stream, final AuditOperation operation, final String request) {
    return new AuditInfo(stream, operation.getName(), request, null);
  }

  /**
   * Instantiate a AuditInfo object with method parameter.
   *
   * @param tenant on which operation is performed
   * @param stream on which operation is performed
   * @param operation what is done
   * @param request client request
   * @return AuditInfo
   */
  public final AuditInfo begin(
      String tenant, String stream, AuditOperation operation, String request) {
    return new AuditInfo(stream, operation.getName(), request, tenant);
  }

  /**
   * Instantiate a AuditInfo object with method parameter.
   *
   * @param stream on which operation is performed
   * @param operation what is done
   * @param request client request
   * @param tenant tenant on which operation is performed
   * @return AuditInfo
   */
  public final AuditInfo begin(
      final String stream,
      final AuditOperation operation,
      final AdminWriteRequest<?> request,
      final String tenant) {
    String auditOperation = operation.getName();
    if (request.getOperation().equals(Operation.REMOVE)) {
      auditOperation = auditOperation.replace("Add", "Delete");
    }
    auditOperation = auditOperation + "(" + request.getPhase().name() + ")";

    final var auditInfo = new AuditInfo(stream, auditOperation, request.toString(), tenant);
    auditInfo.setPhase(request.getPhase().toValue());
    return auditInfo;
  }

  public final <RespT> void logSuccess(AuditInfo auditInfo, RespT response) {
    String respStr = AuditConstants.NA;
    if (response != null) {
      try {
        respStr = mapper.writeValueAsString(response);
      } catch (JsonProcessingException e) {
        logger.error(
            "Serializing an audit logging item failed; audit={}, response={}, error={}",
            auditInfo,
            response,
            e.getMessage());
        logFailure(auditInfo, e);
      }
    }
    commit(auditInfo, OperationStatus.SUCCESS.name(), respStr);
  }

  public final void logFailure(AuditInfo auditInfo, Throwable t) {
    if (t instanceof TfosException) {
      commit(auditInfo, (TfosException) t);
    } else {
      commit(auditInfo, Status.INTERNAL_SERVER_ERROR.toString(), t.getMessage());
    }
  }

  /**
   * Method to commit audit info to db for a failure.
   *
   * @param auditInfo audit info to commit
   * @param t exception occurred during operation
   */
  @Override
  public abstract void commit(final AuditInfo auditInfo, final Throwable t);

  /**
   * Method to commit audit info to db.
   *
   * @param auditInfo audit info to commit
   * @param status operation status
   * @param response operation response
   */
  @Override
  public abstract void commit(AuditInfo auditInfo, String status, String response);

  /**
   * Method to commit audit info to database.
   *
   * @param auditInfo audit info to commit
   * @param response operation response
   */
  @Override
  public abstract void commit(AuditInfo auditInfo, Response response);
}
