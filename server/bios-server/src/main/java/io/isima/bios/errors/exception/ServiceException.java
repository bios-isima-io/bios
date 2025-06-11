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
package io.isima.bios.errors.exception;

import io.isima.bios.errors.GenericError;
import io.isima.bios.integrations.validator.IntegrationError;
import io.isima.bios.models.ErrorResponsePayload;
import java.util.HashSet;
import java.util.Set;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

/** Exception to report TFOS service error. */
public class ServiceException extends WebApplicationException {
  private static final long serialVersionUID = 7050482577880026092L;

  private static final Set<String> nonBugInternalServerErrors;

  static {
    nonBugInternalServerErrors = new HashSet<>();
    for (var entry : IntegrationError.values()) {
      nonBugInternalServerErrors.add(entry.getErrorCode());
    }
    nonBugInternalServerErrors.add(GenericError.INVALID_CONFIGURATION.getErrorCode());
    nonBugInternalServerErrors.add(GenericError.BIOS_APPS_DEPLOYMENT_FAILED.getErrorCode());
  }

  public ServiceException(String message, Response.Status status) {
    super(message, status);
  }

  @Override
  public Response getResponse() {
    Response resp = super.getResponse();
    return Response.status(resp.getStatus())
        .entity(
            new ErrorResponsePayload(
                Response.Status.fromStatusCode(resp.getStatus()), getMessage()))
        .build();
  }
}
