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
package io.isima.bios.errors.mapper;

import io.isima.bios.errors.exception.BiAppException;
import io.isima.bios.models.ErrorResponsePayload;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class BiAppExceptionMapper implements ExceptionMapper<BiAppException> {

  @Override
  public Response toResponse(BiAppException exception) {
    Status status = Status.BAD_REQUEST;
    if (exception.getStatusCode() != -1) {
      status = Status.fromStatusCode(exception.getStatusCode());
    }
    return Response.status(status)
        .type(MediaType.APPLICATION_JSON)
        .entity(new ErrorResponsePayload(status, exception.getMessage()))
        .build();
  }
}
