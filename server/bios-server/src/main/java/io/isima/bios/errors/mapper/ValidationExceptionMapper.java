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

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Path.Node;
import javax.validation.ValidationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class ValidationExceptionMapper implements ExceptionMapper<ValidationException> {

  @Override
  public Response toResponse(ValidationException ex) {
    if (ex instanceof ConstraintViolationException) {
      return handleConstraintViolationException((ConstraintViolationException) ex);
    }

    return Response.status(Status.BAD_REQUEST)
        .type(MediaType.TEXT_PLAIN)
        .entity("bad: " + unwrapException(ex))
        .build();
  }

  private Response handleConstraintViolationException(ConstraintViolationException ex) {
    StringBuilder sb = new StringBuilder();
    String delimiter = "";
    for (ConstraintViolation<?> v : ex.getConstraintViolations()) {
      String lastName = "";
      for (Node n : v.getPropertyPath()) {
        lastName = n.getName();
      }
      sb.append(delimiter);
      if (lastName != null) {
        sb.append("'" + lastName + "' ");
      }
      sb.append(v.getMessage());
      delimiter = ";";
    }
    return Response.status(Status.BAD_REQUEST)
        .type(MediaType.TEXT_PLAIN)
        .entity(sb.toString())
        .build();
  }

  protected String unwrapException(Throwable t) {
    StringBuffer sb = new StringBuffer();
    unwrapException(sb, t);
    return sb.toString();
  }

  private void unwrapException(StringBuffer sb, Throwable t) {
    if (t == null) {
      return;
    }
    sb.append(t.getMessage());
    if (t.getCause() != null && t != t.getCause()) {
      sb.append('[');
      unwrapException(sb, t.getCause());
      sb.append(']');
    }
  }
}
