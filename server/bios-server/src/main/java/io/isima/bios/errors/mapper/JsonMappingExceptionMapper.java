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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonMappingException.Reference;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.ErrorResponsePayload;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class JsonMappingExceptionMapper implements ExceptionMapper<JsonMappingException> {
  private static final Pattern p = Pattern.compile("^.*, problem:\\s*(.*)");

  @Override
  public Response toResponse(JsonMappingException e) {
    StringBuilder sb = new StringBuilder();
    for (Reference ref : e.getPath()) {
      if (ref.getIndex() >= 0) {
        sb.append('[').append(ref.getIndex()).append(']');
      }
      if (ref.getFieldName() != null) {
        if (sb.length() > 0) {
          sb.append('.');
        }
        sb.append(ref.getFieldName());
      }
    }
    sb.append(": ");
    if (e.getCause() instanceof TfosException) {
      final TfosException ex = (TfosException) e.getCause();
      sb.append(ex.getMessage());
    } else {
      Matcher m = p.matcher(e.getMessage());
      if (m.find()) {
        sb.append(m.group(1));
      } else {
        String[] lines = e.getMessage().split("\n");
        for (int i = 0; i < 2; ++i) {
          if (i == lines.length) {
            break;
          }
          sb.append(lines[i].replaceAll("Source: \\S+; ", "").replace("\"", "\\\""));
        }
      }
    }
    return Response.status(Status.BAD_REQUEST)
        .type(MediaType.APPLICATION_JSON)
        .entity(new ErrorResponsePayload(Response.Status.BAD_REQUEST, sb.toString()))
        .build();
  }
}
