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
package io.isima.bios.server.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import io.isima.bios.errors.exception.InvalidRequestException;
import io.isima.bios.exceptions.BiosServerException;
import io.isima.bios.exceptions.InternalServerErrorException;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.ByteBufOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;

public class JsonPayloadCodec implements PayloadCodec {
  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();

  private static final Validator validator =
      Validation.buildDefaultValidatorFactory().getValidator();

  private boolean validationEnabled = false;

  public JsonPayloadCodec enableValidation() {
    validationEnabled = true;
    return this;
  }

  @Override
  public <T> T decode(ByteBuf payload, Class<T> clazz) throws InvalidRequestException {
    Objects.requireNonNull(clazz);
    try (final InputStream inputStream = new ByteBufInputStream(payload)) {
      final var decoded = payload == null ? null : mapper.readValue(inputStream, clazz);
      if (validationEnabled) {
        final var violations = validator.validate(decoded);
        if (!violations.isEmpty()) {
          throw new InvalidRequestException(buildViolationMessage(violations));
        }
      }
      return decoded;
    } catch (UnrecognizedPropertyException e) {
      final var ex =
          new InvalidRequestException(
              "Invalid request JSON: Unrecognized field name: %s", e.getPropertyName());
      ex.setInternalMessage(String.format("class %s", e.getReferringClass().getSimpleName()));
      throw ex;
    } catch (IOException e) {
      final var ex = new InvalidRequestException("Invalid request JSON: %s", e.getMessage());
      ex.setInternalMessage(e.getMessage());
      throw ex;
    }
  }

  private <T> String buildViolationMessage(Set<ConstraintViolation<T>> violations) {
    assert !violations.isEmpty();
    final var contentClass = violations.iterator().next().getRootBeanClass();
    final var sb = new StringBuilder("class ").append(contentClass.getSimpleName()).append(": ");
    sb.append(
        violations.stream()
            .map(
                (violation) ->
                    String.format(
                        "Property '%s' %s", violation.getPropertyPath(), violation.getMessage()))
            .collect(Collectors.joining("; ")));
    return sb.toString();
  }

  @Override
  public <T> void encode(T content, ByteBuf payload) throws BiosServerException {
    Objects.requireNonNull(content);
    Objects.requireNonNull(payload);
    try {
      final OutputStream outputStream = new ByteBufOutputStream(payload);
      mapper.writeValue(outputStream, content);
    } catch (IOException e) {
      throw new InternalServerErrorException("Failed to encode output content", e);
    }
  }
}
