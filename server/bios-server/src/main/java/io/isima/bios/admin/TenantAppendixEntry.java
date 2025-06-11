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
package io.isima.bios.admin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class TenantAppendixEntry {
  private static final ObjectMapper objectMapper = BiosObjectMapperProvider.get();

  @NonNull private String entryId;
  @NonNull private String config;
  private long entryVersion;

  /**
   * Static method to build an instance from a source entity.
   *
   * @param <T> Type of the entity
   * @param entryId Entry ID
   * @param source The content
   * @return Built appendix entry
   * @throws ApplicationException thrown to indicate that something unexpected has happened.
   */
  public static <T> TenantAppendixEntry build(String entryId, T source)
      throws ApplicationException {
    Objects.requireNonNull(source);
    final long entryVersion = System.currentTimeMillis();
    try {
      return new TenantAppendixEntry(
          entryId, objectMapper.writeValueAsString(source), entryVersion);
    } catch (JsonProcessingException e) {
      throw new ApplicationException(
          String.format(
              "Failed to encode entity; class=%s, error=%s, entity=%s",
              source.getClass(), e, source),
          e);
    }
  }

  public static TenantAppendixEntry buildBlankEntry(String entryId) {
    final long entryVersion = System.currentTimeMillis();
    return new TenantAppendixEntry(entryId, "", entryVersion);
  }

  /**
   * Converts the JSON serialized tenant appendix entry to the specified type.
   *
   * @param <T> The specified type
   * @param clazz Class object of the specified type
   * @return Decoded object
   * @throws ApplicationException Thrown to indicate that the decoding has failed
   */
  public <T> T decode(Class<T> clazz) throws ApplicationException {
    try {
      return clazz.cast(objectMapper.readValue(config, clazz));
    } catch (IOException e) {
      throw new ApplicationException("TenantAppendix decoding failed: " + e.toString(), e);
    }
  }
}
