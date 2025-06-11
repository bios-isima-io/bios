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
package io.isima.bios.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.LocalDate;
import java.util.TimeZone;

/** Class that provides ObjectMapper instances that is specialized for TFOS service. */
public class TfosObjectMapperProvider {
  private static final ObjectMapper tfosMapper;

  static {
    tfosMapper =
        new ObjectMapper()
            .registerModule(
                new SimpleModule().addSerializer(LocalDate.class, new LocalDateSerializer()))
            .configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  public static ObjectMapper get() {
    return tfosMapper;
  }

  /**
   * This class serializes LocalDate objects.
   *
   * <p>This serializer class converts a LocalDate object to an ISO-8601 calendar string, such as
   * 2007-12-03. The serializer is meant be registered to the ObjectMapper owned by parent
   * {@ObjectMapperProvider} class.
   *
   * <p>Jackson-providing JavaTimeModule class can do similar serialization, but we cannot use the
   * class because we want to configure the serializer as WRITE_DATES_AS_TIMESTAMPS=false for
   * LocalDate (used for DATE attribute type) type but WRITE_DATES_AS_TIMESTAMPS=true for Date (used
   * for TIMESTAMP attribute type).
   *
   * <p>This class is instroduced to solve this problem.
   */
  private static class LocalDateSerializer extends StdSerializer<LocalDate> {
    /** generated version uid. */
    private static final long serialVersionUID = -7933038046345741652L;

    public LocalDateSerializer() {
      this(null);
    }

    public LocalDateSerializer(Class<LocalDate> t) {
      super(t);
    }

    @Override
    public void serialize(LocalDate value, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      gen.writeString(value.toString());
    }
  }

  public static class TimeZoneDeserializer extends StdDeserializer<TimeZone> {
    private static final long serialVersionUID = 1959076510928924748L;

    public TimeZoneDeserializer() {
      this(null);
    }

    public TimeZoneDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public TimeZone deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      final String src = p.getValueAsString();
      if (src != null) {
        if (src.startsWith("UTC")) {
          return TimeZone.getTimeZone(src.replaceAll("UTC", "GMT"));
        } else {
          throw new InvalidFormatException("not a valid TimeZone value", src, TimeZone.class);
        }
      }
      return null;
    }
  }
}
