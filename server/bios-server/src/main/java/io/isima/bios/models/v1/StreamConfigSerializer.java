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
package io.isima.bios.models.v1;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamConfigSerializer extends StdSerializer<StreamConfig> {
  private static final long serialVersionUID = -7502502084010073276L;

  private static final Logger logger = LoggerFactory.getLogger(StreamConfigSerializer.class);

  public StreamConfigSerializer() {
    this(null);
  }

  public StreamConfigSerializer(Class<StreamConfig> t) {
    super(t);
  }

  @Override
  public void serialize(StreamConfig streamConfig, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {

    jgen.writeStartObject();
    serializeCore(streamConfig, jgen, provider);

    // additionalAttributes
    if (streamConfig.getAdditionalAttributes() != null) {
      final List<AttributeDesc> additional =
          streamConfig.getAdditionalAttributes().stream()
              .map(
                  attr -> {
                    AttributeDesc toWrite;
                    if (attr.getMissingValuePolicy() == streamConfig.missingValuePolicy) {
                      toWrite = attr.duplicate();
                      toWrite.setMissingValuePolicy(null);
                    } else {
                      toWrite = attr;
                    }
                    return toWrite;
                  })
              .collect(Collectors.toList());
      jgen.writeObjectField("additionalAttributes", additional);
    }

    jgen.writeEndObject();
  }

  public static void serializeCore(
      StreamConfig streamConfig, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    // name
    jgen.writeStringField("name", streamConfig.name);

    // type
    jgen.writeObjectField("type", streamConfig.type);

    if (streamConfig.streamId != null) {
      final String fieldName;
      switch (streamConfig.type) {
        case SIGNAL:
          fieldName = "signalId";
          break;
        case CONTEXT:
          fieldName = "contextId";
          break;
        default:
          fieldName = "streamId";
      }
      jgen.writeStringField(fieldName, streamConfig.streamId);
    }

    // missingValuePolicy
    jgen.writeObjectField("missingValuePolicy", streamConfig.missingValuePolicy);

    // version
    if (streamConfig.version != null) {
      jgen.writeNumberField("version", streamConfig.version);
    }

    // attributes
    List<AttributeDesc> attributes =
        streamConfig.getAttributes().stream()
            .map(
                attr -> {
                  AttributeDesc toWrite;
                  if (attr.getMissingValuePolicy() == streamConfig.missingValuePolicy) {
                    toWrite = attr.duplicate();
                    toWrite.setMissingValuePolicy(null);
                  } else {
                    toWrite = attr;
                  }
                  return toWrite;
                })
            .collect(Collectors.toList());
    jgen.writeObjectField("attributes", attributes);

    // primary key
    if (streamConfig.primaryKey != null) {
      jgen.writeObjectField("primaryKey", streamConfig.primaryKey);
    }

    // views
    if (streamConfig.views != null) {
      jgen.writeObjectField("views", streamConfig.views);
    }

    // preprocesses
    if (streamConfig.preprocesses != null) {
      jgen.writeObjectField("preprocesses", streamConfig.preprocesses);
    }

    if (streamConfig.postprocesses != null) {
      jgen.writeObjectField("postprocesses", streamConfig.postprocesses);
    }

    if (streamConfig.getMissingLookupPolicy() != null) {
      jgen.writeObjectField("globalLookupPolicy", streamConfig.missingLookupPolicy);
    }

    if (streamConfig.getExportDestinationId() != null) {
      jgen.writeObjectField("exportDestinationId", streamConfig.exportDestinationId);
    }

    if (streamConfig.enrichments != null) {
      jgen.writeObjectField("enrichments", streamConfig.enrichments);
    }

    if (streamConfig.ingestTimeLag != null) {
      jgen.writeObjectField("ingestTimeLag", streamConfig.ingestTimeLag);
    }
    // auditEnabled Flag for contexts only
    if (streamConfig.type == StreamType.CONTEXT) {
      jgen.writeBooleanField("auditEnabled", streamConfig.getAuditEnabled());
    }
    if (streamConfig.ttl != null) {
      jgen.writeObjectField("ttl", streamConfig.ttl);
    }
    if (streamConfig.features != null) {
      jgen.writeObjectField("features", streamConfig.features);
    }
  }
}
