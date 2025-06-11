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
package io.isima.bios.admin.v1;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.isima.bios.models.v1.StreamConfigSerializer;
import java.io.IOException;

public class StreamStoreDescSerializer extends StdSerializer<StreamStoreDesc> {
  private static final long serialVersionUID = -7502502084010073276L;

  public StreamStoreDescSerializer() {
    this(null);
  }

  public StreamStoreDescSerializer(Class<StreamStoreDesc> t) {
    super(t);
  }

  @Override
  public void serialize(
      StreamStoreDesc streamConfig, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {

    jgen.writeStartObject();

    StreamConfigSerializer.serializeCore(streamConfig, jgen, provider);

    // previous config name
    if (streamConfig.prevName != null) {
      jgen.writeStringField("prevName", streamConfig.prevName);
    }

    // previous config version
    if (streamConfig.prevVersion != null) {
      jgen.writeNumberField("prevVersion", streamConfig.prevVersion);
    }

    // table name
    if (streamConfig.tableName != null) {
      jgen.writeStringField("tableName", streamConfig.tableName);
    }

    // schema name
    if (streamConfig.schemaName != null) {
      jgen.writeStringField("schemaName", streamConfig.schemaName);
    }

    // schema version
    if (streamConfig.schemaVersion != null) {
      jgen.writeNumberField("schemaVersion", streamConfig.schemaVersion);
    }

    // data version
    if (streamConfig.formatVersion != null) {
      jgen.writeNumberField("formatVersion", streamConfig.formatVersion);
    }

    // index window length
    if (streamConfig.indexWindowLength != null) {
      jgen.writeNumberField("indexWindowLength", streamConfig.indexWindowLength);
    }

    if (streamConfig.referencedStreams != null) {
      jgen.writeObjectField("referencedStreams", streamConfig.referencedStreams);
    }

    if (streamConfig.streamNameProxy != null) {
      jgen.writeObjectField("streamNameProxy", streamConfig.streamNameProxy);
    }

    if (streamConfig.attributeProxyInfo != null) {
      jgen.writeObjectField("attributeProxyInfo", streamConfig.attributeProxyInfo);
    }

    if (streamConfig.maxAttributeProxy != null) {
      jgen.writeObjectField("maxAttributeProxy", streamConfig.maxAttributeProxy);
    }

    if (streamConfig.inferredTagsForAdditionalAttributes != null) {
      jgen.writeObjectField(
          "inferredTagsForAdditionalAttributes", streamConfig.inferredTagsForAdditionalAttributes);
    }

    jgen.writeEndObject();
  }
}
