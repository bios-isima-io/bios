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
package io.isima.bios.models;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

class AttributeConfigDeserializer extends StdDeserializer<AttributeConfig> {
  private static final long serialVersionUID = 2053897345737985084L;

  protected AttributeConfigDeserializer() {
    this(null);
  }

  protected AttributeConfigDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public AttributeConfig deserialize(JsonParser jp, DeserializationContext deserializationContext)
      throws IOException {
    final AttributeConfig attribute = new AttributeConfig();
    final ObjectMapper mapper = BiosObjectMapperProvider.get();
    try {
      final JsonNode parentNode = jp.getCodec().readTree(jp);
      attribute.setName(
          Optional.ofNullable(parentNode.get("attributeName"))
              .orElseThrow(() -> new JsonParseException(jp, "Property 'attributeName' must exist"))
              .asText());

      final String typeText =
          Optional.ofNullable(parentNode.get("type"))
              .orElseThrow(() -> new JsonParseException(jp, "Property 'type' must exist"))
              .asText();
      final AttributeType type = AttributeType.destringify(typeText);
      attribute.setType(type);

      final var tags = parentNode.get("tags");
      if (tags != null) {
        attribute.setTags(mapper.readValue(mapper.writeValueAsString(tags), AttributeTags.class));
      }

      Optional.ofNullable(parentNode.get("allowedValues"))
          .ifPresent(
              node -> {
                final List<AttributeValueGeneric> allowedValues = new ArrayList<>();
                node.forEach(
                    item -> allowedValues.add(new AttributeValueGeneric(item.asText(), type)));
                attribute.setAllowedValues(allowedValues);
              });

      Optional.ofNullable(parentNode.get("missingAttributePolicy"))
          .ifPresent(
              node ->
                  attribute.setMissingAttributePolicy(
                      MissingAttributePolicy.forValue(node.asText())));

      Optional.ofNullable(parentNode.get("default"))
          .ifPresent(
              node -> attribute.setDefaultValue(new AttributeValueGeneric(node.asText(), type)));

    } catch (IllegalArgumentException e) {
      throw new JsonParseException(jp, e.getMessage());
    }
    return attribute;
  }
}
