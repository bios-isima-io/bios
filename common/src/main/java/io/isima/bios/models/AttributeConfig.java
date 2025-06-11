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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Attribute schema. */
@Getter
@Setter
@ToString
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize(using = AttributeConfigDeserializer.class)
@JsonPropertyOrder({
  "attributeName",
  "type",
  "tags",
  "inferredTags",
  "allowedValues",
  "missingAttributePolicy",
  "default"
})
public class AttributeConfig {
  /** Attribute name. */
  @JsonProperty("attributeName")
  protected String name;

  /** Attribute value datatype. */
  @JsonProperty("type")
  protected AttributeType type;

  /** Tags (including kind of values are stored, unit, etc.) specified by the user. */
  protected AttributeTags tags;

  /** Tags inferred by bios; can be overridden by the user by specifying "tags" above. */
  protected AttributeTags inferredTags;

  /** Allowed attribute values (enums). */
  @JsonProperty("allowedValues")
  protected List<AttributeValueGeneric> allowedValues;

  /** Behavior when the attribute value is missing in ingestion. */
  @JsonProperty("missingAttributePolicy")
  protected MissingAttributePolicy missingAttributePolicy;

  /** The default value used when missingAttributePolicy is STORE_DEFAULT_VALUE. */
  @JsonProperty("default")
  protected AttributeValueGeneric defaultValue;

  public AttributeConfig() {}

  public AttributeConfig(String name, AttributeType type) {
    this.name = name;
    this.type = type;
  }

  public AttributeConfig(AttributeConfig src) {
    this.name = src.name;
    this.type = src.type;
    this.tags = src.tags;
    this.inferredTags = src.inferredTags;
    if (src.allowedValues != null) {
      this.allowedValues = List.copyOf(src.allowedValues);
    }
    this.missingAttributePolicy = src.missingAttributePolicy;
    this.defaultValue = src.defaultValue;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof AttributeConfig)) {
      return false;
    }
    final var that = (AttributeConfig) other;
    return Objects.equals(name, that.name)
        && Objects.equals(type, that.type)
        && Objects.equals(tags, that.tags)
        && Objects.equals(inferredTags, that.inferredTags)
        && Objects.equals(allowedValues, that.allowedValues)
        && Objects.equals(missingAttributePolicy, that.missingAttributePolicy)
        && Objects.equals(defaultValue, that.defaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name, type, tags, inferredTags, allowedValues, missingAttributePolicy, defaultValue);
  }
}
