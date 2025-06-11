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
package io.isima.bios.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import io.isima.bios.models.ContentRepresentation;
import java.util.List;
import javax.validation.constraints.NotNull;

@JsonPropertyOrder({"contentRepresentation", "primaryKey", "attributes"})
public class UpdateContextEntryRequest {
  @NotNull
  @JsonProperty("contentRepresentation")
  private ContentRepresentation contentRepresentation;

  @NotNull
  @JsonProperty("primaryKey")
  private List<Object> primaryKey;

  @NotNull
  @JsonProperty("attributes")
  private List<AttributeSpec> attributes;

  public ContentRepresentation getContentRepresentation() {
    return contentRepresentation;
  }

  public void setContentRepresentation(ContentRepresentation contentRepresentation) {
    this.contentRepresentation = contentRepresentation;
  }

  public List<Object> getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(List<Object> primaryKey) {
    this.primaryKey = primaryKey;
  }

  public List<AttributeSpec> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<AttributeSpec> attributes) {
    this.attributes = attributes;
  }
}
