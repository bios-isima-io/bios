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
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.ContentRepresentation;
import io.isima.bios.models.ContextEntryRecord;
import java.util.List;

/** Get context entries response. */
@JsonPropertyOrder({"contentRepresentation", "primaryKey", "definitions", "entries"})
public class GetContextEntriesResponse {
  @JsonProperty("contentRepresentation")
  ContentRepresentation contentRepresentation;

  @JsonProperty("primaryKey")
  private List<String> primaryKey;

  @JsonProperty("definitions")
  private List<AttributeConfig> definitions;

  @JsonProperty("entries")
  private List<ContextEntryRecord> entries;

  public ContentRepresentation getContentRepresentation() {
    return contentRepresentation;
  }

  public void setContentRepresentation(ContentRepresentation contentRepresentation) {
    this.contentRepresentation = contentRepresentation;
  }

  public List<String> getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(List<String> primaryKey) {
    this.primaryKey = primaryKey;
  }

  public List<AttributeConfig> getDefinitions() {
    return definitions;
  }

  public void setDefinitions(List<AttributeConfig> definitions) {
    this.definitions = definitions;
  }

  public List<ContextEntryRecord> getEntries() {
    return entries;
  }

  public void setEntries(List<ContextEntryRecord> contextEntries) {
    this.entries = contextEntries;
  }
}
