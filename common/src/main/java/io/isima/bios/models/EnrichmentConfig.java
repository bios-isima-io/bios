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
import java.util.List;
import java.util.Objects;
import lombok.ToString;

/** Enrichment schema. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@ToString
public abstract class EnrichmentConfig extends Duplicatable {
  /** Enrichment name. */
  @JsonProperty("enrichmentName")
  protected String name;

  /**
   * Attribute from the containing stream (signal or context) to be used as the foreign key to join
   * with a context.
   */
  @JsonProperty("foreignKey")
  protected List<String> foreignKey;

  /** Behavior when the context does not have an entry for the specified foreign key. */
  @JsonProperty("missingLookupPolicy")
  protected MissingLookupPolicy missingLookupPolicy;

  public EnrichmentConfig() {}

  public EnrichmentConfig(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getForeignKey() {
    return foreignKey;
  }

  public void setForeignKey(List<String> foreignKey) {
    this.foreignKey = foreignKey;
  }

  public MissingLookupPolicy getMissingLookupPolicy() {
    return missingLookupPolicy;
  }

  public void setMissingLookupPolicy(MissingLookupPolicy missingLookupPolicy) {
    this.missingLookupPolicy = missingLookupPolicy;
  }

  @Override
  public Duplicatable duplicate() {
    // This should not be called directly; it should be called on child classes.
    return null;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof EnrichmentConfig)) {
      return false;
    }
    final var that = (EnrichmentConfig) other;
    return Objects.equals(name, that.name)
        && Objects.equals(foreignKey, that.foreignKey)
        && Objects.equals(missingLookupPolicy, that.missingLookupPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, foreignKey, missingLookupPolicy);
  }
}
