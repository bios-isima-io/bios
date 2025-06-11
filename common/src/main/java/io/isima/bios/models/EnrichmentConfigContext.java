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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.ToString;

/** Enrichment schema for contexts. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@ToString(callSuper = true)
public class EnrichmentConfigContext extends EnrichmentConfig {
  /**
   * Definitions of enriched attributes including source and attribute values (which may include
   * some calculations).
   */
  @JsonProperty("enrichedAttributes")
  @NotNull
  @Size(min = 1)
  private List<EnrichedAttribute> enrichedAttributes;

  public EnrichmentConfigContext() {}

  public EnrichmentConfigContext(String name) {
    super(name);
  }

  public List<EnrichedAttribute> getEnrichedAttributes() {
    return enrichedAttributes;
  }

  public void setEnrichedAttributes(List<EnrichedAttribute> enrichedAttributes) {
    this.enrichedAttributes = enrichedAttributes;
  }

  public EnrichmentConfigContext addEnrichedAttribute(EnrichedAttribute enrichedAttribute) {
    if (enrichedAttributes == null) {
      enrichedAttributes = new ArrayList<>();
    }
    enrichedAttributes.add(enrichedAttribute);
    return this;
  }

  @Override
  public EnrichmentConfigContext duplicate() {
    return new EnrichmentConfigContext().duplicate(this);
  }

  protected EnrichmentConfigContext duplicate(EnrichmentConfigContext src) {
    name = src.name;
    if (src.foreignKey != null) {
      foreignKey = new ArrayList<>(src.foreignKey);
    }
    missingLookupPolicy = src.missingLookupPolicy;
    enrichedAttributes = copyList(src.enrichedAttributes);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof EnrichmentConfigContext)) {
      return false;
    }

    final EnrichmentConfigContext test = (EnrichmentConfigContext) o;
    if (!(name == null ? test.name == null : name.equalsIgnoreCase(test.name))) {
      return false;
    }
    if (!(foreignKey == null ? test.foreignKey == null : foreignKey.equals(test.foreignKey))) {
      return false;
    }
    if (!(missingLookupPolicy == null
        ? test.missingLookupPolicy == null
        : missingLookupPolicy.equals(test.missingLookupPolicy))) {
      return false;
    }
    if (!(enrichedAttributes == null
        ? test.enrichedAttributes == null
        : enrichedAttributes.equals(test.enrichedAttributes))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, foreignKey, missingLookupPolicy, enrichedAttributes);
  }
}
