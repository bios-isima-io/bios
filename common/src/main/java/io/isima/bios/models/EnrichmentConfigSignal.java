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

/** Enrichment schema. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class EnrichmentConfigSignal extends EnrichmentConfig {
  public EnrichmentConfigSignal() {
    super();
  }

  public EnrichmentConfigSignal(String name) {
    super(name);
  }

  /** Name of the context to lookup. */
  @JsonProperty("contextName")
  private String contextName;

  /** Context attributes to join. */
  @JsonProperty("contextAttributes")
  private List<EnrichmentAttribute> contextAttributes;

  public String getContextName() {
    return contextName;
  }

  public void setContextName(String contextName) {
    this.contextName = contextName;
  }

  public List<EnrichmentAttribute> getContextAttributes() {
    return contextAttributes;
  }

  public void setContextAttributes(List<EnrichmentAttribute> contextAttributes) {
    this.contextAttributes = contextAttributes;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof EnrichmentConfigSignal)) {
      return false;
    }
    final var that = (EnrichmentConfigSignal) other;
    return super.equals(that)
        && Objects.equals(contextName, that.contextName)
        && Objects.equals(contextAttributes, that.contextAttributes);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hash(contextName, contextAttributes);
  }
}
