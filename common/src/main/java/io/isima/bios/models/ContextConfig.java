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
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Context schema. */
@Getter
@Setter
@ToString(callSuper = true)
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonPropertyOrder({
  "contextName",
  "version",
  "biosVersion",
  "missingAttributePolicy",
  "attributes",
  "primaryKey",
  "missingLookupPolicy",
  "enrichments",
  "features",
  "auditEnabled",
  "isInternal",
  "ttl"
})
public class ContextConfig extends BiosStreamConfig {

  /**
   * List of attributes used as the primary key. Currently we only support one attribute in this
   * list.
   */
  @JsonProperty("primaryKey")
  private List<String> primaryKey;

  /**
   * Missing lookup policy applied commonly to the enrichments. This configuration can be overridden
   * by property {@link EnrichmentConfig#missingLookupPolicy} in enrichments.
   */
  @JsonProperty("missingLookupPolicy")
  private MissingLookupPolicy missingLookupPolicy;

  /** Enrichment schema definitions. */
  @JsonProperty("enrichments")
  private List<EnrichmentConfigContext> enrichments;

  /** Features configuration. */
  @JsonProperty("features")
  private List<ContextFeatureConfig> features;

  /** Audit the context. */
  @JsonProperty("auditEnabled")
  private Boolean auditEnabled;

  /** Time to live in milliseconds - automatically delete entries after TTL. */
  @JsonProperty("ttl")
  private Long ttl;

  public ContextConfig() {
    super();
  }

  public ContextConfig(ContextConfig base) {
    this(base, true);
  }

  public ContextConfig(ContextConfig base, boolean includeInferredTags) {
    super(base, includeInferredTags);
    this.primaryKey = base.primaryKey;
    this.missingLookupPolicy = base.missingLookupPolicy;
    this.enrichments = base.enrichments;
    this.auditEnabled = base.auditEnabled;
    this.ttl = base.ttl;
    this.features = base.features;
  }

  public ContextConfig(String name) {
    super();
    super.setName(name);
  }

  public ContextConfig(String name, Long version) {
    super.setName(name);
    super.setVersion(version);
  }

  @Override
  @JsonProperty("contextName")
  public String getName() {
    return super.getName();
  }

  @Override
  @JsonProperty("contextName")
  public void setName(String name) {
    super.setName(name);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ContextConfig)) {
      return false;
    }
    final var that = (ContextConfig) other;
    return super.equals(other)
        && Objects.equals(primaryKey, that.primaryKey)
        && Objects.equals(missingLookupPolicy, that.missingLookupPolicy)
        && Objects.equals(enrichments, that.enrichments)
        && Objects.equals(features, that.features)
        && Objects.equals(auditEnabled, that.auditEnabled)
        && Objects.equals(ttl, that.ttl);
  }

  @Override
  public int hashCode() {
    return super.hashCode()
        + Objects.hash(primaryKey, missingLookupPolicy, enrichments, features, auditEnabled, ttl);
  }
}
