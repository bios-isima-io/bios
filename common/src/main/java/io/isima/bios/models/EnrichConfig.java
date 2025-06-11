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
import lombok.Getter;
import lombok.Setter;

/** Enrich schema. */
@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
public class EnrichConfig {
  /**
   * Missing lookup policy applied commonly to the enrichments. This configuration can be overridden
   * by property {@link EnrichmentConfig#missingLookupPolicy} in enrichments.
   */
  @JsonProperty("missingLookupPolicy")
  private MissingLookupPolicy missingLookupPolicy;

  /** Enrichment schema definitions. */
  @JsonProperty("enrichments")
  private List<EnrichmentConfigSignal> enrichments;

  /** Dynamic enrichment configurations. */
  @JsonProperty("ingestTimeLag")
  private List<IngestTimeLagEnrichmentConfig> ingestTimeLag;

  public EnrichConfig() {}

  public EnrichConfig(List<EnrichmentConfigSignal> enrichments) {
    this.enrichments = enrichments;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof EnrichConfig)) {
      return false;
    }
    final var that = (EnrichConfig) other;
    return Objects.equals(missingLookupPolicy, that.missingLookupPolicy)
        && Objects.equals(enrichments, that.enrichments)
        && Objects.equals(ingestTimeLag, that.ingestTimeLag);
  }

  @Override
  public int hashCode() {
    return Objects.hash(missingLookupPolicy, enrichments, ingestTimeLag);
  }
}
