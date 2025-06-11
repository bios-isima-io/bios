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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Feature schema. */
@Getter
@Setter
@ToString
@JsonInclude(Include.NON_NULL)
@JsonPropertyOrder({
  "featureName",
  "biosVersion",
  "dimensions",
  "attributes",
  "dataSketches",
  "featureInterval",
  "alerts",
  "featureAsContextName",
  "items",
  "ttl",
  "aggregated",
  "indexed",
  "indexType",
  "timeIndexInterval",
  "aggregated",
  "materializedAs",
  "indexOnInsert"
})
public class SignalFeatureConfig extends FeatureConfigBase {
  /** List of data sketches to be built for this feature. */
  @JsonProperty("dataSketches")
  private List<DataSketchType> dataSketches;

  /** Alert configurations. */
  @JsonProperty("alerts")
  private List<AlertConfig> alerts;

  @JsonProperty("featureAsContextName")
  private String featureAsContextName;

  @JsonProperty("items")
  private Long items;

  @JsonProperty("ttl")
  private Long ttl;

  /**
   * Enable aggregation for contexts - similar to base features in signals. At least one of
   * "aggregated" and "indexed" must be true for a feature in a context.
   *
   * <p>This is an optional parameter. The value is treated as false if omitted.
   */
  @JsonProperty("aggregated")
  private Boolean aggregated;

  @JsonProperty("materializedAs")
  private MaterializedAs materializedAs;

  public SignalFeatureConfig() {
    super();
  }

  /**
   * Method to make a clone of the instance.
   *
   * @return duplicated instance.
   */
  @Override
  public SignalFeatureConfig duplicate() {
    return new SignalFeatureConfig(this);
  }

  public SignalFeatureConfig(SignalFeatureConfig src) {
    super(src);
    if (src.dataSketches != null) {
      dataSketches = new ArrayList<>(src.dataSketches);
    }
    alerts = copyList(src.alerts);
    featureAsContextName = src.featureAsContextName;
    items = src.items;
    ttl = src.ttl;
    aggregated = src.aggregated;
    materializedAs = src.materializedAs;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof SignalFeatureConfig)) {
      return false;
    }
    final var other = (SignalFeatureConfig) obj;
    return Objects.equals(dimensions, other.dimensions)
        && Objects.equals(dataSketches, other.dataSketches)
        && Objects.equals(alerts, other.alerts)
        && Objects.equals(featureAsContextName, other.featureAsContextName)
        && Objects.equals(items, other.items)
        && Objects.equals(ttl, other.ttl)
        && Objects.equals(aggregated, other.aggregated)
        && Objects.equals(materializedAs, other.materializedAs);
  }

  @Override
  public int hashCode() {
    return super.hashCode()
        + Objects.hash(
            dimensions,
            dataSketches,
            alerts,
            featureAsContextName,
            items,
            ttl,
            aggregated,
            materializedAs);
  }
}
