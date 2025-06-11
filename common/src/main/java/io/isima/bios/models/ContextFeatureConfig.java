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
  "featureInterval",
  "aggregated",
  "indexed",
  "indexType",
  "timeIndexInterval",
  "aggregated",
  "indexOnInsert"
})
public class ContextFeatureConfig extends FeatureConfigBase {
  /**
   * Enable aggregation for contexts - similar to base features in signals. At least one of
   * "aggregated" and "indexed" must be true for a feature in a context.
   *
   * <p>This is an optional parameter. The value is treated as false if omitted.
   */
  @JsonProperty("aggregated")
  private Boolean aggregated;

  public ContextFeatureConfig() {
    super();
  }

  /**
   * Method to make a clone of the instance.
   *
   * @return duplicated instance.
   */
  @Override
  public ContextFeatureConfig duplicate() {
    return new ContextFeatureConfig(this);
  }

  public ContextFeatureConfig(ContextFeatureConfig src) {
    super(src);
    aggregated = src.aggregated;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }
    if (!(obj instanceof ContextFeatureConfig)) {
      return false;
    }
    final var other = (ContextFeatureConfig) obj;
    return Objects.equals(dimensions, other.dimensions)
        && Objects.equals(aggregated, other.aggregated);
  }

  @Override
  public int hashCode() {
    return super.hashCode() + Objects.hash(dimensions, aggregated);
  }
}
