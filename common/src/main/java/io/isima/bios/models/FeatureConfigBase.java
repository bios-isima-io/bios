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
public class FeatureConfigBase extends Duplicatable {
  /** Feature name. */
  @JsonProperty("featureName")
  private String name;

  /** Dimensions to group by. */
  @JsonProperty("dimensions")
  protected List<String> dimensions;

  /** Attributes on which to calculate rollups or more generally, data sketches. */
  @JsonProperty("attributes")
  private List<String> attributes;

  /** Rollup interval in milliseconds. */
  @JsonProperty("featureInterval")
  private Long featureInterval;

  /**
   * Enable indexing. At least one of "aggregated" and "indexed" must be true for a feature in a
   * context.
   *
   * <p>This is an optional parameter. The value is treated as false if omitted.
   */
  @JsonProperty("indexed")
  private Boolean indexed;

  /**
   * Type of the index.
   *
   * <p>This is an optional parameter. The value is treated as ExactMatch if omitted.
   */
  @JsonProperty("indexType")
  private IndexType indexType;

  @JsonProperty("timeIndexInterval")
  private Long timeIndexInterval;

  @JsonProperty("indexOnInsert")
  private Boolean indexOnInsert;

  public FeatureConfigBase() {
    super();
  }

  /**
   * Method to make a clone of the instance.
   *
   * @return duplicated instance.
   */
  @Override
  public FeatureConfigBase duplicate() {
    return new FeatureConfigBase(this);
  }

  public FeatureConfigBase(FeatureConfigBase src) {
    name = src.name;
    if (src.dimensions != null) {
      dimensions = new ArrayList<>(src.dimensions);
    }
    if (src.attributes != null) {
      attributes = new ArrayList<>(src.attributes);
    }
    featureInterval = src.featureInterval;
    indexed = src.indexed;
    indexType = src.indexType;
    timeIndexInterval = src.timeIndexInterval;
    indexOnInsert = src.indexOnInsert;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FeatureConfigBase)) {
      return false;
    }
    final var other = (FeatureConfigBase) obj;
    return Objects.equals(name, other.name)
        && Objects.equals(dimensions, other.dimensions)
        && Objects.equals(attributes, other.attributes)
        && Objects.equals(featureInterval, other.featureInterval)
        && Objects.equals(indexed, other.indexed)
        && Objects.equals(indexType, other.indexType)
        && Objects.equals(timeIndexInterval, other.timeIndexInterval)
        && Objects.equals(indexOnInsert, other.indexOnInsert);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        dimensions,
        attributes,
        featureInterval,
        indexed,
        indexType,
        timeIndexInterval,
        indexOnInsert);
  }
}
