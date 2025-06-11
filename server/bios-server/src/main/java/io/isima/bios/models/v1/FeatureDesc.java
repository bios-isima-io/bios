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
package io.isima.bios.models.v1;

import static java.lang.Boolean.TRUE;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.isima.bios.common.BiosConstants;
import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.ContextFeatureConfig;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.IndexType;
import io.isima.bios.models.MaterializedAs;
import io.isima.bios.models.SignalFeatureConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Feature schema. */
@Getter
@Setter
@ToString
@JsonInclude(Include.NON_NULL)
public class FeatureDesc extends Duplicatable {
  /** Feature name. */
  @JsonProperty("featureName")
  private String name;

  /** Storage schema version (equivalent to ViewDesc.schemaVersion). */
  @JsonProperty("biosVersion")
  private Long biosVersion;

  /** Dimensions to group by. */
  @JsonProperty("dimensions")
  protected List<String> dimensions;

  /** Attributes on which to calculate rollups or more generally, data sketches. */
  @JsonProperty("attributes")
  private List<String> attributes;

  /** List of data sketches to be built for this feature. */
  @JsonProperty("dataSketches")
  private List<DataSketchType> dataSketches;

  /** Rollup interval in milliseconds. */
  @JsonProperty("featureInterval")
  private Long featureInterval;

  /** Alert configurations. */
  @JsonProperty("alerts")
  private List<AlertConfig> alerts;

  @JsonProperty("featureAsContextName")
  private String featureAsContextName;

  @JsonProperty("lastN")
  private Boolean lastN;

  @JsonProperty("lastNItems")
  private Long lastNItems;

  @JsonProperty("lastNTtl")
  private Long lastNTtl;

  /**
   * Enable aggregation for contexts - similar to base features in signals. At least one of
   * "aggregated" and "indexed" must be true for a feature in a context.
   *
   * <p>This is an optional parameter. The value is treated as false if omitted.
   */
  @JsonProperty("aggregated")
  private Boolean aggregated;

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

  /**
   * Enable accumulated snapshots into a new snapshot context that will be automatically created.
   *
   * <p>This is an optional parameter. The value is treated as false if omitted.
   */
  @JsonProperty("snapshot")
  private Boolean snapshot;

  @JsonProperty("writeTimeIndexing")
  private Boolean writeTimeIndexing;

  public String getEffectiveFeatureAsContextName(String streamName) {
    if (featureAsContextName != null) {
      return featureAsContextName;
    }
    return streamName + "_" + name;
  }

  public FeatureDesc() {
    super();
  }

  /**
   * Method to make a clone of the instance.
   *
   * @return duplicated instance.
   */
  @Override
  public FeatureDesc duplicate() {
    FeatureDesc clone = new FeatureDesc();

    clone.name = name;
    clone.biosVersion = biosVersion;
    if (dimensions != null) {
      clone.dimensions = new ArrayList<>(dimensions);
    }
    if (attributes != null) {
      clone.attributes = new ArrayList<>(attributes);
    }
    if (dataSketches != null) {
      clone.dataSketches = new ArrayList<>(dataSketches);
    }
    clone.featureInterval = featureInterval;
    clone.alerts = copyList(alerts);
    clone.featureAsContextName = featureAsContextName;
    clone.lastN = lastN;
    clone.lastNItems = lastNItems;
    clone.lastNTtl = lastNTtl;
    clone.aggregated = aggregated;
    clone.indexed = indexed;
    clone.indexType = indexType;
    clone.timeIndexInterval = timeIndexInterval;
    clone.snapshot = snapshot;
    clone.writeTimeIndexing = writeTimeIndexing;

    return clone;
  }

  public FeatureDesc(FeatureDesc src) {
    name = src.name;
    biosVersion = src.biosVersion;
    dimensions = src.dimensions;
    attributes = src.attributes;
    dataSketches = src.dataSketches;
    featureInterval = src.featureInterval;
    alerts = src.alerts;
    featureAsContextName = src.featureAsContextName;
    lastN = src.lastN;
    lastNItems = src.lastNItems;
    lastNTtl = src.lastNTtl;
    aggregated = src.aggregated;
    indexed = src.indexed;
    indexType = src.indexType;
    timeIndexInterval = src.timeIndexInterval;
    snapshot = src.snapshot;
    writeTimeIndexing = src.writeTimeIndexing;
  }

  public FeatureDesc(SignalFeatureConfig src) {
    name = src.getName();
    dimensions = src.getDimensions();
    attributes = src.getAttributes();
    dataSketches = src.getDataSketches();
    featureInterval = src.getFeatureInterval();
    alerts = src.getAlerts();
    featureAsContextName = src.getFeatureAsContextName();
    lastN =
        (src.getDataSketches() != null && src.getDataSketches().contains(DataSketchType.LAST_N))
            || src.getMaterializedAs() == MaterializedAs.ACCUMULATING_COUNT;
    lastNItems = src.getItems();
    lastNTtl = src.getTtl();
    aggregated = src.getAggregated();
    indexed = src.getIndexed();
    indexType = src.getIndexType();
    timeIndexInterval = src.getTimeIndexInterval();
    snapshot = src.getMaterializedAs() == MaterializedAs.ACCUMULATING_COUNT;
    writeTimeIndexing = src.getIndexOnInsert();
  }

  public FeatureDesc(ContextFeatureConfig src) {
    name = src.getName();
    dimensions = src.getDimensions();
    attributes = src.getAttributes();
    featureInterval = src.getFeatureInterval();
    aggregated = src.getAggregated();
    indexed = src.getIndexed();
    indexType = src.getIndexType();
    timeIndexInterval = src.getTimeIndexInterval();
    writeTimeIndexing = src.getIndexOnInsert();
  }

  public SignalFeatureConfig toSignalFeatureConfig() {
    final var featureConfig = new SignalFeatureConfig();
    featureConfig.setName(name);
    featureConfig.setDimensions(dimensions);
    featureConfig.setAttributes(attributes);
    if (dataSketches != null) {
      final var filteredSketches =
          dataSketches.stream()
              .filter((sketch) -> sketch != DataSketchType.LAST_N)
              .collect(Collectors.toList());
      featureConfig.setDataSketches(filteredSketches.isEmpty() ? null : filteredSketches);
    }
    featureConfig.setFeatureInterval(featureInterval);
    featureConfig.setAlerts(alerts);
    featureConfig.setFeatureAsContextName(featureAsContextName);
    final boolean lastNEnabled =
        lastN == TRUE || (dataSketches != null && dataSketches.contains(DataSketchType.LAST_N));
    if (lastNEnabled) {
      featureConfig.setMaterializedAs(MaterializedAs.LAST_N);
    }
    featureConfig.setItems(lastNItems);
    featureConfig.setTtl(lastNTtl);
    if (lastNEnabled && featureConfig.getTtl() == null) {
      featureConfig.setTtl(BiosConstants.DEFAULT_LAST_N_COLLECTION_TTL);
    }
    featureConfig.setTimeIndexInterval(timeIndexInterval);
    featureConfig.setIndexed(indexed);
    featureConfig.setIndexType(indexType);
    if (snapshot == TRUE) {
      featureConfig.setMaterializedAs(MaterializedAs.ACCUMULATING_COUNT);
    }
    featureConfig.setIndexOnInsert(writeTimeIndexing);
    return featureConfig;
  }

  public ContextFeatureConfig toContextFeatureConfig() {
    final var featureConfig = new ContextFeatureConfig();
    featureConfig.setName(name);
    featureConfig.setDimensions(dimensions);
    featureConfig.setAttributes(attributes);
    featureConfig.setFeatureInterval(featureInterval);
    featureConfig.setTimeIndexInterval(timeIndexInterval);
    featureConfig.setIndexed(indexed);
    featureConfig.setIndexType(indexType);
    featureConfig.setIndexOnInsert(writeTimeIndexing);
    return featureConfig;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FeatureDesc)) {
      return false;
    }
    final var other = (FeatureDesc) obj;
    return Objects.equals(name, other.name)
        && Objects.equals(biosVersion, other.biosVersion)
        && Objects.equals(dimensions, other.dimensions)
        && Objects.equals(attributes, other.attributes)
        && Objects.equals(dataSketches, other.dataSketches)
        && Objects.equals(featureInterval, other.featureInterval)
        && Objects.equals(alerts, other.alerts)
        && Objects.equals(featureAsContextName, other.featureAsContextName)
        && Objects.equals(lastN, other.lastN)
        && Objects.equals(lastNItems, other.lastNItems)
        && Objects.equals(lastNTtl, other.lastNTtl)
        && Objects.equals(aggregated, other.aggregated)
        && Objects.equals(indexed, other.indexed)
        && Objects.equals(indexType, other.indexType)
        && Objects.equals(timeIndexInterval, other.timeIndexInterval)
        && Objects.equals(snapshot, other.snapshot)
        && Objects.equals(writeTimeIndexing, other.writeTimeIndexing);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        biosVersion,
        dimensions,
        attributes,
        dataSketches,
        featureInterval,
        alerts,
        featureAsContextName,
        lastN,
        lastNItems,
        lastNTtl,
        aggregated,
        indexed,
        indexType,
        timeIndexInterval,
        snapshot,
        writeTimeIndexing);
  }
}
