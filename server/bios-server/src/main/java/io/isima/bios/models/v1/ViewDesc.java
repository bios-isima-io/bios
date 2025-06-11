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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.Duplicatable;
import io.isima.bios.models.IndexType;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class ViewDesc extends Duplicatable {

  @NotNull private String name;

  @NotNull @Valid private List<String> groupBy;

  @NotNull @Valid private List<String> attributes;

  private List<DataSketchType> dataSketches;

  /**
   * Table version for the stream. The table version may be different from stream version In case,
   * e.g., only view and post-process has modified.
   */
  protected Long schemaVersion;

  protected String featureAsContextName;

  protected Boolean lastN;

  protected Long lastNItems;

  protected Long lastNTtl;

  protected Boolean indexTableEnabled;

  protected IndexType indexType;

  protected Long timeIndexInterval;

  protected Boolean snapshot;

  protected Boolean writeTimeIndexing;

  public String getEffectiveFeatureAsContextName(String streamName) {
    if (featureAsContextName != null) {
      return featureAsContextName;
    }
    return streamName + "_" + name;
  }

  public ViewDesc() {}

  @Override
  public ViewDesc duplicate() {
    ViewDesc clone = new ViewDesc();
    clone.name = name;
    clone.schemaVersion = schemaVersion;
    clone.groupBy = (groupBy != null) ? new ArrayList<>(groupBy) : null;
    clone.attributes = (attributes != null) ? new ArrayList<>(attributes) : null;
    clone.dataSketches = (dataSketches != null) ? new ArrayList<>(dataSketches) : null;
    clone.featureAsContextName = featureAsContextName;
    clone.lastN = lastN;
    clone.lastNItems = lastNItems;
    clone.lastNTtl = lastNTtl;
    clone.indexTableEnabled = indexTableEnabled;
    clone.indexType = indexType;
    clone.timeIndexInterval = timeIndexInterval;
    clone.snapshot = snapshot;
    clone.writeTimeIndexing = writeTimeIndexing;
    return clone;
  }

  public boolean hasGenericDigestion() {
    return lastN == Boolean.TRUE
        || (dataSketches != null && dataSketches.contains(DataSketchType.LAST_N));
  }

  public FeatureDesc toFeatureConfig() {
    final var feature = new FeatureDesc();

    feature.setName(this.getName());
    feature.setDimensions(this.getGroupBy());
    feature.setAttributes(this.getAttributes());
    feature.setDataSketches(this.getDataSketches());
    feature.setIndexed(this.getIndexTableEnabled());
    feature.setIndexType(this.getIndexType());
    feature.setTimeIndexInterval(this.getTimeIndexInterval());
    feature.setFeatureAsContextName(this.getFeatureAsContextName());
    feature.setLastN(this.getLastN());
    feature.setLastNItems(this.getLastNItems());
    feature.setLastNTtl(this.getLastNTtl());
    feature.setSnapshot(this.getSnapshot());
    feature.setWriteTimeIndexing(this.writeTimeIndexing);

    return feature;
  }
}
