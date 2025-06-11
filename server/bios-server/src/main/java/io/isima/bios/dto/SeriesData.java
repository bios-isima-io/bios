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
package io.isima.bios.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class SeriesData {
  private String seriesId;

  private Double ymin;

  private Double ymax;

  private List<List<Object>> data;

  public String getSeriesId() {
    return seriesId;
  }

  public void setSeriesId(String seriesId) {
    this.seriesId = seriesId;
  }

  @JsonProperty("yMin")
  public Double getYmin() {
    return ymin;
  }

  @JsonProperty("yMin")
  public void setYmin(Double ymin) {
    this.ymin = ymin;
  }

  @JsonProperty("yMax")
  public Double getYmax() {
    return ymax;
  }

  @JsonProperty("yMax")
  public void setYmax(Double ymax) {
    this.ymax = ymax;
  }

  public List<List<Object>> getData() {
    return data;
  }

  public void setData(List<List<Object>> data) {
    this.data = data;
  }
}
