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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class ReportConfig {

  private String reportId;

  private Metadata metadata = new Metadata();

  private List<SeriesConfig> series = Collections.emptyList();

  public String getReportId() {
    return reportId;
  }

  public void setReportId(String reportId) {
    this.reportId = reportId;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public void setMetadata(Metadata metadata) {
    this.metadata = metadata;
  }

  public List<SeriesConfig> getSeries() {
    return series;
  }

  public void setSeries(List<SeriesConfig> series) {
    this.series = series;
  }

  public void addSeries(SeriesConfig series) {
    if (this.series == null || this.series.isEmpty()) {
      this.series = new ArrayList<>();
    }
    this.series.add(series);
  }

  public static class Metadata {
    private String xaxis;

    private List<String> yaxes = Collections.emptyList();

    Long timeRange;

    @JsonProperty("X")
    public String getX() {
      return xaxis;
    }

    @JsonProperty("X")
    public void setX(String xaxis) {
      this.xaxis = xaxis;
    }

    @JsonProperty("Y")
    public List<String> getY() {
      return yaxes;
    }

    @JsonProperty("Y")
    public void setY(List<String> yaxis) {
      this.yaxes = yaxis;
    }

    public void addY(String yaxis) {
      if (this.yaxes.isEmpty()) {
        this.yaxes = new ArrayList<>();
      }
      this.yaxes.add(yaxis);
    }

    public Long getTimeRange() {
      return timeRange;
    }

    public void setTimeRange(Long timeRange) {
      this.timeRange = timeRange;
    }
  }
}
