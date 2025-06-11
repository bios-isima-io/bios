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
import io.isima.bios.models.ChartType;
import java.util.List;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
public class SeriesConfig {
  @NotNull private String seriesId;

  private String name;

  @NotNull private List<String> dimensions;

  @NotNull private String metric;

  @NotNull private String signalId;

  @Valid ChartTypes chartTypes;

  public String getSeriesId() {
    return seriesId;
  }

  public void setSeriesId(String seriesId) {
    this.seriesId = seriesId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<String> dimensions) {
    this.dimensions = dimensions;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getSignalId() {
    return signalId;
  }

  public void setSignalId(String signalId) {
    this.signalId = signalId;
  }

  public ChartTypes getChartTypes() {
    return chartTypes;
  }

  public void setChartTypes(ChartTypes chartTypes) {
    this.chartTypes = chartTypes;
  }

  public static class ChartTypes {
    @NotNull private List<ChartType> allSupported;

    @NotNull private List<ChartType> availableInReport;

    @NotNull private ChartType currentlySelected;

    public List<ChartType> getAllSupported() {
      return allSupported;
    }

    public void setAllSupported(List<ChartType> allSupported) {
      this.allSupported = allSupported;
    }

    public List<ChartType> getAvailableInReport() {
      return availableInReport;
    }

    public void setAvailableInReport(List<ChartType> availableInReport) {
      this.availableInReport = availableInReport;
    }

    public ChartType getCurrentlySelected() {
      return currentlySelected;
    }

    public void setCurrentlySelected(ChartType currentlySelected) {
      this.currentlySelected = currentlySelected;
    }
  }
}
