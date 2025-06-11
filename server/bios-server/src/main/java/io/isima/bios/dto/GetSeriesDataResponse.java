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

import java.util.List;

public class GetSeriesDataResponse {

  private Long xmin;

  private Long xmax;

  List<SeriesData> series;

  public Long getXmin() {
    return xmin;
  }

  public void setXmin(Long xmin) {
    this.xmin = xmin;
  }

  public Long getXmax() {
    return xmax;
  }

  public void setXmax(Long xmax) {
    this.xmax = xmax;
  }

  public List<SeriesData> getSeries() {
    return series;
  }

  public void setSeries(List<SeriesData> series) {
    this.series = series;
  }
}
