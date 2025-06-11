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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.isima.bios.models.v1.Aggregate;
import io.isima.bios.models.v1.validators.ValidExtractRequest;
import java.util.List;
import javax.validation.Valid;

/** POJO to bring ingest request. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@ValidExtractRequest
public class ExtractRequest implements io.isima.bios.req.ExtractRequest {

  @JsonProperty("startTime")
  private Long startTime;

  @JsonProperty("endTime")
  private Long endTime;

  @JsonProperty("attributes")
  private List<String> attributes;

  @JsonProperty("view")
  private View view;

  @JsonIgnore private List<io.isima.bios.req.View> views;

  @Valid
  @JsonProperty("aggregates")
  private List<Aggregate> aggregates;

  @JsonProperty("streamVersion")
  private Long streamVersion;

  @JsonProperty("filter")
  private String filter;

  private Boolean onTheFly;

  @Override
  public Long getStartTime() {
    return startTime;
  }

  public ExtractRequest setStartTime(final Long startTime) {
    this.startTime = startTime;
    return this;
  }

  @Override
  public Long getEndTime() {
    return endTime;
  }

  public ExtractRequest setEndTime(final Long endTime) {
    this.endTime = endTime;
    return this;
  }

  @Override
  public List<String> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<String> attributes) {
    this.attributes = attributes;
  }

  @Override
  public List<Aggregate> getAggregates() {
    return aggregates;
  }

  public void setAggregates(List<Aggregate> aggregates) {
    this.aggregates = aggregates;
  }

  public View getView() {
    return view;
  }

  public void setView(View view) {
    this.view = view;
  }

  @Override
  public Long getStreamVersion() {
    return streamVersion;
  }

  public void setStreamVersion(Long streamVersion) {
    this.streamVersion = streamVersion;
  }

  @Override
  public String getFilter() {
    return filter;
  }

  public void setFilter(final String filter) {
    this.filter = filter;
  }

  public void setViews(List<io.isima.bios.req.View> views) {
    this.views = views;
  }

  @Override
  public List<io.isima.bios.req.View> getViews() {
    if (views != null) {
      return views;
    } else {
      return view != null ? List.of(view) : List.of();
    }
  }

  @Override
  public Integer getLimit() {
    // We don't support this via TFOS API
    return null;
  }

  public void setOnTheFly(boolean b) {
    this.onTheFly = b;
  }

  @Override
  public boolean isOnTheFly() {
    return onTheFly != null && onTheFly;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    String delimiter = "";
    if (startTime != null) {
      sb.append(delimiter).append("startTime='").append(startTime);
      delimiter = ", ";
    }
    if (endTime != null) {
      sb.append(delimiter).append("endTime=").append(endTime);
      delimiter = ", ";
    }
    if (attributes != null) {
      final String separator = ", ";
      if (attributes != null) {
        sb.append(delimiter).append("attributes=[");
        delimiter = ", ";
        String delim = "";
        for (String attribute : attributes) {
          sb.append(delim).append(attribute);
          delim = separator;
        }
        sb.append("]");
      }
    }
    if (aggregates != null) {
      final String separator = "}, {";
      sb.append(delimiter).append("aggregates=[{");
      delimiter = ", ";
      String delim = "";
      for (Aggregate aggregate : aggregates) {
        sb.append(delim).append(aggregate);
        delim = separator;
      }
      sb.append("}]");
    }
    if (view != null) {
      sb.append(delimiter).append("view={").append(view).append("}");
      delimiter = ", ";
    }
    if (filter != null) {
      sb.append(delimiter).append("filter={").append(filter).append("}");
    }
    return sb.toString();
  }
}
