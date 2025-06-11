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

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/** Synopsis of a signal or a context. */
@Getter
@Setter
@ToString
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ContextSynopsis {
  private String contextName;
  private Long lastUpdated;
  private Long count;
  private TrendLineData trendLineData;
  private long dailyChanges;
  private List<DailyOperation> dailyOperations;

  private Long durationSecs;
  private Long timeWindowSizeSecs;
  private List<Long> startTime;

  private List<AttributeSynopsis> attributes;

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class TrendLineData {
    private List<Long> count;
  }

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  public static class DailyOperation {
    private String operation;
    private Long count;
  }
}
