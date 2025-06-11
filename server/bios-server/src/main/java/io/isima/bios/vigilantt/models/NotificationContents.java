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
package io.isima.bios.vigilantt.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import io.isima.bios.models.Event;
import io.isima.bios.utils.StringUtils;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Contents of notification sent for an alert. Includes the source event and other metadata - alert
 * name, condition, time etc.
 */
@Getter
@Setter
@ToString
@JsonInclude(Include.NON_NULL)
public class NotificationContents {
  private String domainName;
  private String tenantName;
  private String signalName;
  private String featureName;
  private String alertName;
  private String condition;
  private String windowStartTime;
  private String windowEndTime;
  private String windowLength;
  private Event event;
  private String timestamp;
  private Long timestampMillisSinceEpoch;
  private String user;
  private String password;

  public NotificationContents(Event event) {
    this.event = event;
    this.timestampMillisSinceEpoch = System.currentTimeMillis();
    this.timestamp =
        StringUtils.tsToReadableTime(
            this.timestampMillisSinceEpoch, "yyyy-MM-dd 'T' HH:mm:ss.SSS XXX");
  }

  public NotificationContents(Event event, long windowStartTime, long windowEndTime) {
    this(event);
    this.windowStartTime =
        StringUtils.tsToReadableTime(windowStartTime, "yyyy-MM-dd'T'HH:mm:ssXXX");
    this.windowEndTime = StringUtils.tsToReadableTime(windowEndTime, "yyyy-MM-dd'T'HH:mm:ssXXX");
    this.windowLength = StringUtils.shortReadableDuration(windowEndTime - windowStartTime);
  }
}
