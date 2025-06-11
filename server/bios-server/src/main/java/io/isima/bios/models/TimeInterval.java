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
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@Setter
@ToString
@JsonIgnoreProperties(ignoreUnknown = true)
public class TimeInterval extends Duplicatable {

  @NotNull private int value;

  // TODO(TFOS-1080): Eliminate TimeunitType and use java.util.concurrent.TimeUnit.
  @NotNull private TimeunitType timeunit;

  public TimeInterval() {}

  @JsonIgnore
  public int getValueInMillis() {
    switch (timeunit) {
      case MILLISECOND:
        return value;
      case SECOND:
        return value * 1000;
      case MINUTE:
        return value * 1000 * 60;
      case HOUR:
        return value * 1000 * 60 * 60;
      case DAY:
        return value * 1000 * 60 * 60 * 24;
      default:
        throw new RuntimeException("invalid interval value");
    }
  }

  @Override
  public TimeInterval duplicate() {
    TimeInterval clone = new TimeInterval();
    clone.value = value;
    clone.timeunit = timeunit;
    return clone;
  }
}
