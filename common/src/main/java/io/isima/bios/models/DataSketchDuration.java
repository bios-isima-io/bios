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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

/** The list of allowed durations (time window lengths) for data sketches. */
@Getter
public enum DataSketchDuration {
  // First digit of proxy indicates unit - 2: millisecond, 3:second, 4:minute, 5:hour, 6:day.
  // Second digit of proxy indicates an approximate "logarithmic distance" between the current unit
  // and the next higher unit, scaled to fit between 0 and 9.
  MILLISECONDS_1((byte) 20, 1L),
  MILLISECONDS_5((byte) 22, 5L),
  MILLISECONDS_10((byte) 23, 10L),
  MILLISECONDS_50((byte) 25, 50L),
  MILLISECONDS_100((byte) 26, 100L),
  MILLISECONDS_500((byte) 28, 500L),

  SECONDS_1((byte) 30, 1000L),
  SECONDS_5((byte) 34, 1000L * 5),
  SECONDS_15((byte) 36, 1000L * 15),
  SECONDS_30((byte) 38, 1000L * 30),

  MINUTES_1((byte) 40, 1000L * 60),
  MINUTES_5((byte) 44, 1000L * 60 * 5),
  MINUTES_15((byte) 46, 1000L * 60 * 15),
  MINUTES_30((byte) 48, 1000L * 60 * 30),

  HOURS_1((byte) 50, 1000L * 60 * 60),
  HOURS_3((byte) 53, 1000L * 60 * 60 * 3),
  HOURS_6((byte) 56, 1000L * 60 * 60 * 6),
  HOURS_12((byte) 58, 1000L * 60 * 60 * 12),

  DAYS_1((byte) 60, 1000L * 60 * 60 * 24),

  ALL_TIME((byte) 127, Long.MAX_VALUE);

  private final byte proxy;
  private final long milliseconds;

  private static final Map<Byte, DataSketchDuration> proxyToDurationMap =
      createProxyToDurationMap();
  private static final Map<Long, DataSketchDuration> millisecondsToDurationMap =
      createMillisecondsToDurationMap();

  DataSketchDuration(byte proxy, long milliseconds) {
    this.proxy = proxy;
    this.milliseconds = milliseconds;
  }

  private static Map<Byte, DataSketchDuration> createProxyToDurationMap() {
    final var map = new HashMap<Byte, DataSketchDuration>();

    for (DataSketchDuration duration : values()) {
      map.put(duration.proxy, duration);
    }

    return map;
  }

  private static Map<Long, DataSketchDuration> createMillisecondsToDurationMap() {
    final var map = new HashMap<Long, DataSketchDuration>();

    for (DataSketchDuration duration : values()) {
      map.put(duration.milliseconds, duration);
    }

    return map;
  }

  /**
   * Resolves DataSketchDuration entry from a proxy.
   *
   * @param proxy DataSketch duration proxy
   * @return Resolved entry
   * @throws IllegalArgumentException if the specified proxy does not exist
   */
  public static DataSketchDuration fromProxy(byte proxy) {
    final var duration = proxyToDurationMap.get(proxy);
    if (duration == null) {
      throw new IllegalArgumentException("Proxy " + proxy + " does not exist");
    }
    return duration;
  }

  /**
   * Resolves DataSketchDuration entry from a duration in milliseconds.
   *
   * @param milliseconds Duration in milliseconds
   * @return Resolved entry
   * @throws IllegalArgumentException thrown to indicate that an unsupported interval is specified
   */
  public static DataSketchDuration fromMillis(long milliseconds) {
    final var duration = millisecondsToDurationMap.get(milliseconds);
    if (duration == null) {
      throw new IllegalArgumentException(
          String.format(
              "Interval %d is not supported. Supported intervals are %s",
              milliseconds, supportedIntervalsString(0, Long.MAX_VALUE)));
    }
    return duration;
  }

  public static boolean isValidDataSketchDuration(long milliseconds) {
    return millisecondsToDurationMap.containsKey(milliseconds);
  }

  /**
   * Provides a string that gives choices of possible durations.
   *
   * @param minDurationMillis Minimum duration milliseconds, inclusive.
   * @param maxDurationMillis Maximum duration milliseconds, inclusive.
   */
  public static String supportedIntervalsString(long minDurationMillis, long maxDurationMillis) {
    // final var sb = new StringBuilder();
    final var units = new String[] {" ms", " second", " minute", " hour", " day"};
    final var unitBoundaries =
        new long[] {1000, 60 * 1000, 60 * 60 * 1000, 24 * 60 * 60 * 1000, Long.MAX_VALUE};

    var choicesPerUnit = new ArrayList<String>();

    long denominator = 1;
    String delimiter = "";
    var sb = new StringBuilder();
    int unitIndex = 0;
    for (var entry : values()) {
      final var millis = entry.getMilliseconds();
      if (millis >= unitBoundaries[unitIndex] || millis > maxDurationMillis) {
        if (!delimiter.isEmpty()) {
          sb.append(units[unitIndex]);
          choicesPerUnit.add(sb.toString());
          delimiter = "";
          sb = new StringBuilder();
        }
        denominator = unitBoundaries[unitIndex];
        ++unitIndex;
        if (millis > maxDurationMillis) {
          break;
        }
      }
      if (millis < minDurationMillis) {
        continue;
      }
      sb.append(delimiter).append(millis / denominator);
      delimiter = "/";
    }

    sb = new StringBuilder();
    delimiter = "";
    for (int i = 0; i < choicesPerUnit.size(); ++i) {
      sb.append(delimiter).append(choicesPerUnit.get(i));
      delimiter = i < choicesPerUnit.size() - 2 ? ", " : ", and ";
    }
    return sb.toString();
  }

  public String friendlyName() {
    if (this == ALL_TIME) {
      return "All time";
    }
    final String name = this.name();
    final StringBuilder sb = new StringBuilder();
    final String[] parts = name.split("_");
    sb.append(parts[1]).append(" ");
    if (Long.parseLong(parts[1]) > 1) {
      sb.append(parts[0].toLowerCase());
    } else {
      // Remove the last "s" for the singular form of the word.
      sb.append(StringUtils.chop(parts[0].toLowerCase()));
    }
    return sb.toString();
  }

  private static final EnumStringifier<DataSketchDuration> stringifier =
      new EnumStringifier<>(values());

  @JsonCreator
  static DataSketchDuration forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  public String stringify() {
    return stringifier.stringify(this);
  }

  @Override
  public String toString() {
    return name();
  }
}
