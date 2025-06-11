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

import io.isima.bios.common.SharedProperties;

public class DataSketchDurationHelper {
  public static final String MIN_FEATURE_INTERVAL_MILLIS_KEY = "prop.minFeatureIntervalMillis";
  static final String MAX_FEATURE_INTERVAL_MILLIS_KEY = "prop.maxFeatureIntervalMillis";
  private static final long MIN_FEATURE_INTERVAL_MILLIS_DEFAULT = 50L;
  private static final long MAX_FEATURE_INTERVAL_MILLIS_DEFAULT = 30L * 60 * 1000;

  /** Validates Feature interval and returns an error message if invalid. */
  public static String validateFeatureInterval(long milliseconds) {
    final Long minFeatureInterval =
        SharedProperties.getCached(
            MIN_FEATURE_INTERVAL_MILLIS_KEY, MIN_FEATURE_INTERVAL_MILLIS_DEFAULT);
    final Long maxFeatureInterval =
        SharedProperties.getCached(
            MAX_FEATURE_INTERVAL_MILLIS_KEY, MAX_FEATURE_INTERVAL_MILLIS_DEFAULT);
    if (milliseconds < minFeatureInterval
        || milliseconds > maxFeatureInterval
        || !DataSketchDuration.isValidDataSketchDuration(milliseconds)) {
      return "featureInterval must be chosen from among: "
          + DataSketchDuration.supportedIntervalsString(minFeatureInterval, maxFeatureInterval);
    }
    return null;
  }
}
