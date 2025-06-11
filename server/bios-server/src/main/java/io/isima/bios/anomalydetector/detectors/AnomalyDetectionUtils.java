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
package io.isima.bios.anomalydetector.detectors;

import io.isima.bios.common.TfosConfig;
import io.isima.bios.models.Event;
import io.isima.bios.models.v1.AlertNotification;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Utility class for operations needed by anomaly detector. */
public class AnomalyDetectionUtils {
  private static final double anomalyThreshold = TfosConfig.getAnomalyDetectionThreshold();
  public static List<String> violatedRules =
      Collections.singletonList("Rolling Window Average Rule");

  public static boolean isOutSidePermissibleLimits(Double averageValue, Double incomingValue) {
    double permissibleDelta = anomalyThreshold * averageValue;
    return Math.abs(incomingValue - averageValue) > permissibleDelta;
  }

  public static Double convertToDouble(Object value) {
    Double result = null;
    try {
      result = Double.valueOf(value.toString());
    } catch (NumberFormatException e) {
      // Nothing to do. Anomaly detection logic will not be run for this attribute
    }
    return result;
  }

  public static String getKeyPrefix(AlertNotification alertNotification) {
    StringBuilder sb = new StringBuilder();
    sb.append(alertNotification.getRollupName());
    List<String> groupByAttributes = alertNotification.getViewDimensions();
    Event eventJson = alertNotification.getEvent();
    Map<String, Object> attributes = eventJson.getAttributes();
    if (groupByAttributes != null) {
      for (String groupByAttribute : groupByAttributes) {
        String attributeValueAsString = attributes.get(groupByAttribute).toString();
        sb.append("_");
        sb.append(attributeValueAsString);
      }
    }
    sb.append("_");
    return sb.toString();
  }
}
