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

import io.isima.bios.models.Event;
import io.isima.bios.models.v1.AlertNotification;
import io.isima.bios.vigilantt.classifiers.ClassificationInfo;
import io.isima.bios.vigilantt.classifiers.EventClassifier;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Anomaly detector implementation of event classifier. Implementation maintains a rolling average
 * of attributes and classifies a new event as anomaly if any attribute value falls outside expected
 * threshold
 */
public class AnomalyDetector implements EventClassifier {

  private static final Logger logger = LoggerFactory.getLogger(AnomalyDetector.class);
  private final Map<String, RollingWindowAverageList> attributeMovingAverageMap;
  private final int windowLength;

  public AnomalyDetector(int windowLength) {
    attributeMovingAverageMap = new ConcurrentHashMap<>();
    this.windowLength = windowLength;
  }

  @Override
  public ClassificationInfo classify(Event anomalyDetectionEvent) {
    ClassificationInfo classificationInfo = new ClassificationInfo(false);
    AlertNotification alertNotification = (AlertNotification) anomalyDetectionEvent;
    logger.debug("Running AnomalyDetector for notification {}", alertNotification);
    String attributeKeyPrefix = AnomalyDetectionUtils.getKeyPrefix(alertNotification);
    Event event = alertNotification.getEvent();
    Map<String, Object> attributes = event.getAttributes();
    for (String attributeName : attributes.keySet()) {
      String fullyQualifiedAttributeName = attributeKeyPrefix + attributeName;
      Double incomingValue = AnomalyDetectionUtils.convertToDouble(attributes.get(attributeName));
      if (incomingValue != null) {
        if (attributeMovingAverageMap.containsKey(fullyQualifiedAttributeName)) {
          RollingWindowAverageList rollingWindowAverageListForAttribute =
              attributeMovingAverageMap.get(fullyQualifiedAttributeName);
          rollingWindowAverageListForAttribute.add(incomingValue);
          double currentAverage = rollingWindowAverageListForAttribute.getRollingWindowAverage();
          if (rollingWindowAverageListForAttribute.size() >= windowLength
              && AnomalyDetectionUtils.isOutSidePermissibleLimits(currentAverage, incomingValue)) {
            classificationInfo.setIsAnomaly(true);
            classificationInfo.setViolatedRules(AnomalyDetectionUtils.violatedRules);
            return classificationInfo;
          }
        } else {
          RollingWindowAverageList rollingWindowAverageListForAttribute =
              new RollingWindowAverageList(windowLength);
          rollingWindowAverageListForAttribute.add(incomingValue);
          logger.debug(
              "No entry exists for key " + fullyQualifiedAttributeName + "Creating the entry");
          attributeMovingAverageMap.put(
              fullyQualifiedAttributeName, rollingWindowAverageListForAttribute);
        }
      }
    }
    return classificationInfo;
  }

  private void printMovingAverageMap() {
    StringBuilder sb = new StringBuilder();
    for (String key : attributeMovingAverageMap.keySet()) {
      sb.append(
          "(Key => "
              + key
              + ",Average => "
              + attributeMovingAverageMap.get(key).getRollingWindowAverage()
              + "),");
    }
    logger.debug(sb.toString());
  }
}
