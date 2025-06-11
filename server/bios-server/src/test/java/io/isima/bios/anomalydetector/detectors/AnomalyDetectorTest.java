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

import io.isima.bios.models.AlertConfig;
import io.isima.bios.models.AlertType;
import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;
import io.isima.bios.models.v1.AlertNotification;
import io.isima.bios.vigilantt.classifiers.ClassificationInfo;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AnomalyDetectorTest {

  private AnomalyDetector anomalyDetector;
  private static final int windowLength = 5;
  private static final String attributeName = "item_price";
  private static final String rollupName = "test_rollup";
  private static final String anotherRollupName = "another_test_rollup";
  private static final String groupByAttribute = "product_id";
  private static final String groupByAttributeValue = "TV";
  private Random random;
  private AlertNotification alertNotification;

  /** Sets up commonly-used anomaly detector, random number generator, and example alert config. */
  @Before
  public void setUp() {
    anomalyDetector = new AnomalyDetector(windowLength);
    random = new Random();
    alertNotification = getSampleAlertNotification();
  }

  @After
  public void tearDown() {}

  @Test
  public void testEventNotClassifiedAsAnomalyIfNumberOfEventsAreLessThanWindowLength() {
    for (int i = 0; i < 4; i++) {
      double randomValue = random.nextInt(1000);
      updateAttributeValue(alertNotification, attributeName, randomValue);
      ClassificationInfo classificationInfo = anomalyDetector.classify(alertNotification);
      Assert.assertFalse(classificationInfo.getIsAnomaly());
    }
  }

  @Test
  public void testAnomalousEventsAreDetectedCorrectly() {
    for (int i = 0; i < 100; i++) {
      double randomStreamEntry = random.nextInt(100);
      boolean isAnomalousEvent = i > windowLength && i % 10 == 0;
      if (isAnomalousEvent) {
        randomStreamEntry += 500;
      }
      updateAttributeValue(alertNotification, attributeName, randomStreamEntry);

      ClassificationInfo classificationInfo = anomalyDetector.classify(alertNotification);

      if (isAnomalousEvent) {
        Assert.assertTrue(classificationInfo.getIsAnomaly());
      }
    }
  }

  @Test
  public void testRollingAverageFromRolledupAttributesOfSeparateRollupsAreIsolated() {
    ingestSampleEvents();

    // Verify that an anomalous event on the same rollup is detected correctly
    testTruePositive();

    // Change the rollup name and verify that another anomalous event is not qualified as anomaly
    alertNotification.setRollupName(anotherRollupName);
    double randomStreamEntry = (double) random.nextInt(1000) + 2000;
    updateAttributeValue(alertNotification, attributeName, randomStreamEntry);
    ClassificationInfo classificationInfo = anomalyDetector.classify(alertNotification);
    Assert.assertFalse(classificationInfo.getIsAnomaly());

    // Ingest sample events with the new rollup
    ingestSampleEvents();

    // Verify that an anomalous event on the new rollup is detected correctly
    testTruePositive();

    // Change the list of groupBy attributes in view on which rollup is defined
    List<String> groupByAttributes = Collections.singletonList(groupByAttribute);
    alertNotification.setViewDimensions(groupByAttributes);
    addValueForGroupByAttribute(alertNotification);

    // Verify that another anomalous event is not qualified as anomaly
    alertNotification.setRollupName(anotherRollupName);
    randomStreamEntry = (double) random.nextInt(1000) + 2000;
    updateAttributeValue(alertNotification, attributeName, randomStreamEntry);
    classificationInfo = anomalyDetector.classify(alertNotification);
    Assert.assertFalse(classificationInfo.getIsAnomaly());

    // Ingest sample events with new group by attributes
    ingestSampleEvents();

    // Verify that an anomalous event on the new rollup is detected correctly
    testTruePositive();
  }

  /**
   * Function to ingest some sample events for an attribute so that. anomalyDetector has enough
   * events to run its logic
   */
  private void ingestSampleEvents() {
    for (int i = 0; i < 20; i++) {
      double randomStreamEntry = random.nextInt(1000);
      updateAttributeValue(alertNotification, attributeName, randomStreamEntry);
      anomalyDetector.classify(alertNotification);
    }
  }

  /** Function to validate that anomalous events are detected correctly. */
  private void testTruePositive() {
    double randomStreamEntry = (double) random.nextInt(1000) + 2000;
    updateAttributeValue(alertNotification, attributeName, randomStreamEntry);
    ClassificationInfo classificationInfo = anomalyDetector.classify(alertNotification);
    Assert.assertTrue(classificationInfo.getIsAnomaly());
  }

  /**
   * Construct a sample AlertNotification object which is consumed by Anomaly Detector.
   *
   * @return AlertNotification
   */
  private AlertNotification getSampleAlertNotification() {
    EventJson event = new EventJson();
    event.setEventId(new UUID(1, 2));
    Map<String, Object> attributes = new HashMap<>();
    attributes.put(attributeName, 100);
    event.setAttributes(attributes);
    AlertConfig alert =
        new AlertConfig(
            "item_sold_at_high_price_alert",
            AlertType.JSON,
            "(max(item_price) > 80)",
            "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
            null,
            null);
    AlertNotification alertNotification = new AlertNotification(alert, event);
    alertNotification.setRollupName(rollupName);
    return alertNotification;
  }

  /** Update the attribute value in an AlertNotification. */
  private void updateAttributeValue(
      AlertNotification alertNotification, String attribute, double newValue) {
    Event event = alertNotification.getEvent();
    event.set(attribute, newValue);
  }

  /** Add value for a group by attribute in AlertNotification. */
  private void addValueForGroupByAttribute(AlertNotification alertNotification) {
    Event event = alertNotification.getEvent();
    event.set(groupByAttribute, groupByAttributeValue);
  }
}
