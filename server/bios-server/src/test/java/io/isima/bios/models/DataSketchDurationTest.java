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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import io.isima.bios.common.SharedProperties;
import io.isima.bios.exceptions.ApplicationException;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.Test;

public class DataSketchDurationTest {

  @Test
  public void validateFeatureInterval() throws ApplicationException {
    assertNull(DataSketchDurationHelper.validateFeatureInterval(1000L * 60));
    assertNull(DataSketchDurationHelper.validateFeatureInterval(1000L * 60 * 5));
    assertNull(DataSketchDurationHelper.validateFeatureInterval(1000L * 60 * 15));
    assertNull(DataSketchDurationHelper.validateFeatureInterval(1000L * 60 * 30));

    assertNotNull(DataSketchDurationHelper.validateFeatureInterval(1));
    assertNotNull(DataSketchDurationHelper.validateFeatureInterval(1000L * 60 * 60 * 24 * 1000));

    SharedProperties.clearCache();
    String existingProperty =
        SharedProperties.get(DataSketchDurationHelper.MIN_FEATURE_INTERVAL_MILLIS_KEY);
    try {
      SharedProperties.getInstance()
          .setProperty(DataSketchDurationHelper.MIN_FEATURE_INTERVAL_MILLIS_KEY, "5000");
      SharedProperties.clearCache();
      assertNull(DataSketchDurationHelper.validateFeatureInterval(5000));
      assertNotNull(DataSketchDurationHelper.validateFeatureInterval(1000));
    } finally {
      if (existingProperty != null) {
        SharedProperties.getInstance()
            .setProperty(
                DataSketchDurationHelper.MIN_FEATURE_INTERVAL_MILLIS_KEY, existingProperty);
      } else {
        SharedProperties.getInstance()
            .setProperty(DataSketchDurationHelper.MIN_FEATURE_INTERVAL_MILLIS_KEY, "");
      }
      SharedProperties.clearCache();
    }
  }

  @Test
  public void friendlyName() {
    assertEquals("1 second", DataSketchDuration.SECONDS_1.friendlyName());
    assertEquals("5 seconds", DataSketchDuration.SECONDS_5.friendlyName());
    assertEquals("15 seconds", DataSketchDuration.SECONDS_15.friendlyName());
    assertEquals("30 seconds", DataSketchDuration.SECONDS_30.friendlyName());

    assertEquals("1 day", DataSketchDuration.DAYS_1.friendlyName());
  }

  // The following test is here to help ensure that the valid values are not changed without due
  // consideration and adjustment of any other code or systems that depend on these current valid
  // durations.
  @Test
  public void isValidDataSketchDuration() {
    assertTrue(DataSketchDuration.isValidDataSketchDuration(1000));
    assertTrue(DataSketchDuration.isValidDataSketchDuration(1000 * 60));
    assertTrue(DataSketchDuration.isValidDataSketchDuration(1000 * 60 * 60));
    assertTrue(DataSketchDuration.isValidDataSketchDuration(1000 * 60 * 60 * 24));

    // Values that are not simple factors or multiples of 5 minutes should not be valid
    // data sketch durations.
    assertFalse(DataSketchDuration.isValidDataSketchDuration(900));
    assertFalse(DataSketchDuration.isValidDataSketchDuration(2300));
    assertFalse(DataSketchDuration.isValidDataSketchDuration(1000 * 7));
    assertFalse(DataSketchDuration.isValidDataSketchDuration(1000 * 45));
    assertFalse(DataSketchDuration.isValidDataSketchDuration(1000 * 60 * 8));
    assertFalse(DataSketchDuration.isValidDataSketchDuration(1000 * 60 * 41));
  }

  // The following 2 tests are here to ensure that existing proxies are not changed because these
  // values are depended on by existing data sketch tables in the database. Changing any of these
  // may require a migration of existing data sketch tables.
  @Test
  public void getProxy() {
    assertEquals(30, DataSketchDuration.SECONDS_1.getProxy());
    assertEquals(34, DataSketchDuration.SECONDS_5.getProxy());
    assertEquals(36, DataSketchDuration.SECONDS_15.getProxy());
    assertEquals(38, DataSketchDuration.SECONDS_30.getProxy());

    assertEquals(40, DataSketchDuration.MINUTES_1.getProxy());
    assertEquals(44, DataSketchDuration.MINUTES_5.getProxy());
    assertEquals(46, DataSketchDuration.MINUTES_15.getProxy());
    assertEquals(48, DataSketchDuration.MINUTES_30.getProxy());

    assertEquals(50, DataSketchDuration.HOURS_1.getProxy());
    assertEquals(53, DataSketchDuration.HOURS_3.getProxy());
    assertEquals(56, DataSketchDuration.HOURS_6.getProxy());
    assertEquals(58, DataSketchDuration.HOURS_12.getProxy());

    assertEquals(60, DataSketchDuration.DAYS_1.getProxy());
  }

  @Test
  public void getMilliseconds() {
    assertEquals(1000L, DataSketchDuration.SECONDS_1.getMilliseconds());
    assertEquals(1000L * 5, DataSketchDuration.SECONDS_5.getMilliseconds());
    assertEquals(1000L * 15, DataSketchDuration.SECONDS_15.getMilliseconds());
    assertEquals(1000L * 30, DataSketchDuration.SECONDS_30.getMilliseconds());

    assertEquals(1000L * 60, DataSketchDuration.MINUTES_1.getMilliseconds());
    assertEquals(1000L * 60 * 5, DataSketchDuration.MINUTES_5.getMilliseconds());
    assertEquals(1000L * 60 * 15, DataSketchDuration.MINUTES_15.getMilliseconds());
    assertEquals(1000L * 60 * 30, DataSketchDuration.MINUTES_30.getMilliseconds());

    assertEquals(1000L * 60 * 60, DataSketchDuration.HOURS_1.getMilliseconds());
    assertEquals(1000L * 60 * 60 * 3, DataSketchDuration.HOURS_3.getMilliseconds());
    assertEquals(1000L * 60 * 60 * 6, DataSketchDuration.HOURS_6.getMilliseconds());
    assertEquals(1000L * 60 * 60 * 12, DataSketchDuration.HOURS_12.getMilliseconds());

    assertEquals(1000L * 60 * 60 * 24, DataSketchDuration.DAYS_1.getMilliseconds());
  }

  @Test
  public void proxiesMustBeInSortedOrder() {
    SortedMap<Byte, Long> sortedDurations = new TreeMap<>();
    for (DataSketchDuration d : DataSketchDuration.values()) {
      assertNull(sortedDurations.get(d.getProxy()));
      sortedDurations.put(d.getProxy(), d.getMilliseconds());
    }
    byte currentProxy = 0;
    long currentDurationMillis = 0;
    for (final var entry : sortedDurations.entrySet()) {
      assertTrue(currentProxy < entry.getKey());
      assertTrue(currentDurationMillis < entry.getValue());
      currentProxy = entry.getKey();
      currentDurationMillis = entry.getValue();
    }
  }
}
