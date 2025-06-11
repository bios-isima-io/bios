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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RollingWindowAverageListTest {
  private static final int rollingWindowSize = 5;
  private RollingWindowAverageList rollingWindowAverageList;
  private Random random;

  @Before
  public void setUp() {
    random = new Random();
    rollingWindowAverageList = new RollingWindowAverageList(rollingWindowSize);
  }

  @After
  public void tearDown() {}

  @Test
  public void testRollingWindowAverage() {
    rollingWindowAverageList.add(1.0);
    Assert.assertEquals(1.0, rollingWindowAverageList.getRollingWindowAverage(), 0.001);

    rollingWindowAverageList.add(2.0);
    Assert.assertEquals(1.5, rollingWindowAverageList.getRollingWindowAverage(), 0.001);

    rollingWindowAverageList.add(3.0);
    rollingWindowAverageList.add(4.0);
    rollingWindowAverageList.add(5.0);

    Assert.assertEquals(3.0, rollingWindowAverageList.getRollingWindowAverage(), 0.001);

    rollingWindowAverageList.add(6.0);
    Assert.assertEquals(5, rollingWindowAverageList.size());
    Assert.assertEquals(4.0, rollingWindowAverageList.getRollingWindowAverage(), 0.001);

    rollingWindowAverageList.add(7.0);
    Assert.assertEquals(5, rollingWindowAverageList.size());
    Assert.assertEquals(5, rollingWindowAverageList.getRollingWindowAverage(), 0.001);
  }

  @Test
  public void testRollingWindowAverageRandomStream() {
    List<Double> rollingWindowElements = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      double newEntryInStream = (double) random.nextInt(1000);
      if (rollingWindowElements.size() == rollingWindowSize) {
        rollingWindowElements.remove(0);
      }
      rollingWindowElements.add(newEntryInStream);
      rollingWindowAverageList.add(newEntryInStream);

      int randomValue = random.nextInt(10);
      if (randomValue == 0) {
        double expectedAverage = computeExpectedAverage(rollingWindowElements);
        double actualAverage = rollingWindowAverageList.getRollingWindowAverage();
        Assert.assertEquals(expectedAverage, actualAverage, 0.001);
      }
    }
  }

  private double computeExpectedAverage(List<Double> windowEntries) {
    double sum = 0;
    for (Double entry : windowEntries) {
      sum += entry;
    }
    return windowEntries.size() > 0 ? sum / windowEntries.size() : 0;
  }
}
