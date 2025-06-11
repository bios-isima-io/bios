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

import java.util.Collection;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DataSketchDurationChoicesTest {

  @Parameters
  public static Collection<Object[]> data() {
    return List.of(
        new Object[][] {
          {
            0,
            Long.MAX_VALUE,
            "1/5/10/50/100/500 ms, 1/5/15/30 second, 1/5/15/30 minute, 1/3/6/12 hour, and 1 day"
          },
          {100, 6 * 3600 * 1000, "100/500 ms, 1/5/15/30 second, 1/5/15/30 minute, and 1/3/6 hour"},
          {60000, 15 * 60000, "1/5/15 minute"},
          {50, 15 * 60000, "50/100/500 ms, 1/5/15/30 second, and 1/5/15 minute"},
        });
  }

  private long minIntervalMillis;
  private long maxIntervalMillis;
  private String expected;

  public DataSketchDurationChoicesTest(
      long minIntervalMillis, long maxIntervalMillis, String expected) {
    this.minIntervalMillis = minIntervalMillis;
    this.maxIntervalMillis = maxIntervalMillis;
    this.expected = expected;
  }

  @Test
  public void durationChoicesTest() {
    assertEquals(
        expected,
        DataSketchDuration.supportedIntervalsString(minIntervalMillis, maxIntervalMillis));
  }
}
