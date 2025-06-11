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
package io.isima.bios.data.impl.sketch;

import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.v1.InternalAttributeType;
import junit.framework.TestCase;

public class SketchMomentsTest extends TestCase {

  public void testUpdateDouble() {
    SketchMoments moments =
        (SketchMoments)
            DataSketch.createSketch(DataSketchType.MOMENTS, InternalAttributeType.DOUBLE);
    moments.update(0.23);
    moments.update(100.23);
    moments.update(300.23);
    moments.update(400.23);
    moments.update(500.23);

    assertEquals(0.23, moments.getMin());
    assertEquals(500.23, moments.getMax());
    assertEquals(5, moments.getCount());
    assertEquals(1301.15, moments.getSum());
    assertEquals(
        0.23 * 0.23 + 100.23 * 100.23 + 300.23 * 300.23 + 400.23 * 400.23 + 500.23 * 500.23,
        moments.getSum2());
    assertTrue(moments.getSum3() > 500.0 * 500 * 500);
    assertTrue(moments.getSum3() < 500.0 * 500 * 500 * 2);
    assertTrue(moments.getSum4() > 500.0 * 500 * 500 * 500);
    assertTrue(moments.getSum4() < 500.0 * 500 * 500 * 500 * 2);
  }

  public void testUpdateLong() {
    SketchMoments moments =
        (SketchMoments) DataSketch.createSketch(DataSketchType.MOMENTS, InternalAttributeType.LONG);
    moments.update(-100L);
    moments.update(0L);
    moments.update(400L);
    moments.update(500L);

    assertEquals(-100.0, moments.getMin());
    assertEquals(500.0, moments.getMax());
    assertEquals(4, moments.getCount());
    assertEquals(800.0, moments.getSum());
    assertEquals(100.0 * 100 + 400 * 400 + 500 * 500, moments.getSum2());
    assertTrue(moments.getSum3() > 500.0 * 500 * 500);
    assertTrue(moments.getSum3() < 500.0 * 500 * 500 * 2);
    assertTrue(moments.getSum4() > 500.0 * 500 * 500 * 500);
    assertTrue(moments.getSum4() < 500.0 * 500 * 500 * 500 * 2);
  }
}
