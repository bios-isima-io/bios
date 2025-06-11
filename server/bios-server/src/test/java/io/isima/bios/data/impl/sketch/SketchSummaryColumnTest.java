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

import static io.isima.bios.data.impl.sketch.SketchSummaryColumn.DISTINCT_COUNT_NUM_SLOTS;
import static io.isima.bios.data.impl.sketch.SketchSummaryColumn.DISTINCT_COUNT_SLOT_BEGIN;
import static io.isima.bios.data.impl.sketch.SketchSummaryColumn.QUANTILES_NUM_SLOTS;
import static io.isima.bios.data.impl.sketch.SketchSummaryColumn.QUANTILES_SLOT_BEGIN;
import static io.isima.bios.data.impl.sketch.SketchSummaryColumn.TOTAL_SLOTS;

import io.isima.bios.models.DataSketchType;
import junit.framework.TestCase;

public class SketchSummaryColumnTest extends TestCase {

  public void testSlots() {
    assertEquals(0, QUANTILES_SLOT_BEGIN);
    assertEquals(QUANTILES_NUM_SLOTS, DISTINCT_COUNT_SLOT_BEGIN);
    assertEquals(QUANTILES_NUM_SLOTS + DISTINCT_COUNT_NUM_SLOTS, TOTAL_SLOTS);
    verifySlots(QUANTILES_SLOT_BEGIN, QUANTILES_NUM_SLOTS, DataSketchType.QUANTILES);
    verifySlots(DISTINCT_COUNT_SLOT_BEGIN, DISTINCT_COUNT_NUM_SLOTS, DataSketchType.DISTINCT_COUNT);
  }

  // Verify that the slots are numbered correctly without gaps or repetitions.
  public void verifySlots(int beginSlot, int numSlotsExpected, DataSketchType sketchType) {
    int numSlotsFound = 0;
    Boolean[] slotsUsed = new Boolean[numSlotsExpected];
    for (final var column : SketchSummaryColumn.values()) {
      if (column.getDataSketchType() == sketchType) {
        numSlotsFound++;
        assertTrue(column.toString(), column.getSlot() >= beginSlot);
        assertTrue(column.toString(), column.getSlot() < beginSlot + numSlotsExpected);
        slotsUsed[column.getSlot() - beginSlot] = true;
      }
    }
    assertEquals(numSlotsFound, numSlotsExpected);
    for (int i = 0; i < numSlotsExpected; i++) {
      assertTrue(slotsUsed[i]);
    }
  }

  // Verify that the percentile numbers match the names.
  public void testGetPercentile() {
    for (final var column : SketchSummaryColumn.values()) {
      if (column.getDataSketchType() == DataSketchType.QUANTILES) {
        final String percentileString = column.name().substring(1).replace('_', '.');
        assertEquals(
            column.toString(), column.getPercentile(), Double.parseDouble(percentileString));
      }
    }
  }
}
