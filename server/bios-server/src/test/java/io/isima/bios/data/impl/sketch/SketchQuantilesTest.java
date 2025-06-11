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

public class SketchQuantilesTest extends TestCase {

  private static final double allowedError = 0.001;

  public void testSimpleSequence() {
    SketchQuantiles sketchLong =
        (SketchQuantiles)
            DataSketch.createSketch(DataSketchType.QUANTILES, InternalAttributeType.DOUBLE);
    SketchQuantiles sketchDouble =
        (SketchQuantiles)
            DataSketch.createSketch(DataSketchType.QUANTILES, InternalAttributeType.DOUBLE);

    final long totalValues = 100_000;
    final double factor = 100.0;

    for (long i = 0; i < totalValues; i++) {
      sketchLong.update(i);
      sketchDouble.update(i / factor);
    }

    for (final var column : SketchSummaryColumn.values()) {
      if (column.getDataSketchType() == DataSketchType.QUANTILES) {
        final double q1 = sketchLong.quantile(column.getPercentile());
        final double q2 = sketchDouble.quantile(column.getPercentile());
        assertTrue(
            (Math.abs(totalValues * column.getPercentile() / 100 - q1) / totalValues)
                < allowedError);
        assertTrue(
            (Math.abs(totalValues * column.getPercentile() / 100 / factor - q2)
                    * factor
                    / totalValues)
                < allowedError);
      }
    }
  }
}
