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
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

/**
 * This is a collection of sketch summary rows and/or sketch blobs for a single attribute. Each item
 * in the summaryMap contains data from a single row in the sketch summary table. It is implemented
 * as a sorted map with the row's endTime as the key, so that we can read rows in time sequence.
 *
 * <p>This needs to be thread-safe because multiple query responses may update this object
 * concurrently.
 */
@Getter
public class SingleAttributeSketches {
  private final ConcurrentSkipListMap<Long, SketchSummary> summaryMap =
      new ConcurrentSkipListMap<>();
  private final ConcurrentSkipListMap<DataSketchType, SingleTypeSketches> sketchTypesMap =
      new ConcurrentSkipListMap<>();

  @NoArgsConstructor
  @Getter
  public static class SingleTypeSketches {
    private final RawSketchMap rawSketchMap = new RawSketchMap();
    @Setter private boolean rawSketchMapProcessed = false;
    @Setter private DataSketch mergedSketch = null;
  }

  public static class RawSketchMap extends ConcurrentSkipListMap<Long, RawSketchContents> {}

  @RequiredArgsConstructor
  @Getter
  public static class RawSketchContents {
    private final long count;
    private final byte[] header;
    private final byte[] data;
  }
}
