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
package io.isima.bios.models.isql;

import java.util.concurrent.TimeUnit;

/** Window specified in a query. */
public interface Window<W extends Window<W>> {
  /**
   * Type of window (e.g Tumbling, Sliding etc..)
   *
   * @return type of window.
   */
  WindowType getWindowType();

  W getWindow();

  /**
   * Sliding window specification.
   *
   * @param slideInterval Interval of each slide in specified time unit
   * @param numSlideIntervalsInWindow Total window size per window in multiples of slide interval
   * @param unit Time unit of interval
   * @return Sliding window specifier
   */
  static SlidingWindow.WindowFinalSpecifier sliding(
      long slideInterval, TimeUnit unit, int numSlideIntervalsInWindow) {
    return ISqlBuilderProvider.getBuilderProvider()
        .getSlidingWindow(unit.toMillis(slideInterval), numSlideIntervalsInWindow);
  }

  /**
   * Tumbling window specification.
   *
   * @param windowSize Size of each window in specified time unit
   * @return Tumbling window specifier
   */
  static TumblingWindow.WindowFinalSpecifier tumbling(long windowSize, TimeUnit unit) {
    return ISqlBuilderProvider.getBuilderProvider().getTumblingWindow(unit.toMillis(windowSize));
  }

  interface WindowFinalSpecifier<W> {}
}
