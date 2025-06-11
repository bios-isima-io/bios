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

/** Sliding window query. */
public interface SlidingWindow extends Window<SlidingWindow> {
  /**
   * Returns slide interval in millis.
   *
   * @return slide interval
   */
  long getSlideIntervalMillis();

  /**
   * Returns number of slides per window. Window size will be number of slides * number of window
   * slides.
   *
   * @return number of window slides
   */
  int getNumberOfWindowSlides();

  /** For future optional decorations on window. */
  interface WindowFinalSpecifier extends Window.WindowFinalSpecifier<SlidingWindow> {}
}
