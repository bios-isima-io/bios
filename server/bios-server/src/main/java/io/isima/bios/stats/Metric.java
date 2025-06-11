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
package io.isima.bios.stats;

public interface Metric<M extends Metric<M>> {
  /**
   * Checkpoint this metric. Once check pointed, the metric will be automatically cleared after
   * taking a deep copy of the metric. This deep copy will be returned to the caller.
   *
   * @return deep copy of the current metric before atomically clearing it
   */
  M checkPoint();
}
