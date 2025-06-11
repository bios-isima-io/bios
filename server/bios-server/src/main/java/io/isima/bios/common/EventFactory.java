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
package io.isima.bios.common;

import io.isima.bios.models.Event;
import io.isima.bios.models.EventJson;

public interface EventFactory {
  public static EventFactory DEFAULT_FACTORY = new EventFactory() {};

  /**
   * Creates a new event.
   *
   * @return a concrete event object. Default is always JSON convertible object, for now
   */
  default Event create() {
    return new EventJson();
  }

  /**
   * Creates a new event, given an existing event.
   *
   * @param event existing event
   * @return a new event that copies the contents of existing event
   */
  default Event create(Event event) {
    return new EventJson(event);
  }

  /**
   * Sets the concurrent flag to indicate multiple threads may simultaneously operate on the same
   * schema.
   */
  default void setConcurrent() {}
}
