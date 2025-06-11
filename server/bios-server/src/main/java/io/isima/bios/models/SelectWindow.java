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

import io.isima.bios.models.proto.DataProto;

/** Class that describes the window specification of a select request. */
public class SelectWindow {

  private final WindowType windowType;
  private final long stepSizeInMillis;
  private final long windowSizeInMillis;

  /**
   * The default constructor.
   *
   * <p>The default constructor creates an instance that describes a global window.
   */
  public SelectWindow() {
    windowType = WindowType.GLOBAL;
    stepSizeInMillis = 0;
    windowSizeInMillis = 0;
  }

  /** The constructor that creates an instance from a {@link DataProto.Window} object. */
  public SelectWindow(DataProto.Window windowProto) {
    windowType = WindowType.valueOf(windowProto.getWindowType());
    switch (windowType) {
      case GLOBAL:
        stepSizeInMillis = 0;
        windowSizeInMillis = 0;
        break;
      case TUMBLING:
        windowSizeInMillis = windowProto.getTumbling().getWindowSizeMs();
        stepSizeInMillis = windowSizeInMillis;
        break;
      case SLIDING:
        stepSizeInMillis = windowProto.getSliding().getSlideInterval();
        windowSizeInMillis = stepSizeInMillis * windowProto.getSliding().getWindowSlides();
        break;
      default:
        throw new UnsupportedOperationException("Unkonown window type: " + windowType);
    }
  }

  public WindowType getType() {
    return windowType;
  }

  public long getStepSizeInMillis() {
    return stepSizeInMillis;
  }

  public long getWindowSizeInMillis() {
    return windowSizeInMillis;
  }

  @Override
  public String toString() {
    final var sb =
        new StringBuilder("{type=")
            .append(windowType)
            .append(", step=")
            .append(stepSizeInMillis)
            .append(", windowSize=")
            .append(windowSizeInMillis);
    return sb.append("}").toString();
  }
}
