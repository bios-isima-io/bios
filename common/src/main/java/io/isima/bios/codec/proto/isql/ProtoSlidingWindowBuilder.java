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
package io.isima.bios.codec.proto.isql;

import io.isima.bios.models.isql.SlidingWindow;
import io.isima.bios.models.proto.DataProto;

/** Builds an independent protobuf Sliding Window message. */
public class ProtoSlidingWindowBuilder implements SlidingWindow.WindowFinalSpecifier {
  private final DataProto.SlidingWindow.Builder slidingWindowBuilder;
  private final BasicBuilderValidator validator;

  /** Protobuf window builder. */
  public ProtoSlidingWindowBuilder(
      BasicBuilderValidator validator, long slideInterval, int numSlidesPerWindow) {
    this.validator = validator;
    this.slidingWindowBuilder = DataProto.SlidingWindow.newBuilder();
    validator.validatePositive(slideInterval, "slidingwindow::slideInterval");
    validator.validatePositive(numSlidesPerWindow, "slidingwindow::numSlidesPerWindow");
    this.slidingWindowBuilder.setSlideInterval(slideInterval);
    this.slidingWindowBuilder.setWindowSlides(numSlidesPerWindow);
  }

  DataProto.SlidingWindow buildProto() {
    return slidingWindowBuilder.build();
  }

  BasicBuilderValidator getValidator() {
    return validator;
  }
}
