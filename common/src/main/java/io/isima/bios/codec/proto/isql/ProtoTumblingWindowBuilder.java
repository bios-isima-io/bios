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

import io.isima.bios.models.isql.TumblingWindow;
import io.isima.bios.models.proto.DataProto;

/** Builds an independent protobuf Tumbling Window message. */
public class ProtoTumblingWindowBuilder implements TumblingWindow.WindowFinalSpecifier {
  private final DataProto.TumblingWindow.Builder tumblingWindowBuilder;
  private final BasicBuilderValidator validator;

  /** Protobuf window builder. */
  public ProtoTumblingWindowBuilder(BasicBuilderValidator validator, long windowSizeMillis) {
    this.validator = validator;
    this.tumblingWindowBuilder = DataProto.TumblingWindow.newBuilder();
    validator.validatePositive(windowSizeMillis, "tumblingWindow::windowSizeMillis");
    this.tumblingWindowBuilder.setWindowSizeMs(windowSizeMillis);
  }

  DataProto.TumblingWindow buildProto() {
    return tumblingWindowBuilder.build();
  }

  BasicBuilderValidator getValidator() {
    return validator;
  }
}
