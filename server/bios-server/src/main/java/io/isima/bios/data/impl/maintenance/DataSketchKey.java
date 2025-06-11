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
package io.isima.bios.data.impl.maintenance;

import io.isima.bios.models.DataSketchDuration;
import io.isima.bios.models.DataSketchType;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Immutable key used to identify data sketch, specifically for use in a map containing
 * DataSketchSpecifier objects.
 */
@AllArgsConstructor
@EqualsAndHashCode
@Getter
@ToString
public class DataSketchKey {
  private final DataSketchDuration dataSketchDuration;
  private final short attributeProxy;
  private final DataSketchType dataSketchType;

  public DataSketchKey(byte durationProxy, short attributeProxy, byte sketchTypeProxy) {
    this.dataSketchDuration = DataSketchDuration.fromProxy(durationProxy);
    this.attributeProxy = attributeProxy;
    this.dataSketchType = DataSketchType.fromProxy(sketchTypeProxy);
  }
}
