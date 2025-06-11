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
import io.isima.bios.models.v1.InternalAttributeType;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/** Immutable specification used for a data sketch process. */
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@ToString(callSuper = true)
public class DataSketchSpecifier extends DigestSpecifierBase {
  private final DataSketchDuration dataSketchDuration;
  private final short attributeProxy;
  private final DataSketchType dataSketchType;
  private String attributeName;
  private InternalAttributeType attributeType;

  public DataSketchSpecifier(
      DataSketchDuration dataSketchDuration,
      short attributeProxy,
      DataSketchType dataSketchType,
      long startTime,
      long endTime,
      long doneSince,
      long doneUntil) {
    super(startTime, endTime, doneSince, doneUntil);
    if (dataSketchDuration == null || dataSketchType == null) {
      throw new IllegalArgumentException("dataSketchDuration and dataSketchType may not be null");
    }
    this.dataSketchDuration = dataSketchDuration;
    this.attributeProxy = attributeProxy;
    this.dataSketchType = dataSketchType;
  }
}
