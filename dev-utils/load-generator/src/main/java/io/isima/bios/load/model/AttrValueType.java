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
package io.isima.bios.load.model;

public class AttrValueType {

  private ValueType valType;
  private int minSize;
  private int maxSize;

  public AttrValueType(ValueType valType, int minSize, int maxSize) {
    this.valType = valType;
    this.minSize = minSize;
    this.maxSize = maxSize;
  }

  public AttrValueType(ValueType valType) {
    this.valType = valType;
    this.minSize = 0;
    this.maxSize = 0;
  }

  public Integer getMinSize() {
    return this.minSize;
  }

  public Integer getMaxSize() {
    return this.maxSize;
  }

  public ValueType getValType() {
    return this.valType;
  }

}
