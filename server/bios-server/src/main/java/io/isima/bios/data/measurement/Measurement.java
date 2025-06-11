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
package io.isima.bios.data.measurement;

import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValue;
import io.isima.bios.models.Record;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public abstract class Measurement {

  protected final String name;
  protected final String attributeName;
  protected final AttributeType attributeType;

  protected Measurement(String name, String attributeName, AttributeType attributeType) {
    this.name = name;
    this.attributeName = attributeName;
    this.attributeType = attributeType;
  }

  /** Method to post a record to the measurement. */
  public abstract void post(Record record);

  /**
   * Retrieves current measurement value.
   *
   * @return Current measurement value as an AttributeValue. The value type of the attribute is
   *     available via {@link #getAttributeType()}. If no records have been posted, the method
   *     returns null.
   */
  public abstract AttributeValue get();

  public abstract void merge(Measurement merging);
}
