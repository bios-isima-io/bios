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
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.Record;

/** Measurement to accumulate addition of an attribute of records. */
public class SumMeasurement extends Measurement {

  long intValue;
  double decimalValue;
  boolean empty;

  /**
   * The constructor.
   *
   * @param name Name of the measurement
   * @param attribute Name of the target attribute
   * @param type Type of the target attribute
   */
  public SumMeasurement(String name, String attribute, AttributeType type) {
    super(name, attribute, type);
    assert attributeType == AttributeType.INTEGER || attributeType == AttributeType.DECIMAL;
    intValue = 0;
    decimalValue = 0.0;
    empty = true;
  }

  @Override
  public void post(Record record) {
    assert record != null;
    final var attributeValue = record.getAttribute(attributeName);
    if (attributeType == AttributeType.INTEGER) {
      intValue += attributeValue.asLong();
    } else {
      decimalValue += attributeValue.asDouble();
    }
    empty = false;
  }

  @Override
  public AttributeValue get() {
    if (empty) {
      return null;
    }
    return attributeType == AttributeType.INTEGER
        ? new AttributeValueGeneric(intValue, attributeType)
        : new AttributeValueGeneric(decimalValue, attributeType);
  }

  @Override
  public String toString() {
    if (empty) {
      return "";
    } else if (attributeType == AttributeType.INTEGER) {
      return Long.toString(intValue);
    }
    return Double.toString(decimalValue);
  }

  @Override
  public void merge(Measurement mergingOrig) {
    if (mergingOrig instanceof SumMeasurement) {
      final var merging = (SumMeasurement) mergingOrig;
      if (!merging.empty) {
        intValue += merging.intValue;
        decimalValue += merging.decimalValue;
        empty = false;
      }
    } else {
      throw new IllegalArgumentException(
          "Measurement type unmatch: " + mergingOrig.getClass().getSimpleName());
    }
  }
}
