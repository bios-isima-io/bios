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
import lombok.ToString;

/** Measurement to keep track of the maximum value of an attribute of records. */
@ToString(callSuper = true)
public class MaxMeasurement extends Measurement {

  @SuppressWarnings("rawtypes")
  Comparable maximum;

  /**
   * The constructor.
   *
   * @param name Name of the measurement
   * @param attribute Name of the target attribute
   * @param type Type of the target attribute
   */
  public MaxMeasurement(String name, String attribute, AttributeType type) {
    super(name, attribute, type);
  }

  @SuppressWarnings({"rawtypes"})
  @Override
  public void post(Record record) {
    assert record != null;
    Comparable newValue =
        (Comparable) attributeType.attributeAsObject(record.getAttribute(attributeName));
    if (newValue == null) {
      return;
    }
    tryReplace(newValue);
  }

  @Override
  public AttributeValue get() {
    return maximum != null ? new AttributeValueGeneric(maximum, attributeType) : null;
  }

  @Override
  public void merge(Measurement merging) {
    if (merging instanceof MaxMeasurement) {
      final var newValue = ((MaxMeasurement) merging).maximum;
      if (newValue != null) {
        tryReplace(newValue);
      }
    } else {
      throw new IllegalArgumentException(
          "Measurement type unmatch: " + merging.getClass().getSimpleName());
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void tryReplace(Comparable newValue) {
    if (maximum == null) {
      maximum = newValue;
    } else if (newValue.compareTo(maximum) > 0) {
      maximum = newValue;
    }
  }
}
