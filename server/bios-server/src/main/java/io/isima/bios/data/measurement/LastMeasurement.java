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

public class LastMeasurement extends Measurement {

  AttributeValue last;

  public LastMeasurement(String name, String attribute, AttributeType type) {
    super(name, attribute, type);
  }

  @Override
  public void post(Record record) {
    assert record != null;
    last = record.getAttribute(attributeName);
  }

  @Override
  public AttributeValue get() {
    return last;
  }

  @Override
  public void merge(Measurement merging) {
    if (merging instanceof CountMeasurement) {
      // we assume merging is done in time order
      last = ((LastMeasurement) merging).last;
    } else {
      throw new IllegalArgumentException(
          "Measurement type unmatch: " + merging.getClass().getSimpleName());
    }
  }
}
