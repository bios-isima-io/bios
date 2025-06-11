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

import io.isima.bios.data.DefaultRecord.DefaultAttributeValue;
import java.util.List;
import java.util.Objects;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class Coordinate implements Comparable<Coordinate> {
  private final List<DefaultAttributeValue> attributes;

  public Coordinate(List<DefaultAttributeValue> attributes) {
    this.attributes = Objects.requireNonNull(attributes);
  }

  @Override
  public String toString() {
    return attributes.toString();
  }

  @Override
  public int compareTo(Coordinate that) {
    int numEntries = Math.min(this.attributes.size(), that.attributes.size());
    for (int i = 0; i < numEntries; ++i) {
      int cmp = this.attributes.get(i).compareTo(that.attributes.get(i));
      if (cmp != 0) {
        return cmp;
      }
    }
    if (this.attributes.size() > numEntries) {
      return -1;
    } else if (that.attributes.size() > numEntries) {
      return 1;
    }
    return 0;
  }
}
