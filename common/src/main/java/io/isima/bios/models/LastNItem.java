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

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LastNItem {
  /** "t" (timestamp): timestamp; epoch time in milliseconds */
  @JsonProperty("t")
  private Long timestamp;

  /** "v" (value): value to collect */
  @JsonProperty("v")
  private Object value;

  @Override
  public String toString() {
    return String.format("{t: %d, v: %s}", timestamp, value);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof LastNItem)) {
      return false;
    }
    final var o = (LastNItem) other;
    return Objects.equals(timestamp, o.timestamp) && Objects.equals(value, o.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(timestamp, value);
  }
}
