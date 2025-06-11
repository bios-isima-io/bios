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

import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode
public abstract class Duplicatable {
  public abstract Duplicatable duplicate();

  /**
   * Clone a list.
   *
   * @param src source
   * @return Clone of the source list if src is not null. Null otherwise.
   */
  @SuppressWarnings("unchecked")
  protected static <T extends Duplicatable> List<T> copyList(List<T> src) {
    if (src == null) {
      return null;
    }
    List<T> out = new ArrayList<>();
    for (T entry : src) {
      out.add((T) entry.duplicate());
    }
    return out;
  }

  // used by equals methods
  protected static <T> boolean memberEquals(T left, T right) {
    if ((left == null && right != null) || (left != null && right == null)) {
      return false;
    }
    return left == null || left.equals(right);
  }
}
