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
package io.isima.bios.sdk;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CompositeKey {
  private final List<Object> elements;

  private CompositeKey(List<Object> elements) {
    this.elements = elements;
  }

  public List<Object> asObjectList() {
    return elements;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof CompositeKey)) {
      return false;
    }
    return elements.equals(((CompositeKey) obj).elements);
  }

  @Override
  public int hashCode() {
    return elements.hashCode();
  }

  @Override
  public String toString() {
    return elements.toString();
  }

  public static CompositeKey of(Object... elements) {
    return of(List.of(elements));
  }

  public static CompositeKey of(List<Object> elements) {
    return new CompositeKey(List.copyOf(elements));
  }

  public static CompositeKeyBuilder newBuilder() {
    return new CompositeKeyBuilder();
  }

  public static class CompositeKeyBuilder {
    private final List<Object> elements = new ArrayList<>();

    public CompositeKeyBuilder addValue(Object value) {
      elements.add(value);
      return this;
    }

    public CompositeKey build() {
      return new CompositeKey(Collections.unmodifiableList(elements));
    }
  }
}
