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
package io.isima.bios.utils;

import java.util.Objects;

public class DirectedEdge {
  private String parent;
  private String child;

  public DirectedEdge(String parent, String child) {
    Objects.requireNonNull(parent, "`parent` cannot be null");
    Objects.requireNonNull(child, "`child` cannot be null");
    this.parent = parent;
    this.child = child;
  }

  public String getParent() {
    return parent;
  }

  public String getChild() {
    return child;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DirectedEdge that = (DirectedEdge) o;
    return parent.equals(that.parent) && child.equals(that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parent, child);
  }
}
