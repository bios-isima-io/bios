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
package io.isima.bios.admin.v1;

import java.util.Comparator;

/**
 * Note: this comparator imposes orderings that are inconsistent with equals. The
 * StreamDesc.equals() method compares every element of StreamDesc to determine equality, whereas
 * this comparator only looks at version and case-insensitive name.
 */
public class StreamDescComparatorByVersionAndName implements Comparator<StreamDesc> {
  public int compare(StreamDesc s1, StreamDesc s2) {
    if (s1 == s2) {
      return 0;
    }
    if (s1 == null) {
      return -1;
    }
    if (s2 == null) {
      return 1;
    }
    if (s1.getVersion() < s2.getVersion()) {
      return -1;
    } else if (s1.getVersion() > s2.getVersion()) {
      return 1;
    }
    // The versions are equal. Use name as a tie breaker in order to get a stable sort.
    return s1.getName().toLowerCase().compareTo(s2.getName().toLowerCase());
  }
}
