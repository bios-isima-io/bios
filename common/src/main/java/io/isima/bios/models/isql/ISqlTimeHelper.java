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
package io.isima.bios.models.isql;

/** Utility class that provides iSQL time values. */
public class ISqlTimeHelper {
  public long now() {
    return System.currentTimeMillis();
  }

  public long days(long days) {
    return days * hours(24);
  }

  public long hours(long hours) {
    return hours * minutes(60);
  }

  public long minutes(long minutes) {
    return minutes * seconds(60);
  }

  public long seconds(long seconds) {
    return seconds * 1000;
  }

  public long millis(long millis) {
    return millis;
  }
}
