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

import com.google.common.annotations.VisibleForTesting;

public enum TimeSpan {
  @VisibleForTesting
  _S3(5 * 1000L, "3 seconds"),
  @VisibleForTesting
  _S10(10 * 1000L, "10 seconds"),
  @VisibleForTesting
  _S30(30 * 1000L, "30 seconds"),
  @VisibleForTesting
  _S90(90 * 1000L, "90 seconds"),
  M5(5 * 60 * 1000L, "5 minutes"),
  M15(15 * 60 * 1000L, "15 minutes"),
  M30(15 * 60 * 1000L, "30 minutes"),
  H1(1 * 60 * 60 * 1000L, "1 hour"),
  H6(6 * 60 * 60 * 1000L, "6 hours"),
  H12(12 * 60 * 60 * 1000L, "12 hours"),
  D1(1 * 24 * 60 * 60 * 1000L, "1 day"),
  D4(4 * 24 * 60 * 60 * 1000L, "4 days"),
  D7(7 * 24 * 60 * 60 * 1000L, "7 days"),
  D14(14 * 24 * 60 * 60 * 1000L, "14 days"),
  D30(30 * 24 * 60 * 60 * 1000L, "30 days");

  private Long value;
  private String name;

  private TimeSpan(long value, String name) {
    this.value = value;
    this.name = name;
  }

  public long getValue() {
    return this.value;
  }
}
