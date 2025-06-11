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
package io.isima.bios.data.impl.maintenance;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Immutable digest specification descriptor used for a rollup process (indexing or
 * summarize/rollup).
 */
@EqualsAndHashCode(callSuper = true)
@Getter
@ToString(callSuper = true)
public class DigestSpecifier extends DigestSpecifierBase {
  /** Substream name to rollup (view for indexing or rollup for summarizing). */
  private final String name;

  /** Substream version. */
  private final Long version;

  /**
   * The constructor of the class.
   *
   * @param name Digesting target stream name. The stream is view or rollup.
   * @param version Digesting target stream version.
   * @param startTime Start time of the specifying digest interval in epoch milliseconds.
   * @param endTime End time of the specifying digest interval in epoch milliseconds.
   * @param doneSince Start time of the digest coverage after the process in epoch milliseconds.
   * @param doneUntil End time of the digest coverage after the process in epoch milliseconds.
   * @param interval Specifies how frequently this digestion is expected to occur.
   * @param requested Whether a user has requested this digestion explicitly.
   * @throws IllegalArgumentException when parameter name or version is null.
   */
  public DigestSpecifier(
      String name,
      Long version,
      long startTime,
      long endTime,
      long doneSince,
      long doneUntil,
      Long interval,
      Boolean requested) {
    super(startTime, endTime, doneSince, doneUntil);
    if (name == null || version == null) {
      throw new IllegalArgumentException("name and version may not be null");
    }
    this.name = name;
    this.version = version;
    this.interval = interval;
    this.requested = requested;
  }

  public DigestSpecifier(
      String name,
      Long version,
      long startTime,
      long endTime,
      long doneSince,
      long doneUntil,
      Long interval) {
    this(name, version, startTime, endTime, doneSince, doneUntil, interval, false);
  }

  public DigestSpecifier(
      String name, Long version, long startTime, long endTime, long doneSince, long doneUntil) {
    this(name, version, startTime, endTime, doneSince, doneUntil, null);
  }
}
