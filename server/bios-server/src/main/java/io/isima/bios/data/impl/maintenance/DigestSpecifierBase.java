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
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class DigestSpecifierBase {
  /** Digest start time. */
  protected final long startTime;

  /** Digest end time. */
  protected final long endTime;

  /** Digesting would be done since this timestamp after the post process. */
  protected final long doneSince;

  /** Digesting would be done until this timestamp after the post process. */
  protected final long doneUntil;

  /** Digesting coverage as a time percentage after the post process. */
  protected double doneCoverage;

  @Setter protected Long interval;

  /** True if an update to this digestion has been explicitly requested by a user. */
  @Setter protected Boolean requested;

  /**
   * Some features such as AccumulatingCount needs to fetch certain time before startTime to
   * retrieve events in the previous window to handle late-appearing events.
   */
  @Setter protected long refetchTime = 0;

  /**
   * Calculates the digestion coverage percentage after the post process, given the origin from
   * where the digest is expected to start.
   */
  public void calculateDoneCoverage(long origin) {
    if (doneSince == -1) {
      if (doneUntil == -1) {
        doneCoverage = 0;
      } else {
        doneCoverage = percentage(doneUntil - origin, doneUntil - origin);
      }
    } else {
      doneCoverage = percentage(doneUntil - doneSince, doneUntil - origin);
    }
  }

  /** Calculate percentage with precision down to tenths. */
  private static double percentage(long numerator, long denominator) {
    if (denominator == 0) {
      return 0.0;
    }
    long ratio = (numerator * 10000 / denominator + 5) / 10;
    return ratio / 10.0;
  }
}
