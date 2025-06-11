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

import io.isima.bios.utils.StringUtils;
import lombok.EqualsAndHashCode;

/** Immutable Class that carries a range of long values. */
@EqualsAndHashCode
public class Range {

  private final long begin;

  private final long end;

  /**
   * The constructor to build an instance from beginning time and ending time.
   *
   * @param begin Beginning of the range
   * @param end Ending of the range
   */
  public Range(long begin, long end) {
    if (end < begin) {
      throw new IllegalArgumentException("Ending of the range must not be less than beginning");
    }
    this.begin = begin;
    this.end = end;
  }

  /**
   * Creates another range with the same width at the end of current range.
   *
   * <p>The new range has following values:
   *
   * <ul>
   *   <li>new.begin = old.begin + width
   *   <li>new.end = old.end + width
   * </ul>
   *
   * <p>where width = old.end - old.begin
   *
   * @return The new shifted range
   * @throws IllegalStateException thrown to indicate that the range is not able to shift since its
   *     width is zero
   */
  public Range shift() {
    if (begin == end) {
      throw new IllegalStateException("Zero-width range cannot be shifted");
    }
    long offset = end - begin;
    return new Range(begin + offset, end + offset);
  }

  public long getBegin() {
    return begin;
  }

  public long getEnd() {
    return end;
  }

  public long getWidth() {
    return end - begin;
  }

  /**
   * Checks whether the instance includes the specified time range.
   *
   * @param otherRange The time range to check
   * @return True if the instance includes the given time range, false if not
   */
  public boolean includes(Range otherRange) {
    return begin <= otherRange.begin && end >= otherRange.end;
  }

  /**
   * Answers whether the time range includes an instant.
   *
   * @param instant The instant to test, milliseconds since epoch
   * @return True if the instant is in the range, false otherwise. The instant on the beginning edge
   *     is handled inclusively. The instant on the ending edge is handled exclusively.
   */
  public boolean includes(long instant) {
    return begin <= instant && instant < end;
  }

  /**
   * Checks whether the instance includes the specified time range.
   *
   * @param otherRange The time range to check
   * @return True if there is an overlap, false if not
   */
  public boolean overlaps(Range otherRange) {
    return !this.includes(otherRange) && !otherRange.includes(this);
  }

  /**
   * Checks the relative position of specified instant against the time range.
   *
   * @param instant The instant to check, milliseconds since epoch
   * @return -1 if the instant is before the time range. 0 if the instant is in the range. 1 if the
   *     instant is after the time range.
   */
  public int compare(long instant) {
    if (instant < begin) {
      return -1;
    }
    if (instant < end) {
      return 0;
    }
    return 1;
  }

  @Override
  // TODO(Naoki): What to do when the range is not about time?
  public String toString() {
    return new StringBuilder("[")
        .append(StringUtils.tsToIso8601(begin))
        .append("(")
        .append(begin)
        .append("), ")
        .append(StringUtils.tsToIso8601(end))
        .append("(")
        .append(end)
        .append(")]")
        .toString();
  }
}
