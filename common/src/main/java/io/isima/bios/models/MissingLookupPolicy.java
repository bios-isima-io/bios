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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.isima.bios.utils.EnumStringifier;

/** Behaviors when lookup entry is missing in envirhments. */
public enum MissingLookupPolicy {
  /** Reject the input. This is only allowed if the parent stream is a signal. */
  REJECT,
  /** Store the pre-defined fill-in value instead. This is allowed for both signals and contexts. */
  STORE_FILL_IN_VALUE,
  /**
   * Fail the lookup of the parent stream. This is only allowed if the parent stream is a context.
   *
   * <p>This applies at a higher level than REJECT. E.g. suppose signal A gets enriched from context
   * B using foreign key A_B, and context B gets enriched from context C using foreign key B_C.
   * Further suppose that context B contains an entry with primary key A_B, but context C does not
   * contain an entry with primary key B_C.
   *
   * <ul>
   *   <li>Case 1: Context B uses FAIL_PARENT_LOOKUP when enriching with context C. Context B will
   *       pretend as if it does not have any entry with primary key A_B. This will cause signal A
   *       to make the decision of whether to REJECT or STORE_FILL_IN_VALUE.
   *   <li>Case 2: Context B uses STORE_FILL_IN_VALUE when enriching with context C. Then the fillIn
   *       values specified for the enriched attributes will be returned by context B to signal A.
   * </ul>
   *
   * <p>This is different from REJECT because REJECT just applies to immediate join, and if we had
   * allowed the use of REJECT when the parent stream is a context, we would have needed to reject
   * the upsert of a context entry into context B. However, we do not want to do the lookup at
   * context entry upsert time; rather we want to do it when the context entry is actually used by a
   * signal event. So, we need FAIL_PARENT_LOOKUP separate from REJECT.
   */
  FAIL_PARENT_LOOKUP,
  ;

  private static final EnumStringifier<MissingLookupPolicy> stringifier =
      new EnumStringifier<>(values());

  @JsonCreator
  static MissingLookupPolicy forValue(String value) {
    return stringifier.destringify(value);
  }

  @JsonValue
  String stringify() {
    return stringifier.stringify(this);
  }
}
