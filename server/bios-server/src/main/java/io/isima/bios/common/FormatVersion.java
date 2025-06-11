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
package io.isima.bios.common;

/** Integer constants to indicate TFOS data format version. */
public class FormatVersion {
  /** The initial version. */
  public static final Integer INITIAL = 0;

  /**
   * From this version, a rollup table data does not include convoluted past data when horizon is
   * longer than interval. The data includes only the last interval's data. The past data for longer
   * horizon is convoluted in extraction time. This gives the table capability to be reused for
   * different intervals or horizons. The feature is useful also for reusing rollups between two
   * different stream versions.
   */
  public static final Integer REUSABLE_ROLLUP = 1;

  /**
   * When the formatVersion property is missing in StreamStoreDesc, i.e. stored stream description,
   * the version is considered to be INITIAL.
   */
  public static final Integer DEFAULT = INITIAL;

  /**
   * The version that is used for creating/updating a new stream.
   *
   * <p>Change the assigned version whenever you add a new version.
   */
  public static final Integer LATEST = REUSABLE_ROLLUP;
}
