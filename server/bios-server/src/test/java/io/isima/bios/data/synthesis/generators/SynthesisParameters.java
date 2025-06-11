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
package io.isima.bios.data.synthesis.generators;

/** Class to collect synthesis parameters. */
public class SynthesisParameters {

  public static final int NUM_CONTEXT_ENTRIES = 15;
  public static final double RECORDS_PER_SECOND = 2;

  // parameters for default generators
  public static final long INTEGER_MIN = 0;
  public static final long INTEGER_MAX = 10000;
  public static final double DECIMAL_MIN = 0.0;
  public static final double DECIMAL_MAX = 10000.0;
  public static final double BOOLEAN_TRUE_RATIO = 0.5;
}
