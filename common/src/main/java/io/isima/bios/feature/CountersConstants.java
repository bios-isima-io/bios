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
package io.isima.bios.feature;

/** Class to provide common names for counters. */
public class CountersConstants {
  // Updates signal
  public static final String ATTRIBUTE_SUFFIX_VALUE = "Value";
  public static final String ATTRIBUTE_OPERATION = "operation";
  public static final String OPERATION_VALUE_SET = "set";
  public static final String OPERATION_VALUE_CHANGE = "change";

  // Snapshots context
  public static final String ATTRIBUTE_TIMESTAMP = "timestamp";
  public static final String ATTRIBUTE_METADATA = "metadata";

  // Checkpointing
  public static final String ATTRIBUTE_CHECKPOINT_KEY = "realm";

  // For making keys
  public static final String CHECKPOINT_REALM = "counters";
  public static final String COMPOSITE_KEY_DELIMITER = "\t";
}
