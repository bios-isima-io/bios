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

/** Constant values regarding biOS service. */
public class Constants {
  /** Prefix of an internal name of a TFOS config entity, such as tenant and stream. */
  public static final String PREFIX_INTERNAL_NAME = "_";

  public static final String COUNT_METRIC = "count()";
  public static final String ORDER_BY_TIMESTAMP = ":timestamp";

  public static final String LOOKUP_QOS_THRESHOLD_MILLIS = "prop.lookupQosThresholdMillis";
  public static final String DEFAULT_LOOKUP_QOS_THRESHOLD_MILLIS_VALUE = "0";
}
