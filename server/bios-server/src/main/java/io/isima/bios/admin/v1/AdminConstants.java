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
package io.isima.bios.admin.v1;

/** Class to provide constant values regarding AdminInternal. */
public class AdminConstants {
  /** Default event attributes delimiter of ingesting plain-text events. */
  public static final char DEFAULT_EVENT_ATTR_DELIMITER = ',';

  /** Default event key-value delimiter of ingesting plain-text events with key names. */
  public static final char DEFAULT_EVENT_KEY_VALUE_DELIMITER = ':';

  /** Default event escape character. */
  public static final char DEFAULT_EVENT_ESCAPE = '\\';

  public static final String ROLLUP_STREAM_MAX_SUFFIX = "_max";

  public static final String ROLLUP_STREAM_MIN_SUFFIX = "_min";

  public static final String ROLLUP_STREAM_SUM_SUFFIX = "_sum";

  public static final String ROLLUP_STREAM_COUNT_ATTRIBUTE = "count";

  /** View stream name prefix. */
  public static final String VIEW_STREAM_PREFIX = ".view.";

  /** Index stream name prefix. */
  public static final String INDEX_STREAM_PREFIX = ".index.";

  /** Rollup / context feature stream name prefix. */
  public static final String ROLLUP_STREAM_PREFIX = ".rollup.";

  /** Context index stream name prefix. */
  public static final String CONTEXT_INDEX_STREAM_PREFIX = ".index.";

  /** Root feature name used for storing overall rollup progress. */
  public static final String ROOT_FEATURE = "__root";

  /**
   * Delimiter separating context name and attribute name in an EnrichedAttribute (part of a
   * ContextEnrichment).
   */
  public static final String ENRICHED_ATTRIBUTE_DELIMITER = ".";

  /**
   * A regex string separate from the delimiter itself is needed for characters such as dot ('.'),
   * which have a special meaning in regular expressions.
   */
  public static final String ENRICHED_ATTRIBUTE_DELIMITER_REGEX = "[.]";

  /** This delimiter in the stream name signifies a virtual stream such as a synthetic context. */
  public static final String RESERVED_NAME_DELIMITER = ":";

  /** This prefix is used to derive the signal name for audit signal for contexts. */
  public static final String CONTEXT_AUDIT_SIGNAL_PREFIX = "audit";

  /** Constants used for context audit signals. */
  public static final String CONTEXT_AUDIT_ATTRIBUTE_OPERATION = "_operation";

  public static final String CONTEXT_AUDIT_ATTRIBUTE_PREFIX_PREV = "prev";
  public static final String CONTEXT_AUDIT_FEATURE_NAME = "byOperation";
  public static final Long CONTEXT_AUDIT_FEATURE_INTERVAL = 300000L;
}
