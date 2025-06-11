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
package io.isima.bios.models.isql;

/** The shape of a response from a metric or select statement. */
public enum ResponseShape {
  /**
   * A single value, e.g. result of SUM metric. Not using the words "scalar" or "atomic" to have
   * some flexibility in the future. Two examples of this shape of response are: "10", and "Hello
   * world!". This shape is only applicable to individual metrics and not to select queries as a
   * whole.
   */
  VALUE,

  /**
   * This can be either one set of rows or multiple time-based data windows each containing a set of
   * rows. All the rows in this response have the same columns, e.g. result of SAMPLECOUNTS metric.
   * For a select statement it corresponds to a single SelectQueryResponse.
   */
  RESULT_SET,

  /**
   * Multiple sets of rows and/or multiple groups of time-based data windows, with potentially
   * different sets of columns, e.g. result of SYNOPSIS metric. For a select statement it
   * corresponds to multiple SelectQueryResponse objects.
   */
  MULTIPLE_RESULT_SETS
}
