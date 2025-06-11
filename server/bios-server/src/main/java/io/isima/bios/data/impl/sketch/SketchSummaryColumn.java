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
package io.isima.bios.data.impl.sketch;

import static io.isima.bios.models.DataSketchType.DISTINCT_COUNT;
import static io.isima.bios.models.DataSketchType.MOMENTS;
import static io.isima.bios.models.DataSketchType.QUANTILES;
import static io.isima.bios.models.DataSketchType.SAMPLE_COUNTS;

import io.isima.bios.models.DataSketchType;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * List of columns in sketch_summary table that hold summary information gathered by sketches. This
 * does not include key columns of sketch_summary table.
 */
@RequiredArgsConstructor
@Getter
public enum SketchSummaryColumn {
  MIN("double", MOMENTS, -1, -1),
  MAX("double", MOMENTS, -1, -1),
  COUNT("bigint", MOMENTS, -1, -1),
  SUM("double", MOMENTS, -1, -1),
  SUM2("double", MOMENTS, -1, -1),
  SUM3("double", MOMENTS, -1, -1),
  SUM4("double", MOMENTS, -1, -1),
  P0_01("double", QUANTILES, 0, 0.01),
  P0_1("double", QUANTILES, 1, 0.1),
  P1("double", QUANTILES, 2, 1),
  P10("double", QUANTILES, 3, 10),
  P25("double", QUANTILES, 4, 25),
  P50("double", QUANTILES, 5, 50),
  P75("double", QUANTILES, 6, 75),
  P90("double", QUANTILES, 7, 90),
  P99("double", QUANTILES, 8, 99),
  P99_9("double", QUANTILES, 9, 99.9),
  P99_99("double", QUANTILES, 10, 99.99),
  DISTINCTCOUNT("double", DISTINCT_COUNT, 11, 0),
  DCLB1("double", DISTINCT_COUNT, 12, 0),
  DCUB1("double", DISTINCT_COUNT, 13, 0),
  DCLB2("double", DISTINCT_COUNT, 14, 0),
  DCUB2("double", DISTINCT_COUNT, 15, 0),
  DCLB3("double", DISTINCT_COUNT, 16, 0),
  DCUB3("double", DISTINCT_COUNT, 17, 0),
  NUMSAMPLES("bigint", SAMPLE_COUNTS, -1, 0),
  SAMPLINGFRACTION("double", SAMPLE_COUNTS, -1, 0);

  private final String cassandraDatatype;
  private final DataSketchType dataSketchType;

  /**
   * For some sketches such as quantiles, summary values are stored in array slots instead of named
   * variables.
   */
  private final int slot;

  /** Percentile is only applicable to quantiles; ignored for others. */
  private final double percentile;

  public static final int TOTAL_SLOTS = 18;
  public static final int QUANTILES_SLOT_BEGIN = 0;
  public static final int QUANTILES_NUM_SLOTS = 11;
  public static final int DISTINCT_COUNT_SLOT_BEGIN = QUANTILES_NUM_SLOTS;
  public static final int DISTINCT_COUNT_NUM_SLOTS = 7;

  private static final SketchSummaryColumn[] columnsForSlots = createSlotsArray();

  private static SketchSummaryColumn[] createSlotsArray() {
    final var array = new SketchSummaryColumn[TOTAL_SLOTS];
    for (final var value : values()) {
      if (value.getSlot() >= 0) {
        array[value.getSlot()] = value;
      }
    }
    return array;
  }

  public static SketchSummaryColumn columnForSlot(int slot) {
    return columnsForSlots[slot];
  }

  public String getColumnName() {
    return this.toString().toLowerCase();
  }

  public static SketchSummaryColumn getAnyColumnForSketchType(DataSketchType sketchType) {
    for (final var column : SketchSummaryColumn.values()) {
      if (column.getDataSketchType() == sketchType) {
        return column;
      }
    }
    throw new UnsupportedOperationException();
  }

  /**
   * Returns a comma separated list of all sketch summary columns along with their Cassandra
   * datatypes, e.g. ", min double, max double, count bigint, sum double, sum2 double". There is a
   * leading comma at the beginning.
   */
  public static String getColumnsAndCassandraDatatypes() {
    final var sb = new StringBuilder();
    for (final var column : SketchSummaryColumn.values()) {
      sb.append(", ")
          .append(column.getColumnName())
          .append(" ")
          .append(column.getCassandraDatatype());
    }
    return sb.toString();
  }

  /**
   * Returns a comma separated list of summary columns corresponding to a specific sketch type,
   * without a leading or trailing comma.
   */
  public static String getColumnsAndCassandraDatatypes(DataSketchType sketchType) {
    final var sb = new StringBuilder();
    String delimiter = "";
    for (final var column : SketchSummaryColumn.values()) {
      if (column.getDataSketchType() == sketchType) {
        sb.append(delimiter)
            .append(column.getColumnName())
            .append(" ")
            .append(column.getCassandraDatatype());
        delimiter = ", ";
      }
    }
    return sb.toString();
  }

  /**
   * Returns a comma separated list of all sketch summary columns in Cassandra format, e.g. ", min,
   * max, count, sum". There is a leading comma at the beginning.
   */
  public static String getColumns() {
    final var sb = new StringBuilder();
    for (final var column : SketchSummaryColumn.values()) {
      sb.append(", ").append(column.getColumnName());
    }
    return sb.toString();
  }

  /**
   * Returns a comma separated list of the provided columns with a leading comma, e.g. ", min, max,
   * count, sum, sum2".
   */
  public static String getCommaSeparatedColumns(Iterable<SketchSummaryColumn> columns) {
    final var sb = new StringBuilder();
    for (final var column : columns) {
      sb.append(", ").append(column.getColumnName());
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return name();
  }
}
