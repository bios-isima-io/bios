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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Row;
import io.isima.bios.models.DataSketchType;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@Getter
@Setter
@ToString
public class SketchSummary {
  // Moments.
  private Double min;
  private Double max;
  private Long count;
  private Double sum;
  private Double sum2;
  private Double sum3;
  private Double sum4;

  @Getter(lazy = true)
  private final Double[] columnValues = new Double[SketchSummaryColumn.TOTAL_SLOTS];

  private Long numSamples;
  private Double samplingFraction;

  public SketchSummary incorporateRow(Row row, Iterable<SketchSummaryColumn> columns) {
    for (final var column : columns) {
      switch (column.getDataSketchType()) {
        case MOMENTS:
          switch (column) {
            case MIN:
              min = row.getDouble(column.getColumnName());
              break;
            case MAX:
              max = row.getDouble(column.getColumnName());
              break;
            case COUNT:
              count = row.getLong(column.getColumnName());
              break;
            case SUM:
              sum = row.getDouble(column.getColumnName());
              break;
            case SUM2:
              sum2 = row.getDouble(column.getColumnName());
              break;
            case SUM3:
              sum3 = row.getDouble(column.getColumnName());
              break;
            case SUM4:
              sum4 = row.getDouble(column.getColumnName());
              break;
            default:
              throw new UnsupportedOperationException(
                  "Unsupported SketchSummaryColumn %s" + column.name());
          }
          break;
        case QUANTILES:
        case DISTINCT_COUNT:
          this.getColumnValues()[column.getSlot()] = row.getDouble(column.getColumnName());
          break;
        case SAMPLE_COUNTS:
          if (column == SketchSummaryColumn.NUMSAMPLES) {
            numSamples = row.getLong(column.getColumnName());
          } else if (column == SketchSummaryColumn.SAMPLINGFRACTION) {
            samplingFraction = row.getDouble(column.getColumnName());
          }
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported SketchSummaryColumn %s" + column.name());
      }
    }
    return this;
  }

  /**
   * Bind only the available summary columns to the given BoundStatement. Cassandra will not
   * generate tombstones for the missing unset columns. This, along with the upsert nature of
   * Cassandra insert statements allows us to merge summaries from different sketches into one
   * summary row very conveniently. Reference:
   * https://docs.datastax.com/en/developer/java-driver/3.0/manual/statements/prepared/
   */
  public void bindAvailableSummaryColumns(BoundStatement boundStatement) {
    // Moments sketch columns.
    if (min != null) {
      boundStatement.setDouble("min", min);
      boundStatement.setDouble("max", max);
      boundStatement.setDouble("sum", sum);
      boundStatement.setDouble("sum2", sum2);
      boundStatement.setDouble("sum3", sum3);
      boundStatement.setDouble("sum4", sum4);
    }

    // Quantiles sketch columns.
    if (getColumnValues()[SketchSummaryColumn.QUANTILES_SLOT_BEGIN] != null) {
      for (final var column : SketchSummaryColumn.values()) {
        if (column.getDataSketchType() == DataSketchType.QUANTILES) {
          boundStatement.setDouble(column.getColumnName(), getColumnValues()[column.getSlot()]);
        }
      }
    }

    // Distinct count sketch columns.
    if (getColumnValues()[SketchSummaryColumn.DISTINCT_COUNT_SLOT_BEGIN] != null) {
      for (final var column : SketchSummaryColumn.values()) {
        if (column.getDataSketchType() == DataSketchType.DISTINCT_COUNT) {
          boundStatement.setDouble(column.getColumnName(), getColumnValues()[column.getSlot()]);
        }
      }
    }

    // Sample counts sketch columns.
    if (numSamples != null) {
      boundStatement.setLong("numsamples", numSamples);
      boundStatement.setDouble("samplingfraction", samplingFraction);
    }

    // Count is set by many different sketches.
    if (count != null) {
      boundStatement.setLong("count", count);
    }
  }
}
