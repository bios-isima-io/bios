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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface ISqlStatement {

  StatementType getStatementType();

  // TODO(pradeep): Some of the builders below (especially related to contexts) are not needed any
  //  more and should be deleted. Not deleting yet because of unnecessary dependencies that need
  //  time to be cleaned out well.

  /**
   * @deprecated Use Bios.isql().select() instead. Specifies select with all attributes in the
   *     signal projected.
   * @return Next step in the query builder
   */
  @Deprecated
  static SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ContextSelectBuilder>
      select() {
    return ISqlBuilderProvider.getBuilderProvider().getSelectStatementBuilder();
  }

  /**
   * @deprecated Use Bios.isql().select() instead. Specifies select with specified attributes
   *     projected.
   *     <p>The attributes are specified by name and the attributes MUST be present in the signal.
   * @param attributes attribute names
   * @return Next step in the query builder
   */
  @Deprecated
  static SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ContextSelectBuilder>
      select(String... attributes) {
    if (attributes == null) {
      throw new IllegalArgumentException("nulls not allowed for select parameter 'attributes'");
    }
    return ISqlBuilderProvider.getBuilderProvider()
        .getSelectStatementBuilder(Arrays.asList(attributes));
  }

  /**
   * @deprecated Use Bios.isql().select() instead. Specifies select with metric aggregation.
   *     <p>Use {@link Metric} to look at currently available metrics.
   * @param specifiers Metric specifiers
   * @return Next step in the query builder
   */
  @Deprecated
  static SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ContextSelectBuilder>
      select(Metric.MetricFinalSpecifier... specifiers) {
    if (specifiers == null) {
      throw new IllegalArgumentException("nulls not allowed for select parameter 'metrics'");
    }
    return ISqlBuilderProvider.getBuilderProvider()
        .getSelectStatementBuilder(null, Arrays.asList(specifiers));
  }

  /**
   * @deprecated Use Bios.isql().select() instead. Specifies a combination of normal attributes and
   *     metric aggregations.
   *     <p>In order to provide user the flexibility to specify a mixture of normal Java strings and
   *     {@link Metric.MetricFinalSpecifier} metric specifiers, an type unsafe object array is
   *     needed. Due to this, type safety is checked at run time.
   * @param attributes attribute name and/or metric specifiers
   * @return Next step in the query builder
   */
  @Deprecated
  static SelectStatement.FromContext<
          SelectBuilder, SnappedTimeRangeSelectBuilder, ContextSelectBuilder>
      select(Object... attributes) {
    if (attributes != null) {
      final List<String> attributeList = new ArrayList<>();
      final List<Metric.MetricFinalSpecifier> metricList = new ArrayList<>();
      for (Object o : attributes) {
        if (o instanceof String) {
          attributeList.add((String) o);
        } else if (o instanceof Metric.MetricFinalSpecifier) {
          metricList.add((Metric.MetricFinalSpecifier) o);
        } else {
          throw new IllegalArgumentException("Unknown object of type " + o.getClass().getName());
        }
      }
      return ISqlBuilderProvider.getBuilderProvider()
          .getSelectStatementBuilder(attributeList, metricList);
    } else {
      throw new IllegalArgumentException("nulls not allowed for parameter 'attributes'");
    }
  }

  /**
   * @deprecated Use Bios.isql().insert() instead. Specifies insert clause.
   * @return Insert Query Builder
   */
  @Deprecated
  static InsertStatement.Into insert() {
    return ISqlBuilderProvider.getBuilderProvider().getInsertStatementBuilder();
  }

  // TODO(BB-1230): Do something to following cross dependencies / downcasts

  interface SelectBuilder {
    /**
     * Builds the final select statement.
     *
     * @return Select statement
     */
    SelectStatement build();
  }

  interface SnappedTimeRangeSelectBuilder extends SelectBuilder {
    SelectBuilder onTheFly();
  }

  interface InsertBuilder {
    /**
     * Builds the final insert statement.
     *
     * @return Insert statement
     */
    InsertStatement build();
  }

  interface ContextSelectBuilder {
    /** Builder for the final context select statement. */
    ContextSelectBuilder where(WhereClause.KeysFinalSpecifier specifier);

    ContextSelectStatement build();
  }
}
