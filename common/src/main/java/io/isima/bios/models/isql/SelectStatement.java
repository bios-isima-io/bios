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

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;

/** Select query on a signal based on a time range. */
public interface SelectStatement extends AsyncExecutableISqlStatement {

  /**
   * Returns start time of the time range for the time range query.
   *
   * @return start time
   */
  long getStartTime();

  /**
   * Returns end time of the time range for the time range query.
   *
   * @return end time
   */
  long getEndTime();

  /**
   * Returns whether the distinct keyword was used in the query.
   *
   * @return true, if distinct, false, otherwise
   */
  boolean isDistinct();

  /**
   * Returns attributes in the projection.
   *
   * @return list of attributes projected in the query
   */
  List<String> getSelectAttributes();

  /**
   * Returns metrics (aggregation) projected in the query.
   *
   * @return metrics
   */
  List<? extends Metric> getMetrics();

  /**
   * Returns name of the signal table from which to select records.
   *
   * @return signal table name
   */
  String getFrom();

  /**
   * Returns the where clause in the query (selection criteria).
   *
   * @return where clause of the query as a string.
   */
  String getWhere();

  /**
   * Returns the attributes used in the group by clause in the query.
   *
   * @return list of attributes in the group by clause
   */
  List<String> getGroupBy();

  /**
   * Returns the details of the order by clause in the query.
   *
   * @return order by clause
   */
  OrderBy getOrderBy();

  /**
   * Returns window. Useful for users that may not know the window type in the query.
   *
   * @return Type of window
   */
  Window<?> getOneWindow();

  /**
   * Returns analytic windows specified in the query, if any.
   *
   * <p>Currently only one window can be specified per query. This may change in the future.
   *
   * @return windows specified in the query, null otherwise
   */
  <W extends Window<W>> Window<W> getOneWindow(Class<W> windowType);

  /**
   * Returns true, if a limit was specified in the query.
   *
   * @return true if a limit was specified in the query
   */
  boolean hasLimit();

  /**
   * Returns limit specified in the query. 0 means limit was not specified.
   *
   * @return non-zero limit, zero if no limit was specified.
   */
  int getLimit();

  /**
   * Returns true, if a window was specified in the query.
   *
   * @return true if a window was specified in the query
   */
  boolean hasWindow();

  /**
   * Returns true, if order by clause was specified in the query.
   *
   * @return true if an order by clause was used in the query.
   */
  boolean hasOrderBy();

  boolean isOnTheFly();

  /**
   * Returns the shape of the response this select query is expected to return: a single result set
   * or multiple result sets. See comments in ResponseShape enums for more details.
   */
  ResponseShape getResponseShape();

  @Override
  default StatementType getStatementType() {
    return StatementType.SELECT;
  }

  interface From<P, Q> {
    /**
     * Specify name of the signal to query.
     *
     * @param from Name of the signal
     * @return Next step in the query builder process
     */
    QueryCondition<P, Q> from(String from);
  }

  interface FromContext<P, Q, B> {
    /** Specify name of the context to query. */
    B fromContext(String fromContext);

    /**
     * Specify name of the signal to query.
     *
     * @param from Name of the signal
     * @return Next step in the query builder process
     */
    QueryCondition<P, Q> from(String from);
  }

  interface QueryCondition<P, Q> {
    /**
     * Distinct clause.
     *
     * @return for additional optional query components
     */
    QueryCondition<P, Q> distinct();

    /**
     * Specifies where clause as a single string.
     *
     * @param where where clause (filter condition)
     * @return for additional optional query components
     */
    QueryCondition<P, Q> where(String where);

    /**
     * Specifies group by clause as a list of strings, each representing an attribute name in the
     * signal.
     *
     * @param attributes name(s) of attributes
     * @return for additional optional query components
     */
    QueryCondition<P, Q> groupBy(String... attributes);

    /**
     * Specifies order by clause.
     *
     * @param specifier order by specifier
     * @return for additional optional query components
     */
    QueryCondition<P, Q> orderBy(OrderBy.OrderByFinalSpecifier specifier);

    /**
     * Specifies limit clause for limiting the results.
     *
     * @param limit amount to limit the results to
     * @return for additional optional query components
     */
    QueryCondition<P, Q> limit(int limit);

    /**
     * Specifies analytical window(s) for grouping and summarizing results.
     *
     * <p>Supports {@link SlidingWindow} and {@link TumblingWindow} based queries. See {@link
     * Window} for supported window based queries.
     *
     * @param specifier window specifier for specifying the window type and parameters
     * @return for additional query components
     */
    SnappedTimeRange<Q> window(Window.WindowFinalSpecifier<?> specifier);

    /**
     * Specifies mandatory time range for the query. This will bound the query to return results
     * within the given time range.
     *
     * <p>Query is complete when the time range is specified as time range is the last step in the
     * query building process.
     *
     * @param originTime Origin time as milliseconds since epoch.
     * @param delta Time delta in milliseconds. The value may be negative, in which case the start
     *     time is earlier than originTime.
     * @return Final builder
     */
    P timeRange(long originTime, long delta);
  }

  interface SnappedTimeRange<P> {
    /**
     * Specifies mandatory time range for the query that will be snapped to the nearest rollup
     * interval.
     *
     * <p>A snapped time range must be specified if window is specified in the query.
     *
     * @param originTime Origin time adjusted and aligned to the rollup interval
     * @param delta Delta
     * @return final step
     */
    P snappedTimeRange(long originTime, long delta);

    /**
     * Specifies mandatory time range for the query that will be snapped to the nearest rollup
     * interval.
     *
     * <p>A snapped time range must be specified if window is specified in the query.
     *
     * @param originTime Origin time adjusted and aligned to the rollup interval
     * @param delta Delta
     * @return final step
     */
    P snappedTimeRange(OffsetDateTime originTime, Duration delta);

    /**
     * Specifies mandatory time range for the query that will be snapped to the nearest rollup
     * interval.
     *
     * <p>A snapped time range must be specified if window is specified in the query.
     *
     * @param originTime Origin time adjusted and aligned to the snap step size
     * @param delta Delta
     * @param snapStepSize Step size to snap
     * @return final step
     */
    P snappedTimeRange(long originTime, long delta, long snapStepSize);

    /**
     * Specifies mandatory time range for the query that will be snapped to the nearest rollup
     * interval.
     *
     * <p>A snapped time range must be specified if window is specified in the query.
     *
     * @param originTime Origin time adjusted and aligned to the rollup interval
     * @param delta Delta
     * @param snapStepSize Step size to snap
     * @return final step
     */
    P snappedTimeRange(OffsetDateTime originTime, Duration delta, Duration snapStepSize);
  }

  interface LinkedBuilder<P, Q> extends From<P, Q>, QueryCondition<P, Q>, SnappedTimeRange<Q> {}
}
