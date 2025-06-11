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

/** Supported decorations for the OrderBy part of the select query. */
public interface OrderBy {
  /**
   * Gets the attribute to order by.
   *
   * @return attribute name
   */
  String getBy();

  /**
   * Sorts in descending order. Default is ascending order.
   *
   * @return true if descending, false otherwise
   */
  boolean isDescending();

  /**
   * Case sensitive sorting. Default is case insensitive.
   *
   * @return true if case sensitive sorting, false otherwise.
   */
  boolean isCaseSensitive();

  /**
   * Returns an order by specifier against a single attribute.
   *
   * <p>Currently order by is specified against a single attribute.
   *
   * @param by Sort field
   * @return a specifier for defining further optional properties
   */
  static OrderByFinalSpecifier by(String by) {
    return ISqlBuilderProvider.getBuilderProvider().getOrderBySpecifier(by);
  }

  interface OrderByFinalSpecifier {
    /**
     * Sorts the query results ascending on the sort field. This is the default.
     *
     * @return specifier for defining further properties
     */
    OrderByFinalSpecifier asc();

    /**
     * Sorts the query results descending on the sort field.
     *
     * @return specifier for defining further properties
     */
    OrderByFinalSpecifier desc();

    /**
     * Case sensitive sorting on the sort field. Default is case insensitive.
     *
     * @return specifier for defining further properties
     */
    OrderByFinalSpecifier caseSensitive();
  }
}
