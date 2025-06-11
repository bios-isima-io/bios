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
package io.isima.bios.req;

import io.isima.bios.models.MetricFunction;

/** Aggregate interface hiding protobuf and JSON implementations. */
public interface Aggregate {
  /**
   * Gets the aggregate function.
   *
   * @return aggregate function
   */
  MetricFunction getFunction();

  /**
   * Gets the "by" attribute for this aggregate.
   *
   * @return by attribute
   */
  String getBy();

  /**
   * Gets the Alias.
   *
   * @return alias
   */
  String getAs();

  /**
   * Gets the actual derived attribute name. Depends on whether alias was specified.
   *
   * @return derived attribute name
   */
  String getOutputAttributeName();

  /**
   * Supported Aggregate operations, choosing one per aggregate.
   *
   * @param <B> Next steps in the building process
   */
  interface Op<B> {
    B count();

    B lastOf(String of);

    B minOf(String of);

    B maxOf(String of);

    B sumOf(String of);
  }

  /** For independent builder. */
  interface Build {
    Build as(String as);

    Aggregate build();
  }

  /**
   * For linked builder.
   *
   * @param <P> Parent builder type
   */
  interface End<P> {
    End<P> as(String as);

    P end();
  }

  interface Builder extends Op<Build>, Build {}

  // Builder that is nested with the parent for nested fluency
  interface LinkedBuilder<P> extends Op<End<P>>, End<P> {}

  /**
   * Entry point into the builder.
   *
   * @return Next steps in the building process
   */
  static Op<Build> op() {
    return BuilderProvider.getBuilderProvider().getAggregateBuilder();
  }
}
