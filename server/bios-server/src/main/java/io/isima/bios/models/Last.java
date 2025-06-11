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
package io.isima.bios.models;

import io.isima.bios.models.v1.Aggregate;

/**
 * Class used for specifying a last aggregate function.
 *
 * <p>The function is used for group extraction or summarize -- takes the event that has the latest
 * timestamp in a group, and returns its value of the specified attribute.
 */
public class Last extends Aggregate {

  /**
   * Constructor with the target attribute.
   *
   * @param by Attribute to return.
   */
  public Last(String by) {
    super(MetricFunction.LAST);
    this.by = by;
  }

  /**
   * Set the aggregate output attribute name.
   *
   * <p>Default output name of an aggregate function is <i>function_name</i>(<i>attribute</i>), such
   * as <code>sum(amount)</code>. This method overrids this default name by specified one.
   *
   * @param name Attribute name of the agrregate function output.
   * @return Self.
   */
  public Last as(String as) {
    if (as == null || as.isEmpty()) {
      throw new IllegalArgumentException("'as' may not be null or empty");
    }
    setAs(as);
    return this;
  }
}
