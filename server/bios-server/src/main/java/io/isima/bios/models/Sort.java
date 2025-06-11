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

/**
 * A class that represents a sort view function. The function can be specified for an extract API to
 * sort output events by a certain attribute.
 *
 * <p>The function takes a list of extracted events and sorts entries by given sort key. Output is a
 * sorted list of events. An output event always includes sort key attribute even if it is not
 * specified in attributes argument for the extract operation.
 */
public class Sort extends View {

  /**
   * The basic Sort constructor.
   *
   * @param by Name of the attribute that is used as sort key.
   */
  public Sort(String by) {
    super(ViewFunction.SORT);
    this.by = by;
  }

  /**
   * Constructor with reverse flag.
   *
   * @param by Name of the attribute that is used as sort key.
   * @param reverse If true, the sort is done in descending order
   */
  public Sort(String by, boolean reverse) {
    super(ViewFunction.SORT);
    this.by = by;
    this.reverse = Boolean.valueOf(reverse);
  }

  /**
   * Sets the reverse sort to true, the sort is done in descending order.
   *
   * @return the sort
   */
  public Sort setReverse() {
    this.reverse = Boolean.TRUE;
    return this;
  }
}
