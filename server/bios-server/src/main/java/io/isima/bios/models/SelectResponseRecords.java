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

import io.isima.bios.data.ColumnDefinition;
import io.isima.bios.data.DefaultRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import lombok.Getter;
import lombok.Setter;

/**
 * Class to carry the response records for a select request.
 *
 * <p>Note that this class is not thread safe.
 */
public class SelectResponseRecords {
  /** Column definitions to be returned to the client. */
  @Getter @Setter private Map<String, ColumnDefinition> definitions;

  /** The response data windows. */
  private Map<Long, List<DefaultRecord>> dataWindows;

  /** Constructor with column definitions. */
  public SelectResponseRecords(Map<String, ColumnDefinition> definitions) {
    this.definitions = Objects.requireNonNull(definitions);
  }

  public SelectResponseRecords() {}

  /**
   * Gets a data window from the response records.
   *
   * @param windowBeginTime Window begin time
   * @return The list of response records that starts with the specified time, or null if no such
   *     records are found.
   */
  public List<DefaultRecord> getDataWindow(Long windowBeginTime) {
    return getDataWindows().get(windowBeginTime);
  }

  /**
   * Gets a data window, or create an empty one from the response records.
   *
   * <p>The method finds the data window specified by the begin time. If no such window is found,
   * the method creates an empty one, put it to the data windows, then returns it.
   *
   * @param windowBeginTime Window begin time
   * @return The list of response records.
   */
  public List<DefaultRecord> getOrCreateDataWindow(Long windowBeginTime) {
    var records = getDataWindows().get(windowBeginTime);
    if (records == null) {
      records = new ArrayList<>();
      getDataWindows().put(windowBeginTime, records);
    }
    return records;
  }

  /** Gets the data windows. */
  public Map<Long, List<DefaultRecord>> getDataWindows() {
    if (dataWindows == null) {
      dataWindows = new TreeMap<>();
    }
    return dataWindows;
  }
}
