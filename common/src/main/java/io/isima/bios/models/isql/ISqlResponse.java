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

import io.isima.bios.models.DataWindow;
import io.isima.bios.models.Record;
import java.util.List;

/** Base of the ISql response classes. */
public interface ISqlResponse {

  ISqlResponseType getResponseType();

  /**
   * Records returned based on query.
   *
   * @return all context records in the returned collection
   */
  List<? extends Record> getRecords();

  /**
   * Get all data windows.
   *
   * @return List of data window items.
   */
  List<DataWindow> getDataWindows();

  /**
   * In case of multiExecute, returns the query number that this response belongs to. The response
   * is 0-indexed, i.e. the first query listed in multiExecute corresponds to 0 here.
   */
  default int getRequestQueryNum() {
    return 0;
  }
}
