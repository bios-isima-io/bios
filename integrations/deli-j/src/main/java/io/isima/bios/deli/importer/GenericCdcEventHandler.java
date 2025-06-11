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
package io.isima.bios.deli.importer;

import static io.isima.bios.deli.importer.CdcKeywords.AFTER;
import static io.isima.bios.deli.importer.CdcKeywords.BEFORE;
import static io.isima.bios.deli.importer.CdcKeywords.TABLE;

import java.util.Map;

public class GenericCdcEventHandler extends CdcEventHandler {

  @Override
  protected String getTableName(Map<String, ?> source) {
    return (String) source.get(TABLE);
  }

  @Override
  protected Map<String, Object> retrieveBeforeData(Map<String, Object> sourceData) {
    return (Map<String, Object>) sourceData.get(BEFORE);
  }

  @Override
  protected Map<String, Object> retrieveAfterData(Map<String, Object> sourceData) {
    return (Map<String, Object>) sourceData.get(AFTER);
  }
}
