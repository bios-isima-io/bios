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
package io.isima.bios.bi.teachbios.typedetector;

import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.DOUBLE;
import static io.isima.bios.models.v1.InternalAttributeType.INT;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;

import io.isima.bios.models.v1.InternalAttributeType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
This class attempts to typecast a given stream of values to a fixed set of types and records the
number of successful typecasts. It selects the appropriate type after matching values in a
specific order of the type list
 */

public class TypeCastBasedTypeDetector extends TypeMatcher implements TypeDetector {

  /*
  This map holds the count of successful type casts against the corresponding type
   */
  private Map<InternalAttributeType, Integer> matchCountMap;

  public TypeCastBasedTypeDetector() {
    matchCountMap = new HashMap<>();
  }

  @Override
  public boolean tryParseAsInteger(String value) {
    try {
      Integer.parseInt(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  public boolean tryParseAsDouble(String value) {
    try {
      Double.parseDouble(value);
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  @Override
  public boolean tryParseAsBoolean(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  @Override
  public InternalAttributeType inferDataType(List<String> columnValues) {
    resetMatchCountMap();
    int emptyCount = 0;
    for (String value : columnValues) {
      if (value == null || value.isEmpty()) {
        emptyCount += 1;
      } else if (tryParseAsInteger(value)) {
        incrementTypeCount(INT);
        incrementTypeCount(DOUBLE);
      } else if (tryParseAsDouble(value)) {
        incrementTypeCount(DOUBLE);
      } else if (tryParseAsBoolean(value)) {
        incrementTypeCount(BOOLEAN);
      } else {
        incrementTypeCount(STRING);
      }
    }

    if (matchCountMap.get(INT) + emptyCount == columnValues.size()) {
      return INT;
    } else if (matchCountMap.get(DOUBLE) + emptyCount == columnValues.size()) {
      return DOUBLE;
    } else if (matchCountMap.get(BOOLEAN) + emptyCount == columnValues.size()) {
      return BOOLEAN;
    }
    return STRING;
  }

  private void incrementTypeCount(InternalAttributeType type) {
    matchCountMap.put(type, matchCountMap.get(type) + 1);
  }

  private void resetMatchCountMap() {
    for (InternalAttributeType type : InternalAttributeType.values()) {
      matchCountMap.put(type, 0);
    }
  }
}
