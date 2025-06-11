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
package io.isima.bios.bi.teachbios;

import com.google.common.io.CharSource;
import io.isima.bios.bi.teachbios.namedetector.NameDetector;
import io.isima.bios.bi.teachbios.typedetector.TypeDetector;
import io.isima.bios.dto.teachbios.LearningData;
import io.isima.bios.errors.TeachBiosError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.models.TeachBiosResponse;
import io.isima.bios.models.v1.AttributeLearned;
import io.isima.bios.models.v1.InternalAttributeType;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TeachBios {
  private static final CSVFormat format = CSVFormat.RFC4180;
  private static final int MAX_ROWS = 1000;
  private static final int MAX_COLUMNS = 1000;

  private static final Logger logger = LoggerFactory.getLogger(TeachBios.class);

  private final TypeDetector typeDetector;
  private final NameDetector nameDetector;

  public TeachBios(TypeDetector typeDetector, NameDetector nameDetector) {
    this.typeDetector = typeDetector;
    this.nameDetector = nameDetector;
  }

  public TeachBiosResponse learn(LearningData learningData) throws TfosException {
    final var inferredCountMap = new HashMap<String, Integer>();
    List<List<String>> columnValuesList = parseInput(learningData);
    return inferSchema(columnValuesList, inferredCountMap);
  }

  /**
   * Method which handles the validation of teach bi(OS) input.
   *
   * @param learningData Input learning data
   * @return list of values grouped by columns in input
   * @throws TfosException thrown to indicate a user error happened.
   */
  protected List<List<String>> parseInput(LearningData learningData) throws TfosException {
    final String src = learningData.getRows();
    if (src == null || src.isBlank()) {
      throw new TfosException(TeachBiosError.EMPTY_INPUT_NOW_ALLOWED);
    }
    try {
      final Reader reader = CharSource.wrap(src).openStream();
      final CSVParser csvParser = format.parse(reader);
      final var records = csvParser.getRecords();
      if (records.size() > MAX_ROWS) {
        throw new TfosException(TeachBiosError.MAX_ROW_LIMIT_EXCEEDED);
      }
      final var first = records.get(0);
      if (first.size() > MAX_COLUMNS) {
        throw new TfosException(TeachBiosError.MAX_COLUMN_LIMIT_EXCEEDED);
      }
      // pivot
      final var results = new ArrayList<List<String>>(first.size());
      for (int i = 0; i < first.size(); ++i) {
        results.add(new ArrayList<>(records.size()));
      }
      for (int irow = 0; irow < records.size(); ++irow) {
        final var record = records.get(irow);
        if (record.size() != first.size()) {
          throw new TfosException(
              TeachBiosError.INVALID_INPUT,
              String.format("row[%d]: Inconsistent number of columns", irow));
        }
        for (int icol = 0; icol < record.size(); ++icol) {
          final String column = record.get(icol);
          if (column.isBlank()) {
            throw new TfosException(TeachBiosError.EMPTY_INPUT_NOW_ALLOWED);
          }
          results.get(icol).add(record.get(icol));
        }
      }
      return results;
    } catch (IOException e) {
      throw new TfosException(TeachBiosError.INVALID_INPUT, e.getMessage());
    }
  }

  private TeachBiosResponse inferSchema(
      List<List<String>> columnValuesList, Map<String, Integer> inferredCountMap) {
    final TeachBiosResponse teachBiosResponse = new TeachBiosResponse();
    List<AttributeLearned> inferredSchemaList = new ArrayList<>();
    for (int i = 0; i < columnValuesList.size(); i++) {
      logger.debug(
          "Trying to infer type and name of column with values "
              + Arrays.toString(columnValuesList.get(i).toArray()));
      List<String> columnValues = columnValuesList.get(i);
      InternalAttributeType columnType = this.typeDetector.inferDataType(columnValues);
      // Domain retail is hardcoded for now. This will be taken as input in API request
      String columnName = this.nameDetector.inferColumnName(columnValues, "retail");
      String qualifiedColumnName = getQualifiedColumnName(columnName, inferredCountMap);
      AttributeLearned inferredSchema =
          new AttributeLearned(qualifiedColumnName, columnType, columnValues);
      inferredSchemaList.add(inferredSchema);
    }
    teachBiosResponse.setAttributes(inferredSchemaList);
    return teachBiosResponse;
  }

  private String getQualifiedColumnName(String columnName, Map<String, Integer> inferredCountMap) {
    Integer count = inferredCountMap.getOrDefault(columnName, 0);
    String result = columnName;
    if (count > 0) {
      result = columnName + "_" + count.toString();
    }
    inferredCountMap.put(columnName, count + 1);
    return result;
  }
}
