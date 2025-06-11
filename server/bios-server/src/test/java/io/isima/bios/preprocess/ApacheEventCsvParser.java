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
package io.isima.bios.preprocess;

import com.google.common.io.CharSource;
import io.isima.bios.admin.v1.AdminConstants;
import io.isima.bios.errors.EventIngestError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.TooFewValuesException;
import io.isima.bios.errors.exception.TooManyValuesException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ApacheEventCsvParser implements EventCsvParser {
  private static final CSVFormat format = CSVFormat.RFC4180;
  private final CSVParser csvParser;
  private final List<AttributeDesc> attributes;
  private final boolean isKvPair;

  public ApacheEventCsvParser(String src, List<AttributeDesc> attributes, boolean isKvPair)
      throws TfosException {
    try {
      final Reader reader = CharSource.wrap(src).openStream();
      csvParser = format.parse(reader);
    } catch (IOException e) {
      throw new TfosException("Failed to initialize the input event parser", e);
    }
    this.attributes = attributes;
    this.isKvPair = isKvPair;
  }

  @Override
  public Map<String, Object> parse() throws TfosException {

    Map<String, Object> attributeValues = new HashMap<>();
    CSVRecord records;
    try {
      records = csvParser.getRecords().get(0);
    } catch (IOException e) {
      throw new TfosException(
          EventIngestError.CSV_SYNTAX_ERROR, "Reading source CSV failed: " + e.getMessage(), e);
    }

    if (records.size() > attributes.size()) {
      throw new TooManyValuesException();
    } else if (records.size() < attributes.size()) {
      throw new TooFewValuesException();
    }

    final String kvDelim = "" + AdminConstants.DEFAULT_EVENT_KEY_VALUE_DELIMITER;

    for (int i = 0; i < records.size(); ++i) {
      AttributeDesc descriptor = null;
      final String key;
      final String value;
      if (isKvPair) {
        String[] kv = records.get(i).split(kvDelim, 2);
        if (kv.length != 2) {
          throw new TfosException(EventIngestError.KEYVALUE_SYNTAX_ERROR);
        }
        key = kv[0];
        value = kv[1];

        // Check whether the attribute name exists
        // TODO(Naoki): Inefficient
        descriptor = null;
        for (AttributeDesc inListEntry : this.attributes) {
          if (inListEntry.getName().equalsIgnoreCase(key)) {
            descriptor = inListEntry;
            break;
          }
        }
        if (descriptor == null) {
          throw new TfosException(EventIngestError.UNKNOWN_KEY);
        }

      } else {
        descriptor = attributes.get(i);
        key = descriptor.getName();
        value = records.get(i);
      }
      attributeValues.put(key, Attributes.convertValue(value, descriptor));
    }
    return attributeValues;
  }
}
