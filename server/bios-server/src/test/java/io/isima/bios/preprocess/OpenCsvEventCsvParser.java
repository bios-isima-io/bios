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
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180ParserBuilder;
import com.opencsv.exceptions.CsvValidationException;
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

public class OpenCsvEventCsvParser implements EventCsvParser {
  private final CSVReader csvReader;
  private final List<AttributeDesc> attributes;
  private final boolean isKvPair;

  public OpenCsvEventCsvParser(String src, List<AttributeDesc> attributes, boolean isKvPair)
      throws TfosException {
    try {
      final var parser = new RFC4180ParserBuilder().build();
      final Reader reader = CharSource.wrap(src).openStream();
      csvReader = new CSVReaderBuilder(reader).withCSVParser(parser).build();
    } catch (IOException e) {
      throw new TfosException("Failed to initialize the input event parser", e);
    }
    this.attributes = attributes;
    this.isKvPair = isKvPair;
  }

  @Override
  public Map<String, Object> parse() throws TfosException {

    Map<String, Object> attributeValues = new HashMap<>();
    String[] tokens;
    try {
      tokens = csvReader.readNext();
    } catch (CsvValidationException e) {
      throw new TfosException(EventIngestError.CSV_SYNTAX_ERROR, e.getMessage());
    } catch (IOException e) {
      // shouldn't happen
      throw new TfosException("Reading source CSV failed", e);
    }

    if (tokens.length > attributes.size()) {
      throw new TooManyValuesException();
    } else if (tokens.length < attributes.size()) {
      throw new TooFewValuesException();
    }

    final String kvDelim = "" + AdminConstants.DEFAULT_EVENT_KEY_VALUE_DELIMITER;

    for (int i = 0; i < tokens.length; ++i) {
      AttributeDesc descriptor = null;
      final String key;
      final String value;
      if (isKvPair) {
        String[] kv = tokens[i].split(kvDelim, 2);
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
        value = tokens[i];
      }
      attributeValues.put(key, Attributes.convertValue(value, descriptor));
    }
    return attributeValues;
  }
}
