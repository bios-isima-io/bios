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

import io.isima.bios.admin.v1.AdminConstants;
import io.isima.bios.errors.EventIngestError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.TooFewValuesException;
import io.isima.bios.errors.exception.TooManyValuesException;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.Attributes;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DefaultEventCsvParser implements EventCsvParser {
  // parser special characters and strings
  private static final char DQUOTE = '"';
  private static final char COMMA = ',';
  private static final String DDQUOTES = "\"\"";
  private static final String SDQUOTE = "\"";

  private static final char KV_DELIM = AdminConstants.DEFAULT_EVENT_KEY_VALUE_DELIMITER;

  // parser states
  private static final int NORMAL = 0;
  private static final int ESCAPING = 1;
  private static final int ESCAPING_GOT_DQUOTE = 2;

  private final String src;
  private final List<AttributeDesc> attributes;
  private final boolean isKvPair;

  public DefaultEventCsvParser(String src, List<AttributeDesc> attributes, boolean isKvPair) {
    this.src = src;
    this.attributes = attributes;
    this.isKvPair = isKvPair;
  }

  @Override
  public Map<String, Object> parse() throws TfosException {
    Iterator<AttributeDesc> iter = this.attributes.iterator();

    Map<String, Object> attributeValues = new HashMap<>();
    int state = NORMAL;
    int kvsplit = -1;
    int start = 0;
    int end = -1;
    boolean hasEscapedQuotes = false;
    String key;
    String value;
    AttributeDesc descriptor;
    for (int i = 0; i <= src.length(); ++i) {
      char current = i < src.length() ? src.charAt(i) : COMMA;
      if (state == ESCAPING) {
        if (current == DQUOTE) {
          state = ESCAPING_GOT_DQUOTE;
          end = i;
        } else if (current == KV_DELIM && isKvPair && kvsplit < 0) {
          kvsplit = i;
        }
        continue;
      } else if (state == ESCAPING_GOT_DQUOTE) {
        if (current == DQUOTE) {
          state = ESCAPING;
          end = -1;
          hasEscapedQuotes = true;
          continue;
        } else {
          state = NORMAL;
        }
      }
      switch (current) {
        case COMMA:
          if (end == -1) {
            end = i;
          }
          if (kvsplit < 0) {
            if (isKvPair) {
              throw new TfosException(EventIngestError.KEYVALUE_SYNTAX_ERROR);
            }
            if (!iter.hasNext()) {
              throw new TooManyValuesException();
            }
            descriptor = iter.next();
            key = descriptor.getName();
            value = src.substring(start, end);
            if (hasEscapedQuotes) {
              value = value.replace(DDQUOTES, SDQUOTE);
            }
          } else {
            key = src.substring(start, kvsplit);
            value = src.substring(kvsplit + 1, end);
            if (hasEscapedQuotes) {
              value = value.replace(DDQUOTES, SDQUOTE);
            }
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
            key = descriptor.getName(); // replace instance of the key by one from the descriptor
          }
          attributeValues.put(key, Attributes.convertValue(value, descriptor));
          start = i + 1;
          end = -1;
          kvsplit = -1;
          hasEscapedQuotes = false;
          break;
        case DQUOTE:
          if (i == start) {
            state = ESCAPING;
            start = i + 1;
          }
          break;
        case KV_DELIM:
          if (isKvPair && kvsplit < 0) {
            kvsplit = i;
          }
          break;
        default:
          break;
      }
    }
    if (state == ESCAPING) {
      throw new TfosException(
          EventIngestError.CSV_SYNTAX_ERROR, "EOF reached before encapsulated token finished");
    }
    if (!isKvPair && iter.hasNext()) {
      throw new TooFewValuesException();
    }
    return attributeValues;
  }
}
