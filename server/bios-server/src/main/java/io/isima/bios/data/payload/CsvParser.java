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
package io.isima.bios.data.payload;

import io.isima.bios.errors.EventIngestError;
import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.errors.exception.TooFewValuesException;
import io.isima.bios.errors.exception.TooManyValuesException;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.Event;
import io.isima.bios.models.MissingAttributePolicy;
import io.isima.bios.models.ServerAttributeConfig;
import java.util.List;

public class CsvParser {
  // parser special characters and strings
  private static final char DQUOTE = '"';
  private static final char COMMA = ',';
  private static final String DDQUOTES = "\"\"";
  private static final String SDQUOTE = "\"";

  // parser states
  private static final int NORMAL = 0;
  private static final int ESCAPING = 1;
  private static final int ESCAPING_GOT_DQUOTE = 2;

  public static void parse(String src, List<ServerAttributeConfig> attributes, Event event)
      throws TfosException {
    int state = NORMAL;
    int start = 0;
    int end = -1;
    boolean hasEscapedQuotes = false;
    int iattr = 0;
    for (int i = 0; i <= src.length(); ++i) {
      char current = i < src.length() ? src.charAt(i) : COMMA;
      if (state == ESCAPING) {
        if (current == DQUOTE) {
          state = ESCAPING_GOT_DQUOTE;
          end = i;
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
          if (iattr >= attributes.size()) {
            throw new TooManyValuesException();
          }
          final var attribute = attributes.get(iattr++);
          String value = src.substring(start, end);
          if (hasEscapedQuotes) {
            value = value.replace(DDQUOTES, SDQUOTE);
          }
          setValue(value, attribute, event);
          start = i + 1;
          end = -1;
          hasEscapedQuotes = false;
          break;
        case DQUOTE:
          if (i == start) {
            state = ESCAPING;
            start = i + 1;
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
    if (iattr < attributes.size()) {
      throw new TooFewValuesException();
    }
  }

  private static void setValue(String value, ServerAttributeConfig attribute, Event event)
      throws InvalidValueSyntaxException {
    final String normalized;
    final var type = attribute.getType();
    if (type == AttributeType.STRING) {
      normalized = value;
    } else {
      normalized = value.trim();
    }
    if ((normalized.isEmpty()
            && attribute.getMissingAttributePolicy() == MissingAttributePolicy.STORE_DEFAULT_VALUE)
        && (type != AttributeType.STRING || attribute.getAllowedValues() != null)
        && type != AttributeType.BLOB) {
      // Store default value. DataEngine will fill the missing value in the later stage.
      return;
    }
    final var name = attribute.getName();
    event.set(name, attribute.toInternalValue(normalized));
  }
}
