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
package io.isima.bios.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.TimeZone;

public class StringUtils {

  private static final ObjectMapper mapper = BiosObjectMapperProvider.get();
  private static final long microsecond = 1000;
  private static final long millisecond = 1000 * microsecond;
  private static final long second = 1000 * millisecond;
  private static final long minute = 60 * second;
  private static final long hour = 60 * minute;
  private static final long day = 24 * hour;

  public static String getJsonStringFromObject(Object jsonObject) throws JsonProcessingException {
    return getJsonStringFromObject(jsonObject, true);
  }

  public static String getJsonStringFromObject(Object jsonObject, boolean indent)
      throws JsonProcessingException {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, indent);
    return mapper.writeValueAsString(jsonObject);
  }

  /**
   * Convert Unix time to a string with ISO-8601 format.
   *
   * @param time Unix time in milliseconds
   * @return String timestamp in ISO-8601 format.
   */
  public static String tsToIso8601(long time) {
    return tsToReadableTime(time, "yyyy-MM-dd'T'HH:mm:ss");
  }

  /** Convert Unix time to a string with ISO-8601 format including milliseconds. */
  public static String tsToIso8601Millis(long time) {
    return tsToReadableTime(time, "yyyy-MM-dd'T'HH:mm:ss.sss");
  }

  /**
   * Convert Unix time to a string with readable time format.
   *
   * @param time Unix time in milliseconds
   * @param format Format for printing the time.
   * @return String timestamp in requested format.
   */
  public static String tsToReadableTime(long time, final String format) {
    return dateToIso8601(new Date(time), format);
  }

  /**
   * Convert a Date object to a string with ISO-8601 format.
   *
   * @param date The date to convert.
   * @return String timestamp in ISO-8601 format.
   */
  public static String dateToIso8601(Date date, final String format) {
    SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.getDefault());
    sdf.setTimeZone(TimeZone.getDefault());
    return sdf.format(date);
  }

  /**
   * Convert a time duration in milliseconds to a short readable string. The goal is for the string
   * to be as short as possible, by skipping elements from both sides of the format wherever
   * possible. E.g. if the duration is a multiple of 1000 then milliseconds can be skipped; if it is
   * a multiple of 60,000 then seconds and milliseconds can be skipped; if it is less than an hour,
   * then days and hours can be skipped.
   *
   * @param durationInMs Duration in milliseconds
   */
  public static String shortReadableDuration(long durationInMs) {
    return shortReadableDurationNanos(durationInMs * 1000000L);
  }

  /**
   * Convert a time duration in milliseconds to a short readable string. The goal is for the string
   * to be as short as possible, by skipping elements from both sides of the format wherever
   * possible. E.g. if the duration is a multiple of 1000 then nanoseconds can be skipped; if it is
   * a multiple of 60,000,000,000 then seconds, milliseconds, microseconds, and nanoseconds can be
   * skipped; if it is less than an hour, then days and hours can be skipped.
   *
   * @param durationInNanos Duration in nanoSeconds
   */
  public static String shortReadableDurationNanos(long durationInNanos) {
    final var sb = new StringBuilder();

    if (durationInNanos == 0) {
      return "0";
    }
    if (durationInNanos < 0) {
      sb.append("-");
      durationInNanos = durationInNanos * (-1);
    }

    long remainingDuration = durationInNanos;
    final long days = remainingDuration / day;
    remainingDuration -= days * day;
    final long hours = remainingDuration / hour;
    remainingDuration -= hours * hour;
    final long minutes = remainingDuration / minute;
    remainingDuration -= minutes * minute;
    final long seconds = remainingDuration / second;
    remainingDuration -= seconds * second;
    final long milliseconds = remainingDuration / millisecond;
    remainingDuration -= milliseconds * millisecond;
    final long microseconds = remainingDuration / microsecond;
    remainingDuration -= microseconds * microsecond;
    final long nanoseconds = remainingDuration;
    boolean startedPrinting = false;
    String prefix;
    String format;
    String unit = null;

    while (true) {
      // This executes only once. Using 'while' in order to be able to use "break" statement.
      if (days > 0) {
        sb.append(days);
        if (unit == null) {
          unit = "days";
        }
        startedPrinting = true;
        if ((durationInNanos % day) == 0) {
          break;
        }
      }
      if (startedPrinting || (hours > 0)) {
        prefix = startedPrinting ? ":" : "";
        format = startedPrinting ? "%02d" : "%d";
        sb.append(prefix).append(String.format(format, hours));
        unit = (unit == null) ? "hrs" : unit;
        startedPrinting = true;
        if ((durationInNanos % hour) == 0) {
          break;
        }
      }
      if (startedPrinting || (minutes > 0)) {
        prefix = startedPrinting ? ":" : "";
        format = startedPrinting ? "%02d" : "%d";
        sb.append(prefix).append(String.format(format, minutes));
        unit = (unit == null) ? "mins" : unit;
        startedPrinting = true;
        if ((durationInNanos % minute) == 0) {
          break;
        }
      }
      if (startedPrinting || (seconds > 0)) {
        prefix = startedPrinting ? ":" : "";
        format = startedPrinting ? "%02d" : "%d";
        sb.append(prefix).append(String.format(format, seconds));
        unit = (unit == null) ? "secs" : unit;
        startedPrinting = true;
        if ((durationInNanos % second) == 0) {
          break;
        }
      }
      if (startedPrinting || (milliseconds > 0)) {
        prefix = startedPrinting ? "." : "";
        format = startedPrinting ? "%03d" : "%d";
        sb.append(prefix).append(String.format(format, milliseconds));
        unit = (unit == null) ? "ms" : unit;
        startedPrinting = true;
        if ((durationInNanos % millisecond) == 0) {
          break;
        }
      }
      if (startedPrinting || (microseconds > 0)) {
        prefix = startedPrinting ? "." : "";
        format = startedPrinting ? "%03d" : "%d";
        sb.append(prefix).append(String.format(format, microseconds));
        unit = (unit == null) ? "micros" : unit;
        startedPrinting = true;
        if ((durationInNanos % microsecond) == 0) {
          break;
        }
      }
      if (startedPrinting || (nanoseconds > 0)) {
        prefix = startedPrinting ? "." : "";
        format = startedPrinting ? "%03d" : "%d";
        sb.append(prefix).append(String.format(format, nanoseconds));
        unit = (unit == null) ? "ns" : unit;
        startedPrinting = true;
      }
      assert (startedPrinting && (unit != null));
      break;
    }

    sb.append(" ").append(unit);

    return sb.toString();
  }

  /**
   * Splits the input into constituent words and converts them to lowercase. This is meant to work
   * for camelCase, PascalCase, and snake_case. Not including kebab-case and dot.case since we do
   * not support dots and dashes in biOS names.
   */
  public static List<String> splitIntoWords(String input) {
    final var sb = new StringBuilder();
    boolean previousCharWasLowercase = true;
    // Build a new string that contains all the alphabetic characters of the original, with the
    // underscores and digits replaced by spaces, and additional spaces introduced when the case
    // changed from lowercase to uppercase.
    for (int i = 0; i < input.length(); i++) {
      final char ch = input.charAt(i);
      if (Character.isDigit(ch) || (ch == '_')) {
        sb.append(" ");
        continue;
      }
      if (Character.isUpperCase(ch) && previousCharWasLowercase) {
        sb.append(" ");
      }
      if (Character.isUpperCase(ch)) {
        previousCharWasLowercase = false;
      }
      if (Character.isLowerCase(ch)) {
        previousCharWasLowercase = true;
      }
      sb.append(ch);
    }

    final var wordArray = sb.toString().toLowerCase().split(" ");
    final var wordList = new ArrayList<>(Arrays.asList(wordArray));
    wordList.removeAll(List.of("", " "));
    return wordList;
  }

  /**
   * Add a prefix to a camel-case string that makes a camel case string with the prefix.
   *
   * @param prefix The prefix
   * @param camelCaseString The string to be prefixed to
   * @return Created string
   * @throws NullPointerException thrown when any of the parameters is null
   * @throws IllegalArgumentException thrown when the prefix or the string is empty
   */
  public static String prefixToCamelCase(String prefix, String camelCaseString) {
    Objects.requireNonNull(camelCaseString);
    Objects.requireNonNull(prefix);
    if (prefix.isEmpty()) {
      throw new IllegalArgumentException("The prefix may not be empty");
    }
    if (camelCaseString.isEmpty()) {
      throw new IllegalArgumentException("The camelCase string may not be empty");
    }
    return prefix + camelCaseString.substring(0, 1).toUpperCase() + camelCaseString.substring(1);
  }

  /**
   * Creates a concrete path from a path template.
   *
   * <p>The method finds keywords of format '{key}' in the template and replace them by specified
   * parameters.
   *
   * <p>The keyword names so not matter. The appeared keywords are replaced by the parameters in
   * specified order. For example, for a path template "/tenant/{tenant}/signal/{signal}" with
   * parameters ["abc", "123"], the created path would be "/tenant/abc/signal/123".
   *
   * <p>An invalid request, such as syntax error in template, lack of parameters, are handled in
   * best-effort manner, this method
   *
   * @param pathTemplate Path template with keywords of format "{key}"
   * @param params Parameters to replace keywords
   * @return Generated concrete path
   * @throws NullPointerException thrown to indicate that pathTemplate is null
   */
  public static String generatePath(String pathTemplate, String... params) {
    Objects.requireNonNull(pathTemplate);
    final var sb = new StringBuilder();
    boolean inKeyword = false;
    int paramIndex = 0;
    for (int i = 0; i < pathTemplate.length(); ++i) {
      final char ch = pathTemplate.charAt(i);
      if (inKeyword) {
        if (ch == '}') {
          inKeyword = false;
        }
      } else if (ch == '{') {
        final var param = paramIndex < params.length ? params[paramIndex++] : null;
        if (param == null) {
          sb.append(pathTemplate.substring(i));
          break;
        }
        sb.append(param);
        inKeyword = true;
      } else {
        sb.append(ch);
      }
    }
    return sb.toString();
  }
}
