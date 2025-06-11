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
package io.isima.bios.data.impl.feature;

import com.google.common.net.InetAddresses;
import io.isima.bios.errors.exception.NullAttributeException;
import io.isima.bios.models.Event;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.net.InetAddress;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

/** Class to provide comparators that are useful in DataEngine implementation. */
public class Comparators {

  /** Comparator used for sorting events by ingestTimestamp. */
  public static final Comparator<Event> TIMESTAMP =
      new Comparator<>() {
        @Override
        public int compare(Event e1, Event e2) {
          long t1 = e1.getIngestTimestamp().getTime();
          long t2 = e2.getIngestTimestamp().getTime();
          if (t1 > t2) {
            return 1;
          } else if (t1 == t2) {
            return 0;
          } else {
            return -1;
          }
        }
      };

  static final Comparator<Event> REVERSE_TIMESTAMP =
      new Comparator<>() {
        @Override
        public int compare(Event e1, Event e2) {
          return -TIMESTAMP.compare(e1, e2);
        }
      };

  /** Comparator used for sorting primary key. */
  public static final Comparator<List<Object>> CONTEXT_PRIMARY_KEY =
      new Comparator<>() {
        @Override
        public int compare(List<Object> o1, List<Object> o2) {
          final var size = Math.min(o1.size(), o2.size());
          for (int i = 0; i < size; ++i) {
            Comparable<Object> v1 = (Comparable<Object>) o1.get(i);
            Comparable<Object> v2 = (Comparable<Object>) o2.get(i);
            final int comparison = v1.compareTo(v2);
            if (comparison != 0) {
              return comparison;
            }
          }
          return o1.size() - o2.size();
        }
      };

  /**
   * Generate an event comparator by the specified attribute.
   *
   * @param desc Description of the attribute to compare.
   * @param polarity 1 or -1; Give the negative value when comparing the values reversely.
   * @param caseInsensitive Specify true when doing case insensitive string comparison
   * @return Generated event comparator.
   */
  public static Comparator<Event> generateComparator(
      AttributeDesc desc, int polarity, boolean caseInsensitive) {
    return generateComparator(desc.getName(), desc.getAttributeType(), polarity, caseInsensitive);
  }

  public static Comparator<Event> generateComparator(
      String key, AttributeDesc desc, int polarity, boolean caseInsensitive) {
    return generateComparator(key, desc.getAttributeType(), polarity, caseInsensitive);
  }

  public static Comparator<Event> generateComparator(
      String key, InternalAttributeType type, int polarity, boolean caseInsensitive) {
    if (type == InternalAttributeType.STRING && caseInsensitive) {
      return new Comparator<>() {
        @Override
        public int compare(Event event1, Event event2) {
          String value1 = (String) event1.get(key);
          String value2 = (String) event2.get(key);
          return value1.compareToIgnoreCase(value2) * polarity;
        }
      };
    } else if (type == InternalAttributeType.INET) {
      return new Comparator<>() {
        @Override
        public int compare(Event event1, Event event2) {
          Integer value1 = InetAddresses.coerceToInteger((InetAddress) event1.get(key));
          Integer value2 = InetAddresses.coerceToInteger((InetAddress) event2.get(key));
          Long val1 = 0xffffffffL & value1;
          Long val2 = 0xffffffffL & value2;
          return val1.compareTo(val2) * polarity;
        }
      };
    } else {
      return new Comparator<>() {
        @Override
        public int compare(Event event1, Event event2) {
          Comparable<Object> value1 = (Comparable<Object>) event1.get(key);
          Comparable<Object> value2 = (Comparable<Object>) event2.get(key);
          if (value1 == null || value2 == null) {
            throw new NullAttributeException(
                String.format(
                    "Attribute not found. This is supposed not to happen; key=%s, event1=%s, event2=%s",
                    key, event1, event2));
          }
          return value1.compareTo(value2) * polarity;
        }
      };
    }
  }

  public static Comparator<Map<String, Object>> generateMapComparator(
      String key, InternalAttributeType type, int polarity, boolean caseInsensitive) {
    if (type == InternalAttributeType.STRING && caseInsensitive) {
      return new Comparator<>() {
        @Override
        public int compare(Map<String, Object> event1, Map<String, Object> event2) {
          String value1 = (String) event1.get(key);
          String value2 = (String) event2.get(key);
          return value1.compareToIgnoreCase(value2) * polarity;
        }
      };
    } else {
      return new Comparator<>() {
        @Override
        public int compare(Map<String, Object> event1, Map<String, Object> event2) {
          Comparable<Object> value1 = (Comparable<Object>) event1.get(key);
          Comparable<Object> value2 = (Comparable<Object>) event2.get(key);
          if (value1 == null || value2 == null) {
            throw new NullAttributeException(
                String.format(
                    "Attribute not found. This is supposed not to happen; key=%s, event1=%s, event2=%s",
                    key, event1, event2));
          }
          return value1.compareTo(value2) * polarity;
        }
      };
    }
  }

  public static BiFunction<Object, Object, Object> generateMinFunction(
      InternalAttributeType type, boolean caseInsensitive) {
    if (type == InternalAttributeType.STRING && caseInsensitive) {
      return (value1, value2) -> {
        if (value1 == null) {
          return value2;
        }
        if (value2 == null) {
          return value1;
        }
        return ((String) value1).compareToIgnoreCase((String) value2) < 0 ? value1 : value2;
      };
    }

    return (v1, v2) -> {
      if (v1 == null) {
        return v2;
      }
      if (v2 == null) {
        return v1;
      }
      final var value1 = (Comparable<Object>) v1;
      final var value2 = (Comparable<Object>) v2;
      return value1.compareTo(value2) < 0 ? v1 : v2;
    };
  }

  public static BiFunction<Object, Object, Object> generateMaxFunction(
      InternalAttributeType type, boolean caseInsensitive) {
    if (type == InternalAttributeType.STRING && caseInsensitive) {
      return (value1, value2) -> {
        if (value1 == null) {
          return value2;
        }
        if (value2 == null) {
          return value1;
        }
        return ((String) value1).compareToIgnoreCase((String) value2) > 0 ? value1 : value2;
      };
    }

    return (v1, v2) -> {
      if (v1 == null) {
        return v2;
      }
      if (v2 == null) {
        return v1;
      }
      final var value1 = (Comparable<Object>) v1;
      final var value2 = (Comparable<Object>) v2;
      return value1.compareTo(value2) > 0 ? v1 : v2;
    };
  }
}
