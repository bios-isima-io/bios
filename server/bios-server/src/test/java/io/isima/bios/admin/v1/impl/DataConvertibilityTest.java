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
package io.isima.bios.admin.v1.impl;

import static io.isima.bios.models.v1.InternalAttributeType.BLOB;
import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.DATE;
import static io.isima.bios.models.v1.InternalAttributeType.DOUBLE;
import static io.isima.bios.models.v1.InternalAttributeType.ENUM;
import static io.isima.bios.models.v1.InternalAttributeType.INET;
import static io.isima.bios.models.v1.InternalAttributeType.INT;
import static io.isima.bios.models.v1.InternalAttributeType.LONG;
import static io.isima.bios.models.v1.InternalAttributeType.NUMBER;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;
import static io.isima.bios.models.v1.InternalAttributeType.TIMESTAMP;
import static io.isima.bios.models.v1.InternalAttributeType.UUID;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.datastax.driver.core.LocalDate;
import com.google.common.net.InetAddresses;
import io.isima.bios.common.TfosUtils;
import io.isima.bios.errors.exception.FileReadException;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DataConvertibilityTest {

  private static List<Object> list(Object... items) {
    return Arrays.asList(items);
  }

  private static java.util.UUID uuid(String src) {
    return java.util.UUID.fromString(src);
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(
        // to, from, isConvertible, expectedConversion, conversionSrc
        new Object[][] {
          // to string
          {STRING, STRING, TRUE, "Hello", "Hello"},
          {STRING, BOOLEAN, TRUE, list("true", "false"), list(TRUE, FALSE)},
          {STRING, INT, TRUE, "94061", 94061},
          {STRING, LONG, TRUE, "6507226550", 6507226550L},
          {
            STRING,
            NUMBER,
            TRUE,
            "7839439356501333078394393565013330",
            new BigInteger("7839439356501333078394393565013330")
          },
          {STRING, DOUBLE, TRUE, "3.141592653", Double.valueOf(3.141592653)},
          {STRING, INET, TRUE, "101.23.4.50", InetAddresses.forString("101.23.4.50")},
          {STRING, DATE, TRUE, "2001-02-10", LocalDate.fromDaysSinceEpoch(11363)},
          {STRING, TIMESTAMP, TRUE, "981763200000", new Date(981763200000L)},
          {
            STRING,
            UUID,
            TRUE,
            list("01ffd65e-3572-11e9-b210-d663bd873d93", "c09e53e1-9f40-4f8d-b520-a28b989387ee"),
            list(
                uuid("01ffd65e-3572-11e9-b210-d663bd873d93"),
                uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee"))
          },
          {STRING, ENUM, TRUE, "FISH", 0},
          {STRING, BLOB, FALSE, null, ByteBuffer.allocate(10)},
          // to boolean
          {BOOLEAN, STRING, FALSE, null, "dummy"},
          {BOOLEAN, BOOLEAN, TRUE, list(TRUE, FALSE), list(TRUE, FALSE)},
          {BOOLEAN, INT, FALSE, null, 10},
          {BOOLEAN, LONG, FALSE, null, 100L},
          {BOOLEAN, NUMBER, FALSE, null, new BigInteger("0")},
          {BOOLEAN, DOUBLE, FALSE, null, Double.valueOf(1.1)},
          {BOOLEAN, INET, FALSE, null, InetAddresses.forString("0.0.0.0")},
          {BOOLEAN, DATE, FALSE, null, LocalDate.fromMillisSinceEpoch(System.currentTimeMillis())},
          {BOOLEAN, TIMESTAMP, FALSE, null, new Date(System.currentTimeMillis())},
          {BOOLEAN, UUID, FALSE, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          {BOOLEAN, ENUM, FALSE, null, 1},
          {BOOLEAN, BLOB, FALSE, null, ByteBuffer.allocate(1)},
          // to INT
          {INT, STRING, FALSE, null, list("", "abc", "123")},
          {INT, BOOLEAN, TRUE, list(0, 1), list(FALSE, TRUE)},
          {INT, INT, TRUE, list(10, 100, 1000), list(10, 100, 1000)},
          {INT, LONG, FALSE, null, 1000L},
          {INT, NUMBER, FALSE, null, new BigInteger("0")},
          {INT, DOUBLE, FALSE, null, Double.valueOf(3.33)},
          {INT, INET, FALSE, null, InetAddresses.forString("10.10.10.10")},
          {INT, DATE, TRUE, 11363, LocalDate.fromYearMonthDay(2001, 02, 10)},
          {INT, TIMESTAMP, FALSE, null, new Date(System.currentTimeMillis())},
          {INT, UUID, FALSE, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          {INT, ENUM, FALSE, null, 2},
          {INT, BLOB, FALSE, null, ByteBuffer.allocate(0)},
          // to LONG
          {LONG, STRING, FALSE, null, list("0", "123", "abc")},
          {LONG, BOOLEAN, TRUE, list(1L, 0L), list(TRUE, FALSE)},
          {LONG, INT, TRUE, 2340054L, 2340054},
          {LONG, LONG, TRUE, 6507226550L, 6507226550L},
          {LONG, NUMBER, FALSE, null, new BigInteger("0")},
          {LONG, DOUBLE, FALSE, null, Double.valueOf(1.11)},
          {LONG, INET, FALSE, null, InetAddresses.forString("10.10.10.10")},
          {LONG, DATE, TRUE, 11363L, LocalDate.fromYearMonthDay(2001, 02, 10)},
          {LONG, TIMESTAMP, TRUE, 1550729155000L, new Date(1550729155000L)},
          {LONG, UUID, FALSE, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          {LONG, ENUM, FALSE, null, 3},
          {LONG, BLOB, FALSE, null, ByteBuffer.allocate(1024)},
          // to NUMBER
          {NUMBER, STRING, FALSE, null, "123"},
          {NUMBER, BOOLEAN, TRUE, list(BigInteger.ONE, BigInteger.ZERO), list(TRUE, FALSE)},
          {
            NUMBER,
            INT,
            TRUE,
            list(new BigInteger("12345"), new BigInteger("-23456779")),
            list(12345, -23456779)
          },
          {
            NUMBER,
            LONG,
            TRUE,
            list(new BigInteger("12345"), new BigInteger("-6507226550")),
            list(12345L, -6507226550L)
          },
          {
            NUMBER,
            NUMBER,
            TRUE,
            new BigInteger("7839439356501333078394393565013330"),
            new BigInteger("7839439356501333078394393565013330")
          },
          {NUMBER, DOUBLE, FALSE, null, Double.valueOf(0.1)},
          {NUMBER, INET, FALSE, null, InetAddresses.forString("10.10.10.10")},
          {NUMBER, DATE, TRUE, new BigInteger("123"), LocalDate.fromDaysSinceEpoch(123)},
          {NUMBER, TIMESTAMP, TRUE, new BigInteger("981763200000"), new Date(981763200000L)},
          {
            NUMBER,
            UUID,
            TRUE,
            new BigInteger("-84248507510966580807370813538676013074"),
            uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")
          },
          {
            NUMBER, ENUM, FALSE, null, 4,
          },
          {NUMBER, BLOB, FALSE, null, ByteBuffer.allocateDirect(1024)},
          // to DOUBLE
          {DOUBLE, STRING, FALSE, null, "1.23"},
          {DOUBLE, BOOLEAN, FALSE, null, TRUE},
          {DOUBLE, INT, TRUE, 879.0, 879},
          {DOUBLE, LONG, TRUE, 6507226550.0, 6507226550L},
          {
            DOUBLE,
            NUMBER,
            TRUE,
            7839439356501333078394393565013330.0,
            new BigInteger("7839439356501333078394393565013330")
          },
          {DOUBLE, DOUBLE, TRUE, 12.456, 12.456},
          {DOUBLE, INET, FALSE, null, InetAddresses.forString("10.10.10.10")},
          {DOUBLE, DATE, FALSE, null, LocalDate.fromDaysSinceEpoch(123)},
          {DOUBLE, TIMESTAMP, FALSE, null, new Date(981763200000L)},
          {DOUBLE, UUID, FALSE, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          {DOUBLE, ENUM, FALSE, null, 5},
          {DOUBLE, BLOB, FALSE, null, ByteBuffer.allocate(100)},
          // to INET
          {INET, STRING, FALSE, null, "10.20.30.40"},
          {INET, BOOLEAN, FALSE, null, TRUE},
          {INET, INT, TRUE, InetAddresses.forString("10.11.12.13"), 0x0a0b0c0d},
          {INET, LONG, FALSE, null, 10L},
          {INET, NUMBER, FALSE, null, BigInteger.TEN},
          {INET, DOUBLE, FALSE, null, 8.99},
          {
            INET,
            INET,
            TRUE,
            InetAddresses.forString("10.10.10.10"),
            InetAddresses.forString("10.10.10.10")
          },
          {INET, DATE, FALSE, null, LocalDate.fromDaysSinceEpoch(123)},
          {INET, TIMESTAMP, FALSE, null, new Date(981763200000L)},
          {INET, UUID, FALSE, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          {INET, ENUM, FALSE, null, 6},
          {INET, BLOB, FALSE, null, ByteBuffer.allocate(100)},
          // to DATE
          {DATE, STRING, FALSE, null, "2012-02-10"},
          {DATE, BOOLEAN, FALSE, null, FALSE},
          {DATE, INT, TRUE, LocalDate.fromYearMonthDay(2001, 2, 10), 11363},
          {DATE, LONG, TRUE, LocalDate.fromYearMonthDay(2001, 02, 11), 11364L},
          {DATE, NUMBER, FALSE, null, new BigInteger("-8888888888888888888888888888888")},
          {DATE, DOUBLE, FALSE, null, 3.33},
          {DATE, INET, FALSE, null, InetAddresses.forString("10.10.10.10")},
          {
            DATE,
            DATE,
            TRUE,
            LocalDate.fromYearMonthDay(1995, 8, 2),
            LocalDate.fromYearMonthDay(1995, 8, 2)
          },
          {DATE, TIMESTAMP, FALSE, null, new Date(System.currentTimeMillis())},
          {DATE, UUID, FALSE, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          {DATE, ENUM, FALSE, null, 7},
          {DATE, BLOB, FALSE, null, ByteBuffer.allocate(100)},
          // to TIMESTAMP
          {TIMESTAMP, STRING, FALSE, null, "12345"},
          {TIMESTAMP, BOOLEAN, FALSE, null, TRUE},
          {TIMESTAMP, INT, TRUE, new Date(1123456), 1123456},
          {TIMESTAMP, LONG, TRUE, new Date(981763200000L), 981763200000L},
          {TIMESTAMP, NUMBER, FALSE, null, new BigInteger("981763200000")},
          {TIMESTAMP, DOUBLE, FALSE, null, 88.888},
          {TIMESTAMP, INET, FALSE, null, InetAddresses.forString("10.11.12.13")},
          {TIMESTAMP, DATE, TRUE, new Date(807321600000L), LocalDate.fromYearMonthDay(1995, 8, 2)},
          {TIMESTAMP, TIMESTAMP, TRUE, new Date(807321600000L), new Date(807321600000L)},
          {TIMESTAMP, UUID, FALSE, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          {TIMESTAMP, ENUM, FALSE, null, 8},
          {TIMESTAMP, BLOB, FALSE, null, ByteBuffer.allocate(100)},
          // to UUID
          {UUID, STRING, FALSE, null, "c09e53e1-9f40-4f8d-b520-a28b989387ee"},
          {UUID, BOOLEAN, FALSE, null, FALSE},
          {UUID, INT, FALSE, null, 3},
          {UUID, LONG, FALSE, null, 10L},
          {UUID, NUMBER, FALSE, null, BigInteger.ONE},
          {UUID, DOUBLE, FALSE, null, 3.11},
          {UUID, INET, FALSE, null, InetAddresses.forString("10.11.12.13")},
          {UUID, DATE, FALSE, null, LocalDate.fromYearMonthDay(1995, 8, 2)},
          {UUID, TIMESTAMP, FALSE, null, new Date(981763200000L)},
          {
            UUID,
            UUID,
            TRUE,
            uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee"),
            uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")
          },
          {UUID, ENUM, FALSE, null, 9},
          {UUID, BLOB, FALSE, null, ByteBuffer.allocate(100)},
          // to ENUM
          {ENUM, STRING, FALSE, null, "c09e53e1-9f40-4f8d-b520-a28b989387ee"},
          {ENUM, BOOLEAN, FALSE, null, FALSE},
          {ENUM, INT, FALSE, null, 3},
          {ENUM, LONG, FALSE, null, 10L},
          {ENUM, NUMBER, FALSE, null, BigInteger.ONE},
          {ENUM, DOUBLE, FALSE, null, 3.11},
          {ENUM, INET, FALSE, null, InetAddresses.forString("10.11.12.13")},
          {ENUM, DATE, FALSE, null, LocalDate.fromDaysSinceEpoch(123)},
          {ENUM, TIMESTAMP, FALSE, null, new Date(981763200000L)},
          {ENUM, UUID, false, null, uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")},
          // {ENUM, ENUM, TRUE, null, null},
          {ENUM, BLOB, FALSE, null, ByteBuffer.allocate(100)},
          // to BLOB
          {BLOB, STRING, TRUE, ByteBuffer.wrap("hello world".getBytes()), "hello world"},
          {BLOB, BOOLEAN, FALSE, null, TRUE},
          {BLOB, INT, TRUE, ByteBuffer.allocate(4).putInt(2340054), 2340054},
          {BLOB, LONG, TRUE, ByteBuffer.allocate(8).putLong(6507226550L), 6507226550L},
          {
            BLOB,
            NUMBER,
            TRUE,
            ByteBuffer.wrap(new BigInteger("-78394393565013330783943935650133").toByteArray()),
            new BigInteger("-78394393565013330783943935650133")
          },
          {BLOB, DOUBLE, TRUE, ByteBuffer.allocate(8).putDouble(3.889798279), 3.889798279},
          {BLOB, INET, FALSE, null, InetAddresses.forString("10.11.12.13")},
          {BLOB, DATE, FALSE, null, LocalDate.fromYearMonthDay(2015, 5, 1)},
          {BLOB, TIMESTAMP, FALSE, null, new Date(System.currentTimeMillis())},
          {
            BLOB,
            UUID,
            TRUE,
            ByteBuffer.allocate(16)
                .put((byte) 0xc0)
                .put((byte) 0x9e)
                .put((byte) 0x53)
                .put((byte) 0xe1)
                .put((byte) 0x9f)
                .put((byte) 0x40)
                .put((byte) 0x4f)
                .put((byte) 0x8d)
                .put((byte) 0xb5)
                .put((byte) 0x20)
                .put((byte) 0xa2)
                .put((byte) 0x8b)
                .put((byte) 0x98)
                .put((byte) 0x93)
                .put((byte) 0x87)
                .put((byte) 0xee),
            uuid("c09e53e1-9f40-4f8d-b520-a28b989387ee")
          },
          {BLOB, ENUM, FALSE, null, 3},
          {
            BLOB,
            BLOB,
            TRUE,
            ByteBuffer.allocate(8).putLong(0xfeedfacecafebeefL),
            ByteBuffer.allocate(8).putLong(0xfeedfacecafebeefL)
          },
        });
  }

  @Parameter(0)
  public InternalAttributeType to;

  @Parameter(1)
  public InternalAttributeType from;

  @Parameter(2)
  public Boolean isConvertible;

  @Parameter(3)
  public Object expectedConversion;

  @Parameter(4)
  public Object conversionSrc;

  private static List<String> enumList;

  AdminImpl admin;

  @BeforeClass
  public static void setUpClass() {
    enumList =
        Arrays.asList("FISH", "HELLO", "ABC", "A", "AAA", "KLM", "FBI", "KGB", "KQED", "BMW");
  }

  @Before
  public void setUp() throws FileReadException {
    admin = new AdminImpl(null, new MetricsStreamProvider());
  }

  @After
  public void tearDown() throws Exception {}

  @SuppressWarnings("unchecked")
  @Test
  public void test() {
    AttributeDesc descFrom = new AttributeDesc("old", from);
    if (from == ENUM) {
      descFrom.setEnum(enumList);
    }
    AttributeDesc descTo = new AttributeDesc("new", to);
    final String message = String.format("to=%s from=%s", to, from);
    assertEquals(message, isConvertible, admin.isConvertible(descFrom, descTo));
    if (conversionSrc != null) {
      if (isConvertible) {
        if (conversionSrc instanceof List<?>) {
          final List<Object> srcs = (List<Object>) conversionSrc;
          final List<Object> expect = (List<Object>) expectedConversion;
          for (int i = 0; i < srcs.size(); ++i) {
            assertEquals(expect.get(i), TfosUtils.convertAttribute(srcs.get(i), descFrom, descTo));
          }
        } else {
          assertEquals(
              message,
              expectedConversion,
              TfosUtils.convertAttribute(conversionSrc, descFrom, descTo));
        }
      } else if (conversionSrc instanceof List<?>) {
        final List<Object> srcs = (List<Object>) conversionSrc;
        for (Object src : srcs) {
          try {
            TfosUtils.convertAttribute(src, descFrom, descTo);
            fail("exception is expected: " + message);
          } catch (UnsupportedOperationException e) {
            // expected
          }
        }
      } else if (conversionSrc != null) {
        try {
          TfosUtils.convertAttribute(conversionSrc, descFrom, descTo);
          fail("exception is expected: " + message);
        } catch (UnsupportedOperationException e) {
          // expected
        }
      }
    }
  }

  @Test
  public void enumTest() {
    assertTrue(
        isEnumConvertible(
            Arrays.asList("one", "two", "three"), Arrays.asList("one", "two", "three")));

    assertTrue(
        isEnumConvertible(
            Arrays.asList("one", "two", "three", "four"), Arrays.asList("one", "two", "three")));

    assertFalse(
        isEnumConvertible(
            Arrays.asList("one", "two", "three"), Arrays.asList("one", "two", "three", "four")));

    assertTrue(
        isEnumConvertible(
            Arrays.asList("one", "two", "three"), Arrays.asList("two", "three", "one")));

    assertTrue(
        isEnumConvertible(
            Arrays.asList("one", "four", "two", "three"), Arrays.asList("two", "three", "one")));

    assertFalse(
        isEnumConvertible(
            Arrays.asList("one", "two", "three"), Arrays.asList("One", "Two", "Three")));
  }

  private boolean isEnumConvertible(List<String> to, List<String> from) {
    final AttributeDesc descFrom = new AttributeDesc("old", ENUM);
    descFrom.setEnum(from);
    final AttributeDesc descTo = new AttributeDesc("new", ENUM);
    descTo.setEnum(to);
    return admin.isConvertible(descFrom, descTo);
  }
}
