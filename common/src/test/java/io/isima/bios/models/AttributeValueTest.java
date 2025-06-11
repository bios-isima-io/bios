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
package io.isima.bios.models;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import io.isima.bios.exceptions.InvalidDataRetrievalException;
import java.util.Base64;
import org.junit.Test;

public class AttributeValueTest {

  @Test
  public void testNonNumericString() {
    final AttributeValueGeneric hello = new AttributeValueGeneric("hello", AttributeType.STRING);
    assertEquals("hello", hello.asString());
    try {
      hello.asLong();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      hello.asDouble();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    assertEquals(Boolean.FALSE, hello.asBoolean());
    try {
      hello.asByteArray();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
  }

  @Test
  public void testIntegerString() {
    final AttributeValueGeneric integer = new AttributeValueGeneric("-123", AttributeType.STRING);
    assertEquals("-123", integer.asString());
    assertEquals(-123, integer.asLong());
    assertEquals(-123.0, integer.asDouble(), 0.0);
    assertEquals(false, integer.asBoolean());
    try {
      integer.asByteArray();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
  }

  @Test
  public void testDecimalString() {
    final AttributeValueGeneric decimal = new AttributeValueGeneric("123.45", AttributeType.STRING);
    assertEquals("123.45", decimal.asString());
    try {
      decimal.asLong();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    assertEquals(123.45, decimal.asDouble(), 0.0);
    assertEquals(false, decimal.asBoolean());
    try {
      decimal.asByteArray();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
  }

  @Test
  public void testBooleanString() {
    final AttributeValueGeneric hello = new AttributeValueGeneric("true", AttributeType.STRING);
    assertEquals("true", hello.asString());
    try {
      hello.asLong();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      hello.asDouble();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    assertEquals(Boolean.TRUE, hello.asBoolean());
    try {
      hello.asByteArray();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
  }

  @Test
  public void testInteger() {
    final AttributeValueGeneric integer =
        new AttributeValueGeneric("16507226550", AttributeType.STRING);
    assertEquals("16507226550", integer.asString());
    assertEquals(16507226550L, integer.asLong());
    assertEquals(16507226550L, integer.asDouble(), 0.0);
    try {
      integer.asBoolean();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      integer.asByteArray();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
  }

  @Test
  public void testDecimal() {
    final AttributeValueGeneric decimal =
        new AttributeValueGeneric("16507226550.01234", AttributeType.DECIMAL);
    assertEquals("1.650722655001234E10", decimal.asString());
    assertEquals(16507226550L, decimal.asLong());
    assertEquals(16507226550.01234, decimal.asDouble(), 0.0);
    try {
      decimal.asBoolean();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      decimal.asByteArray();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
  }

  @Test
  public void testBoolean() {
    final AttributeValueGeneric bool = new AttributeValueGeneric("True", AttributeType.BOOLEAN);
    assertEquals("true", bool.asString());
    try {
      bool.asLong();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      bool.asDouble();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    assertEquals(true, bool.asBoolean());
    try {
      bool.asByteArray();
      fail("exception must happen");
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
  }

  @Test
  public void testBlob() {
    final byte[] source = new byte[] {6, 5, 0, 7, 2, 2, 6, 5, 5, 0};
    final String encoded = Base64.getEncoder().encodeToString(source);
    final AttributeValueGeneric blob = new AttributeValueGeneric(encoded, AttributeType.BLOB);
    try {
      blob.asString();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      blob.asLong();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      blob.asDouble();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    try {
      blob.asBoolean();
    } catch (InvalidDataRetrievalException e) {
      // expected
    }
    assertArrayEquals(source, blob.asByteArray());
  }
}
