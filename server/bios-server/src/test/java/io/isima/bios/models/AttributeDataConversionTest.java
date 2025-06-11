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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import io.isima.bios.models.v1.InternalAttributeType;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;
import org.junit.Test;

public class AttributeDataConversionTest {

  @Test
  public void stringConversion() throws InvalidValueSyntaxException {
    Object converted = InternalAttributeType.STRING.parse("Strings are converted to strings.");
    assertEquals("Strings are converted to strings.", converted);

    try {
      InternalAttributeType.STRING.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: String value may not be null", ex.getMessage());
    }

    Object out = InternalAttributeType.STRING.parse("");
    assertEquals("", out);

    out = InternalAttributeType.STRING.parse(" ");
    assertEquals(" ", out);
  }

  @Test
  public void booleanConversion() throws InvalidValueSyntaxException {
    assertEquals(Boolean.TRUE, InternalAttributeType.BOOLEAN.parse("true"));
    assertEquals(Boolean.TRUE, InternalAttributeType.BOOLEAN.parse("True"));
    assertEquals(Boolean.TRUE, InternalAttributeType.BOOLEAN.parse("TRUE"));
    assertEquals(Boolean.TRUE, InternalAttributeType.BOOLEAN.parse(" true"));
    assertEquals(Boolean.TRUE, InternalAttributeType.BOOLEAN.parse("true "));
    assertEquals(Boolean.TRUE, InternalAttributeType.BOOLEAN.parse(" true "));

    assertEquals(Boolean.FALSE, InternalAttributeType.BOOLEAN.parse("false"));
    assertEquals(Boolean.FALSE, InternalAttributeType.BOOLEAN.parse("False"));
    assertEquals(Boolean.FALSE, InternalAttributeType.BOOLEAN.parse("FALSE"));
    assertEquals(Boolean.FALSE, InternalAttributeType.BOOLEAN.parse("  false"));
    assertEquals(Boolean.FALSE, InternalAttributeType.BOOLEAN.parse("false  "));
    assertEquals(Boolean.FALSE, InternalAttributeType.BOOLEAN.parse("  false  "));

    try {
      Object converted = InternalAttributeType.BOOLEAN.parse("yes");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: For value 'yes': Boolean value must be 'true' or 'false'",
          ex.getMessage());
    }

    try {
      Object converted = InternalAttributeType.BOOLEAN.parse("tr ue");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: For value 'tr ue': Boolean value must be 'true' or 'false'",
          ex.getMessage());
    }

    // we don't allow shortened expressions, too
    try {
      Object converted = InternalAttributeType.BOOLEAN.parse("t");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: For value 't': Boolean value must be 'true' or 'false'",
          ex.getMessage());
    }

    try {
      Object converted = InternalAttributeType.BOOLEAN.parse("f");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: For value 'f': Boolean value must be 'true' or 'false'",
          ex.getMessage());
    }

    try {
      InternalAttributeType.BOOLEAN.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Boolean value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.BOOLEAN.parse("");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Boolean value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.BOOLEAN.parse("");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Boolean value may not be null or empty", ex.getMessage());
    }
  }

  @Test
  public void intConversion() throws InvalidValueSyntaxException {
    assertEquals(12345, InternalAttributeType.INT.parse("12345"));
    assertEquals(-178907, InternalAttributeType.INT.parse("-178907"));

    assertEquals(12345, InternalAttributeType.INT.parse(" 12345"));
    assertEquals(12345, InternalAttributeType.INT.parse(" 12345 "));
    assertEquals(12345, InternalAttributeType.INT.parse("12345 "));

    try {
      Object converted = InternalAttributeType.INT.parse("1.e-10");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Input must be a numeric string", ex.getMessage());
    }

    try {
      Object converted = InternalAttributeType.INT.parse("9999999999999999999999999999999999");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Input must be a numeric string", ex.getMessage());
    }

    try {
      InternalAttributeType.INT.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Int value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.INT.parse("  ");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Int value may not be null or empty", ex.getMessage());
    }
  }

  @Test
  public void numberConversion() throws InvalidValueSyntaxException {
    assertEquals(BigInteger.valueOf(12345), InternalAttributeType.NUMBER.parse("12345"));
    assertEquals(BigInteger.valueOf(-178907), InternalAttributeType.NUMBER.parse("-178907"));

    assertEquals(BigInteger.valueOf(12345), InternalAttributeType.NUMBER.parse(" 12345"));
    assertEquals(BigInteger.valueOf(12345), InternalAttributeType.NUMBER.parse("12345 "));
    assertEquals(BigInteger.valueOf(12345), InternalAttributeType.NUMBER.parse(" 12345 "));

    try {
      Object converted = InternalAttributeType.NUMBER.parse("1.e-10");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Illegal embedded sign character", ex.getMessage());
    }

    Object converted = InternalAttributeType.NUMBER.parse("9999999999999999999999999999999999");
    assertTrue(converted instanceof BigInteger);
    assertEquals("9999999999999999999999999999999999", converted.toString());

    converted = InternalAttributeType.NUMBER.parse("-9999999999999999999999999999999999");
    assertTrue(converted instanceof BigInteger);
    assertEquals("-9999999999999999999999999999999999", converted.toString());

    try {
      InternalAttributeType.NUMBER.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Number value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.NUMBER.parse("");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Number value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.NUMBER.parse("   ");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Number value may not be null or empty", ex.getMessage());
    }
  }

  @Test
  public void doubleConversion() throws InvalidValueSyntaxException {
    assertEquals(12345.0, InternalAttributeType.DOUBLE.parse("12345"));
    assertEquals(12345.6, InternalAttributeType.DOUBLE.parse("12345.6"));
    assertEquals(-178907.0, InternalAttributeType.DOUBLE.parse("-178907"));
    assertEquals(1.e-10, InternalAttributeType.DOUBLE.parse("1.e-10"));
    assertEquals(-3.33e22, InternalAttributeType.DOUBLE.parse("-3.33E22"));

    assertEquals(12345.6, InternalAttributeType.DOUBLE.parse(" 12345.6"));
    assertEquals(12345.6, InternalAttributeType.DOUBLE.parse(" 12345.6 "));
    assertEquals(12345.6, InternalAttributeType.DOUBLE.parse("12345.6 "));

    try {
      Object converted = InternalAttributeType.DOUBLE.parse("This does not represent a double");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Input must be a numeric string", ex.getMessage());
    }

    try {
      InternalAttributeType.DOUBLE.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Decimal value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.DOUBLE.parse("");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Decimal value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.DOUBLE.parse("  ");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Decimal value may not be null or empty", ex.getMessage());
    }
  }

  @Test
  public void dateConversion() throws InvalidValueSyntaxException {
    Object converted = InternalAttributeType.DATE.parse("2018-03-08");
    assertEquals("2018-03-08", converted.toString());

    converted = InternalAttributeType.DATE.parse("2018-03-08 ");
    assertEquals("2018-03-08", converted.toString());

    converted = InternalAttributeType.DATE.parse(" 2018-03-08");
    assertEquals("2018-03-08", converted.toString());

    converted = InternalAttributeType.DATE.parse(" 2018-03-08 ");
    assertEquals("2018-03-08", converted.toString());

    converted = InternalAttributeType.DATE.parse("12345");
    assertEquals(12345, ((LocalDate) converted).toEpochDay());

    try {
      converted = InternalAttributeType.DATE.parse("2018 03 08");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: For input string: \"2018 03 08\"", ex.getMessage());
    }

    try {
      converted = InternalAttributeType.DATE.parse("deadbeef");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: For input string: \"deadbeef\"", ex.getMessage());
    }

    try {
      InternalAttributeType.DATE.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Date value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.DATE.parse("");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Date value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.DATE.parse("  ");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Date value may not be null or empty", ex.getMessage());
    }
  }

  @Test
  public void timestampConversion() throws InvalidValueSyntaxException {
    Object converted = InternalAttributeType.TIMESTAMP.parse("2018-03-08T21:30:27.339-0000");
    assertEquals(1520544627339L, ((Date) converted).getTime());
    converted = InternalAttributeType.TIMESTAMP.parse("2018-03-08T21:30:27.339Z");
    assertEquals(1520544627339L, ((Date) converted).getTime());

    converted = InternalAttributeType.TIMESTAMP.parse(" 2018-03-08T21:30:27.339Z");
    assertEquals(1520544627339L, ((Date) converted).getTime());

    converted = InternalAttributeType.TIMESTAMP.parse("2018-03-08T21:30:27.339Z ");
    assertEquals(1520544627339L, ((Date) converted).getTime());

    converted = InternalAttributeType.TIMESTAMP.parse(" 2018-03-08T21:30:27.339Z ");
    assertEquals(1520544627339L, ((Date) converted).getTime());

    converted = InternalAttributeType.TIMESTAMP.parse("12345");
    assertEquals(12345L, ((Date) converted).getTime());

    try {
      converted = InternalAttributeType.TIMESTAMP.parse("Can you eat me?");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Unparseable date: \"Can you eat me?\"", ex.getMessage());
    }

    try {
      InternalAttributeType.TIMESTAMP.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: Timestamp value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.TIMESTAMP.parse("");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: Timestamp value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.TIMESTAMP.parse(" ");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: Timestamp value may not be null or empty", ex.getMessage());
    }
  }

  @Test
  public void uuidConversion() throws InvalidValueSyntaxException {
    Object converted = InternalAttributeType.UUID.parse("4de755b4-acec-46e7-a493-e01215e2db22");
    assertEquals(UUID.fromString("4de755b4-acec-46e7-a493-e01215e2db22"), converted);

    converted = InternalAttributeType.UUID.parse(" 4de755b4-acec-46e7-a493-e01215e2db22");
    assertEquals(UUID.fromString("4de755b4-acec-46e7-a493-e01215e2db22"), converted);

    converted = InternalAttributeType.UUID.parse("4de755b4-acec-46e7-a493-e01215e2db22 ");
    assertEquals(UUID.fromString("4de755b4-acec-46e7-a493-e01215e2db22"), converted);

    converted = InternalAttributeType.UUID.parse(" 4de755b4-acec-46e7-a493-e01215e2db22 ");
    assertEquals(UUID.fromString("4de755b4-acec-46e7-a493-e01215e2db22"), converted);

    try {
      converted = InternalAttributeType.UUID.parse("4de755b4-acec-46e7-a493-e01215e2db2");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: UUID string length must be 36", ex.getMessage());
    }

    try {
      converted = InternalAttributeType.UUID.parse("4de755b4-acec-46e7-a493-e01215e2db222");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: UUID string length must be 36", ex.getMessage());
    }

    try {
      converted = InternalAttributeType.UUID.parse("I am 36 chars length but not an uuid");
      fail(converted.toString());
    } catch (InvalidValueSyntaxException ex) {
      assertEquals(
          "Invalid value syntax: Invalid UUID string: I am 36 chars length but not an uuid",
          ex.getMessage());
    }

    try {
      InternalAttributeType.UUID.parse(null);
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Uuid value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.UUID.parse("");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Uuid value may not be null or empty", ex.getMessage());
    }

    try {
      InternalAttributeType.UUID.parse(" ");
      fail();
    } catch (InvalidValueSyntaxException ex) {
      assertEquals("Invalid value syntax: Uuid value may not be null or empty", ex.getMessage());
    }
  }
}
