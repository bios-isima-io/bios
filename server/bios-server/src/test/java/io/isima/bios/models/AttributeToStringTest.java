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

import static io.isima.bios.models.v1.InternalAttributeType.BLOB;
import static io.isima.bios.models.v1.InternalAttributeType.BOOLEAN;
import static io.isima.bios.models.v1.InternalAttributeType.DATE;
import static io.isima.bios.models.v1.InternalAttributeType.INET;
import static io.isima.bios.models.v1.InternalAttributeType.INT;
import static io.isima.bios.models.v1.InternalAttributeType.NUMBER;
import static io.isima.bios.models.v1.InternalAttributeType.STRING;
import static io.isima.bios.models.v1.InternalAttributeType.TIMESTAMP;
import static org.junit.Assert.assertEquals;

import com.google.common.net.InetAddresses;
import io.isima.bios.models.v1.InternalAttributeType;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class AttributeToStringTest {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {}

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void testStringToString() {
    final String data = "hello world";
    assertEquals("hello world", STRING.toString(data));
  }

  @Test
  public void testLongStringToString() {
    final String data =
        "Raindrops on roses and whiskers on kittens, "
            + "bright copper kettles and warm woolen mittens";
    assertEquals("Raindrops on roses and whiskers on kittens, brig...", STRING.toString(data));
  }

  @Test
  public void testIntToString() {
    final Integer data = Integer.MIN_VALUE;
    assertEquals(data.toString(), INT.toString(data));
  }

  @Test
  public void testNumberToString() {
    final BigInteger data = new BigInteger("987654321098765432109876543210");
    assertEquals("987654321098765432109876543210", NUMBER.toString(data));
  }

  @Test
  public void testDoubleToString() {
    final Double data = 3.14159265318989;
    assertEquals("3.14159265318989", InternalAttributeType.DOUBLE.toString(data));
  }

  @Test
  public void testLocalDateToString() {
    final LocalDate data = LocalDate.of(2018, 9, 6);
    assertEquals("2018-09-06", DATE.toString(data));
  }

  @Test
  public void testDateToString() {
    final Date data = new Date(1234567);
    assertEquals("1234567", TIMESTAMP.toString(data));
  }

  @Test
  public void testInetToString() {
    final InetAddress data = InetAddresses.forString("129.18.21.30");
    assertEquals("129.18.21.30", INET.toString(data));
  }

  @Test
  public void testInet6ToString() {
    final InetAddress data = InetAddresses.forString("2001:0db8:85a3:0000:0000:8a2e:0370:7334");
    assertEquals("2001:db8:85a3::8a2e:370:7334", INET.toString(data));

    final InetAddress data2 = InetAddresses.forString("2001:1db8:85a3:1000:1000:8a2e:1370:7334");
    assertEquals("2001:1db8:85a3:1000:1000:8a2e:1370:7334", INET.toString(data2));
  }

  @Test
  public void testBooleanToString() {
    assertEquals("true", BOOLEAN.toString(Boolean.TRUE));
  }

  @Test
  public void testUuidToString() {
    final String src = "968844e6-c8d0-11e8-a8d5-f2801f1b9fd1";
    final UUID data = UUID.fromString(src);
    assertEquals(src, InternalAttributeType.UUID.toString(data));
  }

  @Test
  public void testBlobToString() {
    final ByteBuffer data = ByteBuffer.wrap("1234567890".getBytes());
    assertEquals("0x31323334353637383930", BLOB.toString(data));
    assertEquals(0, data.position());
  }

  @Test
  public void testLongBlobToString() {
    final ByteBuffer data = ByteBuffer.wrap("A long time ago in a galaxy far, far away".getBytes());
    assertEquals("0x41206c6f6e672074696d652061676f20696e2061206761...", BLOB.toString(data));
    assertEquals(0, data.position());
  }
}
