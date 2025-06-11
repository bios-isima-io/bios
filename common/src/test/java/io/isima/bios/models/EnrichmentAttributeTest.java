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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.nio.ByteBuffer;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnrichmentAttributeTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{" + "  'attributeName': 'statusName'," + "  'as': 'status'," + "  'fillIn': -1" + "}";

    final EnrichmentAttribute attribute =
        objectMapper.readValue(src.replace("'", "\""), EnrichmentAttribute.class);

    assertEquals("statusName", attribute.getAttributeName());
    assertEquals("status", attribute.getAs());
    assertEquals("-1", attribute.getFillInSerialized());
    assertNull(attribute.getFillIn());

    final String expected =
        src.replace("'", "\"")
            .replace(" ", "")
            .replace("\n", "")
            .replace("-1", "\"-1\""); // serialized data is string although the source is integer
    assertEquals(expected, objectMapper.writeValueAsString(attribute));

    assertTrue(attribute.equals(attribute));
    assertFalse(attribute.equals(null));
    assertFalse(attribute.equals("hello"));
    {
      final var attribute2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentAttribute.class);
      assertTrue(attribute.equals(attribute2));
      assertEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final var attribute2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentAttribute.class);
      attribute2.setAttributeName("status");
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final var attribute2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentAttribute.class);
      attribute2.setAs("statusName");
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final var attribute2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentAttribute.class);
      attribute2.setFillInSerialized("0");
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
  }

  @Test
  public void testAttributeWithStringFillIn() throws Exception {
    final var attribute = new EnrichmentAttribute();
    attribute.setAttributeName("areaName");
    attribute.setFillIn(new AttributeValueGeneric("n/a", AttributeType.STRING));

    final String serialized = objectMapper.writeValueAsString(attribute);
    assertEquals("{\"attributeName\":\"areaName\",\"fillIn\":\"n/a\"}", serialized);
    final var decoded = objectMapper.readValue(serialized, EnrichmentAttribute.class);

    decoded.setFillIn(
        new AttributeValueGeneric(decoded.getFillInSerialized(), AttributeType.STRING));
    assertEquals("areaName", decoded.getAttributeName());
    assertEquals("n/a", decoded.getFillIn().asString());
  }

  @Test
  public void testAttributeWithIntegerFillIn() throws Exception {
    final var attribute = new EnrichmentAttribute();
    attribute.setAttributeName("intValue");
    attribute.setFillIn(new AttributeValueGeneric(-83154, AttributeType.INTEGER));

    final String serialized = objectMapper.writeValueAsString(attribute);
    assertEquals("{\"attributeName\":\"intValue\",\"fillIn\":-83154}", serialized);

    final var decoded = objectMapper.readValue(serialized, EnrichmentAttribute.class);
    decoded.setFillIn(
        new AttributeValueGeneric(decoded.getFillInSerialized(), AttributeType.INTEGER));
    assertEquals("intValue", decoded.getAttributeName());
    assertEquals(-83154L, decoded.getFillIn().asLong());

    {
      final var attribute2 = new EnrichmentAttribute();
      attribute2.setAttributeName("intValue");
      attribute2.setFillIn(new AttributeValueGeneric(-83154, AttributeType.INTEGER));
      assertTrue(attribute.equals(attribute2));
      assertEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final var attribute2 = new EnrichmentAttribute();
      attribute2.setAttributeName("intValue");
      attribute2.setFillIn(new AttributeValueGeneric(-83155, AttributeType.INTEGER));
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
  }

  @Test
  public void testAttributeWithDecimalFillIn() throws Exception {
    final var attribute = new EnrichmentAttribute();
    attribute.setAttributeName("decimalValue");
    attribute.setFillIn(new AttributeValueGeneric(-22.3214, AttributeType.DECIMAL));

    final String serialized = objectMapper.writeValueAsString(attribute);
    assertEquals("{\"attributeName\":\"decimalValue\",\"fillIn\":-22.3214}", serialized);

    final var decoded = objectMapper.readValue(serialized, EnrichmentAttribute.class);
    decoded.setFillIn(
        new AttributeValueGeneric(decoded.getFillInSerialized(), AttributeType.DECIMAL));
    assertEquals("decimalValue", decoded.getAttributeName());
    assertEquals(-22.3214, decoded.getFillIn().asDouble(), 1.e-10);
  }

  @Test
  public void testAttributeWithBooleanFillIn() throws Exception {
    final var attribute = new EnrichmentAttribute();
    attribute.setAttributeName("booleanValue");
    attribute.setFillIn(new AttributeValueGeneric(true, AttributeType.BOOLEAN));

    final String serialized = objectMapper.writeValueAsString(attribute);
    assertEquals("{\"attributeName\":\"booleanValue\",\"fillIn\":true}", serialized);

    final var decoded = objectMapper.readValue(serialized, EnrichmentAttribute.class);
    decoded.setFillIn(
        new AttributeValueGeneric(decoded.getFillInSerialized(), AttributeType.BOOLEAN));
    assertEquals("booleanValue", decoded.getAttributeName());
    assertEquals(true, decoded.getFillIn().asBoolean());
  }

  @Test
  public void testAttributeWithBlobFillIn() throws Exception {
    final var attribute = new EnrichmentAttribute();
    attribute.setAttributeName("blobValue");
    ByteBuffer buffer = ByteBuffer.allocate(4);
    buffer.putInt(0xdeadbeef);
    buffer.flip();
    attribute.setFillIn(new AttributeValueGeneric(buffer.array(), AttributeType.BLOB));

    final String serialized = objectMapper.writeValueAsString(attribute);
    assertEquals("{\"attributeName\":\"blobValue\",\"fillIn\":\"3q2+7w==\"}", serialized);

    final var decoded = objectMapper.readValue(serialized, EnrichmentAttribute.class);
    decoded.setFillIn(new AttributeValueGeneric(decoded.getFillInSerialized(), AttributeType.BLOB));

    assertEquals("blobValue", decoded.getAttributeName());
    assertEquals(0xdeadbeef, ByteBuffer.wrap(decoded.getFillIn().asByteArray()).getInt());
  }
}
