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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.nio.ByteBuffer;
import org.junit.BeforeClass;
import org.junit.Test;

public class AttributeTypeTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testSerializer() throws Exception {
    assertEquals("\"String\"", objectMapper.writeValueAsString(AttributeType.STRING));
    assertEquals("\"Integer\"", objectMapper.writeValueAsString(AttributeType.INTEGER));
    assertEquals("\"Decimal\"", objectMapper.writeValueAsString(AttributeType.DECIMAL));
    assertEquals("\"Blob\"", objectMapper.writeValueAsString(AttributeType.BLOB));
    assertEquals("\"Boolean\"", objectMapper.writeValueAsString(AttributeType.BOOLEAN));
  }

  @Test
  public void testDeserializer() throws Exception {
    assertEquals(AttributeType.STRING, objectMapper.readValue("\"string\"", AttributeType.class));
    assertEquals(AttributeType.INTEGER, objectMapper.readValue("\"integer\"", AttributeType.class));
    assertEquals(AttributeType.DECIMAL, objectMapper.readValue("\"decimal\"", AttributeType.class));
    assertEquals(AttributeType.BLOB, objectMapper.readValue("\"blob\"", AttributeType.class));
    assertEquals(AttributeType.BOOLEAN, objectMapper.readValue("\"boolean\"", AttributeType.class));
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializerFail() throws Exception {
    objectMapper.readValue("unknwon", AttributeType.class);
  }

  @Test
  public void stringValueAsObject() {
    final AttributeValueGeneric attribute =
        new AttributeValueGeneric("hello", AttributeType.STRING);
    final Object value = AttributeType.STRING.attributeAsObject(attribute);
    assertThat(value, instanceOf(String.class));
    assertThat(value, is("hello"));
  }

  @Test
  public void integerValueAsObject() {
    final AttributeValueGeneric attribute =
        new AttributeValueGeneric(6507226550L, AttributeType.INTEGER);
    final Object value = AttributeType.INTEGER.attributeAsObject(attribute);
    assertThat(value, instanceOf(Long.class));
    assertThat(value, is(6507226550L));
  }

  @Test
  public void decimalValueAsObject() {
    final AttributeValueGeneric attribute =
        new AttributeValueGeneric(2.18345, AttributeType.DECIMAL);
    final Object value = AttributeType.DECIMAL.attributeAsObject(attribute);
    assertThat(value, instanceOf(Double.class));
    assertThat(value, is(2.18345));
  }

  @Test
  public void booleanValueAsObject() {
    final AttributeValueGeneric attribute = new AttributeValueGeneric(true, AttributeType.BOOLEAN);
    final Object value = AttributeType.BOOLEAN.attributeAsObject(attribute);
    assertThat(value, instanceOf(Boolean.class));
    assertThat(value, is(true));
  }

  @Test
  public void blobValueAsObject() {
    final AttributeValueGeneric attribute =
        new AttributeValueGeneric("hello".getBytes(), AttributeType.BLOB);
    final Object value = AttributeType.BLOB.attributeAsObject(attribute);
    assertThat(value, instanceOf(ByteBuffer.class));
    assertThat(value, is(ByteBuffer.wrap("hello".getBytes())));
  }
}
