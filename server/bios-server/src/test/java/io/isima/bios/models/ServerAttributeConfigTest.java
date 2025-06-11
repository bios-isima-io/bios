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
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import io.isima.bios.errors.exception.InvalidValueSyntaxException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Test;

public class ServerAttributeConfigTest {

  @Test
  public void stringAttributeWithAllowedValues() throws Exception {
    final var attributeConfig =
        makeAttributeConfig(
            "county", AttributeType.STRING, List.of("San Mateo", "Santa Clara", "San Francisco"));
    assertThat(attributeConfig.getEnumIndex("San Mateo"), is(Integer.valueOf(0)));
    assertThat(attributeConfig.getEnumIndex("Santa Clara"), is(Integer.valueOf(1)));
    assertThat(attributeConfig.getEnumIndex("San Francisco"), is(Integer.valueOf(2)));
    assertNull(attributeConfig.getEnumIndex("San Jose"));

    assertThat(attributeConfig.toInternalValue("San Mateo"), is(Integer.valueOf(0)));
    assertThat(attributeConfig.toInternalValue("Santa Clara"), is(Integer.valueOf(1)));
    assertThat(attributeConfig.toInternalValue("San Francisco"), is(Integer.valueOf(2)));
    assertThrows(
        InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue("Alameda"));
  }

  @Test
  public void stringAttribute() throws Exception {
    final var attributeConfig = makeAttributeConfig("productName", AttributeType.STRING, List.of());
    assertNull(attributeConfig.getEnumIndex("whatever"));

    assertThat(attributeConfig.toInternalValue("Smart phone"), is("Smart phone"));
    assertThat(attributeConfig.toInternalValue("アンテナ"), is("アンテナ"));
    assertThat(attributeConfig.toInternalValue(""), is(""));
  }

  @Test
  public void integerAttribute() throws Exception {
    final var attributeConfig = makeAttributeConfig("quantity", AttributeType.INTEGER, List.of());
    assertNull(attributeConfig.getEnumIndex("whatever"));

    assertThat(attributeConfig.toInternalValue("10"), is(Long.valueOf(10)));
    assertThat(attributeConfig.toInternalValue("-1"), is(Long.valueOf(-1)));
    assertThrows(
        InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue("Alameda"));
    assertThrows(InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue("10.0"));
    assertThrows(InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue("false"));
    assertThrows(InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue(""));
  }

  @Test
  public void decimalAttribute() throws Exception {
    final var attributeConfig = makeAttributeConfig("price", AttributeType.DECIMAL, List.of());
    assertNull(attributeConfig.getEnumIndex("whatever"));

    assertThat(attributeConfig.toInternalValue("10"), is(Double.valueOf(10)));
    assertThat(attributeConfig.toInternalValue("-1"), is(Double.valueOf(-1)));
    assertThat(attributeConfig.toInternalValue("10.0"), is(Double.valueOf(10.0)));
    assertThat(attributeConfig.toInternalValue("-36729.65"), is(Double.valueOf(-36729.65)));
    assertThrows(
        InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue("Alameda"));
    assertThrows(InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue("false"));
    assertThrows(InvalidValueSyntaxException.class, () -> attributeConfig.toInternalValue(""));
  }

  @Test
  public void booleanAttribute() throws Exception {
    final var attributeConfig = makeAttributeConfig("isActive", AttributeType.BOOLEAN, List.of());
    assertNull(attributeConfig.getEnumIndex("whatever"));

    assertThat(attributeConfig.toInternalValue("true"), is(Boolean.TRUE));
    assertThat(attributeConfig.toInternalValue("True"), is(Boolean.TRUE));
    assertThat(attributeConfig.toInternalValue("TRUE"), is(Boolean.TRUE));
    assertThat(attributeConfig.toInternalValue("false"), is(Boolean.FALSE));
    assertThat(attributeConfig.toInternalValue("False"), is(Boolean.FALSE));
    assertThat(attributeConfig.toInternalValue("FALSE"), is(Boolean.FALSE));

    // confusing, should we change the behavior?
    assertThat(attributeConfig.toInternalValue("yes"), is(Boolean.FALSE));
    assertThat(attributeConfig.toInternalValue("wrongButItFallsToFalse"), is(Boolean.FALSE));
    assertThat(attributeConfig.toInternalValue(""), is(Boolean.FALSE));
  }

  @Test
  public void blobAttribute() throws Exception {
    final var attributeConfig = makeAttributeConfig("signature", AttributeType.BLOB, List.of());
    assertNull(attributeConfig.getEnumIndex("whatever"));

    final var binaryData = new byte[] {(byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5};

    final var src = Base64.getEncoder().encodeToString(binaryData);

    assertThat(attributeConfig.toInternalValue(src), is(ByteBuffer.wrap(binaryData)));
  }

  private ServerAttributeConfig makeAttributeConfig(
      String name, AttributeType type, List<String> allowedValues) {
    final var attributeConfig = new ServerAttributeConfig();
    attributeConfig.setName(name);
    attributeConfig.setType(type);
    if (allowedValues != null && !allowedValues.isEmpty()) {
      attributeConfig.setAllowedValues(
          allowedValues.stream()
              .map((value) -> new AttributeValueGeneric(value, AttributeType.STRING))
              .collect(Collectors.toList()));
    }
    attributeConfig.compile();
    return attributeConfig;
  }
}
