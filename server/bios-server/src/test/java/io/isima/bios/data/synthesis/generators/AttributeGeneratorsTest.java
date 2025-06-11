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
package io.isima.bios.data.synthesis.generators;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.SignalConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Test;

public class AttributeGeneratorsTest {

  @Test
  public void generatorsForSignalCreationTest() throws Exception {
    final var signalConfig = new SignalConfig();
    signalConfig.setName("testtest");
    signalConfig.setVersion(System.currentTimeMillis());
    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig("myStringAttribute", AttributeType.STRING));
    attributes.add(new AttributeConfig("myIntegerAttribute", AttributeType.INTEGER));
    attributes.add(new AttributeConfig("myDecimalAttribute", AttributeType.DECIMAL));
    attributes.add(new AttributeConfig("myBooleanAttribute", AttributeType.BOOLEAN));
    attributes.add(new AttributeConfig("myBlobAttribute", AttributeType.BLOB));
    final var enumAttribute = new AttributeConfig("myEnumAttribute", AttributeType.STRING);
    enumAttribute.setAllowedValues(
        List.of(
            new AttributeValueGeneric("ONE", AttributeType.STRING),
            new AttributeValueGeneric("TWO", AttributeType.STRING),
            new AttributeValueGeneric("THREE", AttributeType.STRING)));
    attributes.add(enumAttribute);
    signalConfig.setAttributes(attributes);

    final Map<String, AttributeGenerator<?>> generators =
        AttributeGeneratorsCreator.getCreator("tenant", signalConfig).create();

    assertThat(generators.get("myStringAttribute"), instanceOf(GenericStringGenerator.class));
    assertThat(generators.get("myIntegerAttribute"), instanceOf(GenericIntegerGenerator.class));
    assertThat(generators.get("myDecimalAttribute"), instanceOf(GenericDecimalGenerator.class));
    assertThat(generators.get("myBooleanAttribute"), instanceOf(GenericBooleanGenerator.class));
    assertThat(generators.get("myBlobAttribute"), instanceOf(GenericBlobGenerator.class));
    assertThat(
        generators.get("myEnumAttribute"), instanceOf(InternalValueStringEnumGenerator.class));
    final var enumGenerator = (InternalValueStringEnumGenerator) generators.get("myEnumAttribute");
    assertThat(enumGenerator.getValueRange(), is(3));
  }

  @Test
  public void generatorsForContextCreationTest() throws Exception {
    final var signalConfig = new ContextConfig();
    signalConfig.setName("testtest");
    signalConfig.setVersion(System.currentTimeMillis());
    final var attributes = new ArrayList<AttributeConfig>();
    attributes.add(new AttributeConfig("myStringAttribute", AttributeType.STRING));
    attributes.add(new AttributeConfig("myIntegerAttribute", AttributeType.INTEGER));
    attributes.add(new AttributeConfig("myDecimalAttribute", AttributeType.DECIMAL));
    attributes.add(new AttributeConfig("myBooleanAttribute", AttributeType.BOOLEAN));
    attributes.add(new AttributeConfig("myBlobAttribute", AttributeType.BLOB));
    final var enumAttribute = new AttributeConfig("myEnumAttribute", AttributeType.STRING);
    enumAttribute.setAllowedValues(
        List.of(
            new AttributeValueGeneric("ONE", AttributeType.STRING),
            new AttributeValueGeneric("TWO", AttributeType.STRING),
            new AttributeValueGeneric("THREE", AttributeType.STRING)));
    attributes.add(enumAttribute);
    signalConfig.setAttributes(attributes);
    signalConfig.setPrimaryKey(List.of());

    final Map<String, AttributeGenerator<?>> generators =
        AttributeGeneratorsCreator.getCreator("tenant", signalConfig).create();

    assertThat(generators.get("myStringAttribute"), instanceOf(GenericStringGenerator.class));
    assertThat(generators.get("myIntegerAttribute"), instanceOf(GenericIntegerGenerator.class));
    assertThat(generators.get("myDecimalAttribute"), instanceOf(GenericDecimalGenerator.class));
    assertThat(generators.get("myBooleanAttribute"), instanceOf(GenericBooleanGenerator.class));
    assertThat(generators.get("myBlobAttribute"), instanceOf(GenericBlobGenerator.class));
    assertThat(generators.get("myEnumAttribute"), instanceOf(GenericStringEnumGenerator.class));
    final var enumGenerator = (GenericStringEnumGenerator) generators.get("myEnumAttribute");
    assertThat(enumGenerator.getAllowedValues(), is(List.of("ONE", "TWO", "THREE")));
  }

  @Test
  public void genericStringGeneratorTest() {
    final AttributeGenerator<String> generator = new GenericStringGenerator(16);
    final String output = generator.generate();
    assertEquals(16, output.length());
    final var characters = new HashSet<Byte>();
    for (byte ch : output.getBytes()) {
      characters.add(ch);
    }
    assertThat(characters.size(), greaterThan(1));
  }

  @Test
  public void genericIntegerGeneratorTest() {
    final AttributeGenerator<Long> generator = new GenericIntegerGenerator(-334, 912);
    final Long output = generator.generate();
    assertThat(output, greaterThanOrEqualTo(Long.valueOf(-334)));
    assertThat(output, lessThan(Long.valueOf(912)));
  }

  @Test
  public void genericDecimalGeneratorTest() {
    final AttributeGenerator<Double> generator = new GenericDecimalGenerator(-1.22, 4.91);
    final Double output = generator.generate();
    assertThat(output, greaterThanOrEqualTo(Double.valueOf(-1.22)));
    assertThat(output, lessThan(Double.valueOf(4.91)));
  }

  @Test
  public void genericBooleanGeneratorTest() {
    final AttributeGenerator<Boolean> generator0 = new GenericBooleanGenerator(0.0);
    final AttributeGenerator<Boolean> generator07 = new GenericBooleanGenerator(0.7);
    final AttributeGenerator<Boolean> generator1 = new GenericBooleanGenerator(1.0);
    int count = 0;
    for (int i = 0; i < 1000; ++i) {
      assertTrue(generator1.generate());
      assertFalse(generator0.generate());
      if (generator07.generate()) {
        ++count;
      }
    }
    final double ratio = (double) count / 1000;
    assertThat(ratio, greaterThan(Double.valueOf(0.5)));
    assertThat(ratio, lessThan(Double.valueOf(0.9)));
  }

  @Test
  public void genericStringEnumGeneratorTest() {
    final AttributeGenerator<String> generator =
        new GenericStringEnumGenerator(
            List.of("ONE", "TWO", "THREE", "FOUR", "FIVE").stream()
                .map((value) -> new AttributeValueGeneric(value, AttributeType.STRING))
                .collect(Collectors.toList()));
    final var values = new HashSet<String>();
    for (int i = 0; i < 100; ++i) {
      values.add(generator.generate());
    }
    assertEquals(5, values.size());
    assertTrue(values.contains("ONE"));
    assertTrue(values.contains("TWO"));
    assertTrue(values.contains("THREE"));
    assertTrue(values.contains("FOUR"));
    assertTrue(values.contains("FIVE"));
  }

  @Test
  public void internalValueStringEnumGeneratorTest() {
    final AttributeGenerator<Integer> generator =
        new InternalValueStringEnumGenerator(
            List.of("ONE", "TWO", "THREE", "FOUR", "FIVE").stream()
                .map((value) -> new AttributeValueGeneric(value, AttributeType.STRING))
                .collect(Collectors.toList()));
    final var values = new HashSet<Integer>();
    for (int i = 0; i < 100; ++i) {
      values.add(generator.generate());
    }
    assertEquals(5, values.size());
    assertTrue(values.contains(0));
    assertTrue(values.contains(1));
    assertTrue(values.contains(2));
    assertTrue(values.contains(3));
    assertTrue(values.contains(4));
  }
}
