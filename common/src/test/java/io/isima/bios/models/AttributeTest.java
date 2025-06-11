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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.BeforeClass;
import org.junit.Test;

public class AttributeTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshaling() throws Exception {
    final String src = "{" + "  'attributeName': 'hello'," + "  'type': 'string'" + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals("hello", attribute.getName());
    assertEquals(AttributeType.STRING, attribute.getType());

    assertTrue(attribute.equals(attribute));
    assertFalse(attribute.equals(null));
    assertFalse(attribute.equals("abc"));

    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      assertTrue(attribute.equals(attribute2));
      assertEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      attribute2.setName("hi");
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      attribute2.setDefaultValue(new AttributeValueGeneric("MISSING", AttributeType.STRING));
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
  }

  @Test
  public void testJsonMarshalingStringEnum() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'gender',"
            + "  'type': 'string',"
            + "  'allowedValues': ['MALE', 'FEMALE', 'UNKNOWN'],"
            + "  'missingAttributePolicy': 'storeDefaultValue',"
            + "  'default': 'UNKNOWN'"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals("gender", attribute.getName());
    assertEquals(AttributeType.STRING, attribute.getType());
    assertEquals(
        Arrays.asList("MALE", "FEMALE", "UNKNOWN"),
        attribute.getAllowedValues().stream()
            .map(AttributeValueGeneric::asString)
            .collect(Collectors.toList()));
    assertEquals(MissingAttributePolicy.STORE_DEFAULT_VALUE, attribute.getMissingAttributePolicy());
    assertEquals("UNKNOWN", attribute.getDefaultValue().asString());

    assertEquals(
        "{\"attributeName\":\"gender\",\"type\":\"String\","
            + "\"allowedValues\":[\"MALE\",\"FEMALE\",\"UNKNOWN\"],"
            + "\"missingAttributePolicy\":\"StoreDefaultValue\","
            + "\"default\":\"UNKNOWN\"}",
        objectMapper.writeValueAsString(attribute));

    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      assertTrue(attribute.equals(attribute2));
      assertEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      attribute2.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      final var newAllowedValues = new ArrayList<>(attribute2.getAllowedValues());
      newAllowedValues.add(new AttributeValueGeneric("UNSPECIFIED", AttributeType.STRING));
      attribute2.setAllowedValues(newAllowedValues);
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
  }

  @Test
  public void testJsonMarshalingIntegerEnum() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'zipCode',"
            + "  'type': 'integer',"
            + "  'allowedValues': [94061, 94063, 94067],"
            + "  'missingAttributePolicy': 'storeDefaultValue',"
            + "  'default': 94067"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals("zipCode", attribute.getName());
    assertEquals(AttributeType.INTEGER, attribute.getType());
    assertEquals(
        Arrays.asList(94061L, 94063L, 94067L),
        attribute.getAllowedValues().stream()
            .map(AttributeValueGeneric::asLong)
            .collect(Collectors.toList()));
    assertEquals(MissingAttributePolicy.STORE_DEFAULT_VALUE, attribute.getMissingAttributePolicy());
    assertEquals(94067L, attribute.getDefaultValue().asLong());

    assertEquals(
        "{\"attributeName\":\"zipCode\",\"type\":\"Integer\","
            + "\"allowedValues\":[94061,94063,94067],"
            + "\"missingAttributePolicy\":\"StoreDefaultValue\","
            + "\"default\":94067}",
        objectMapper.writeValueAsString(attribute));
  }

  @Test
  public void testJsonMarshalingDecimalEnum() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'anchorPoints',"
            + "  'type': 'decimal',"
            + "  'allowedValues': [-0.29, 1.12, 987.554],"
            + "  'missingAttributePolicy': 'storeDefaultValue',"
            + "  'default': -0.29"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals("anchorPoints", attribute.getName());
    assertEquals(AttributeType.DECIMAL, attribute.getType());
    assertEquals(
        Arrays.asList(-0.29, 1.12, 987.554),
        attribute.getAllowedValues().stream()
            .map(AttributeValueGeneric::asDouble)
            .collect(Collectors.toList()));
    assertEquals(MissingAttributePolicy.STORE_DEFAULT_VALUE, attribute.getMissingAttributePolicy());
    assertEquals(-0.29, attribute.getDefaultValue().asDouble(), 0.0);

    assertEquals(
        "{\"attributeName\":\"anchorPoints\",\"type\":\"Decimal\","
            + "\"allowedValues\":[-0.29,1.12,987.554],"
            + "\"missingAttributePolicy\":\"StoreDefaultValue\","
            + "\"default\":-0.29}",
        objectMapper.writeValueAsString(attribute));
  }

  @Test
  public void testJsonMarshalingBooleanEnum() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'isThisMeaningful',"
            + "  'type': 'boolean',"
            + "  'allowedValues': [true, false],"
            + "  'missingAttributePolicy': 'storeDefaultValue',"
            + "  'default': true"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals("isThisMeaningful", attribute.getName());
    assertEquals(AttributeType.BOOLEAN, attribute.getType());
    assertEquals(
        Arrays.asList(true, false),
        attribute.getAllowedValues().stream()
            .map(AttributeValueGeneric::asBoolean)
            .collect(Collectors.toList()));
    assertEquals(MissingAttributePolicy.STORE_DEFAULT_VALUE, attribute.getMissingAttributePolicy());
    assertEquals(Boolean.TRUE, attribute.getDefaultValue().asBoolean());

    assertEquals(
        "{\"attributeName\":\"isThisMeaningful\",\"type\":\"Boolean\","
            + "\"allowedValues\":[true,false],"
            + "\"missingAttributePolicy\":\"StoreDefaultValue\","
            + "\"default\":true}",
        objectMapper.writeValueAsString(attribute));
  }

  @Test
  public void testJsonMarshalingBlobEnum() throws Exception {
    final byte[] dummyPhoto1 = new byte[51];
    final byte[] dummyPhoto2 = new byte[128];
    final Random random = new Random();
    random.nextBytes(dummyPhoto1);
    random.nextBytes(dummyPhoto2);
    final String encodedPhoto1 = Base64.getEncoder().encodeToString(dummyPhoto1);
    final String encodedPhoto2 = Base64.getEncoder().encodeToString(dummyPhoto2);
    final String src =
        "{"
            + "  'attributeName': 'photo',"
            + "  'type': 'Blob',"
            + "  'allowedValues': ['"
            + encodedPhoto1
            + "', '"
            + encodedPhoto2
            + "'],"
            + "  'missingAttributePolicy': 'StoreDefaultValue',"
            + "  'default': '"
            + encodedPhoto1
            + "'"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals("photo", attribute.getName());
    assertEquals(AttributeType.BLOB, attribute.getType());
    assertEquals(
        Arrays.asList(new String(dummyPhoto1), new String(dummyPhoto2)),
        attribute.getAllowedValues().stream()
            .map(entry -> new String(entry.asByteArray()))
            .collect(Collectors.toList()));
    assertEquals(MissingAttributePolicy.STORE_DEFAULT_VALUE, attribute.getMissingAttributePolicy());
    assertArrayEquals(dummyPhoto1, attribute.getDefaultValue().asByteArray());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");

    assertEquals(expected, objectMapper.writeValueAsString(attribute));
  }

  @Test
  public void testJsonMarshalingDataSketchFields() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'orderAmount',"
            + "  'type': 'Decimal',"
            + "  'tags': {"
            + "    'category': 'Quantity',"
            + "    'kind': 'Money',"
            + "    'unit': 'INR',"
            + "    'unitDisplayPosition': 'Prefix',"
            + "    'positiveIndicator': 'High',"
            + "    'firstSummary': 'SUM',"
            + "    'secondSummary': 'AVG'"
            + "  }"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals(AttributeCategory.QUANTITY, attribute.getTags().getCategory());
    assertEquals(AttributeKind.MONEY, attribute.getTags().getKind());
    assertEquals(Unit.I_N_R, attribute.getTags().getUnit());
    assertEquals(UnitDisplayPosition.PREFIX, attribute.getTags().getUnitDisplayPosition());
    assertEquals(PositiveIndicator.HIGH, attribute.getTags().getPositiveIndicator());
    assertEquals(AttributeSummary.SUM, attribute.getTags().getFirstSummary());
    assertEquals(AttributeSummary.AVG, attribute.getTags().getSecondSummary());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");

    assertEquals(expected, objectMapper.writeValueAsString(attribute));

    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      assertTrue(attribute.equals(attribute2));
      assertEquals(attribute.hashCode(), attribute2.hashCode());
    }
    {
      final AttributeConfig attribute2 =
          objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
      attribute2.getTags().setUnit(Unit.U_S_D);
      assertFalse(attribute.equals(attribute2));
      assertNotEquals(attribute.hashCode(), attribute2.hashCode());
    }
  }

  @Test
  public void testJsonMarshalingDataSketchFields2() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'overhead',"
            + "  'type': 'Integer',"
            + "  'tags': {"
            + "    'category': 'Quantity',"
            + "    'kind': 'OtherKind',"
            + "    'otherKindName': 'BloodPressure',"
            + "    'unitDisplayName': 'mm',"
            + "    'unitDisplayPosition': 'Suffix',"
            + "    'firstSummary': 'AVG'"
            + "  }"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals(AttributeCategory.QUANTITY, attribute.getTags().getCategory());
    assertEquals(AttributeKind.OTHER_KIND, attribute.getTags().getKind());
    assertEquals("BloodPressure", attribute.getTags().getOtherKindName());
    assertNull(attribute.getTags().getUnit());
    assertEquals("mm", attribute.getTags().getUnitDisplayName());
    assertEquals(UnitDisplayPosition.SUFFIX, attribute.getTags().getUnitDisplayPosition());
    assertEquals(AttributeSummary.AVG, attribute.getTags().getFirstSummary());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");

    assertEquals(expected, objectMapper.writeValueAsString(attribute));
  }

  @Test
  public void testJsonMarshalingDataSketchFields3() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'orderAmount',"
            + "  'type': 'String',"
            + "  'tags': {"
            + "    'category': 'Dimension'"
            + "  }"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals(AttributeCategory.DIMENSION, attribute.getTags().getCategory());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");

    assertEquals(expected, objectMapper.writeValueAsString(attribute));
  }

  @Test
  public void testCopyConstructor() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'deliveryCharge',"
            + "  'type': 'Decimal',"
            + "  'tags': {"
            + "    'category': 'Quantity',"
            + "    'kind': 'Money',"
            + "    'unit': 'INR',"
            + "    'unitDisplayPosition': 'Prefix',"
            + "    'positiveIndicator': 'Low',"
            + "    'firstSummary': 'SUM',"
            + "    'secondSummary': 'AVG'"
            + "  }"
            + "}";
    final AttributeConfig originalAttribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    final AttributeConfig attribute = new AttributeConfig(originalAttribute);
    assertEquals("deliveryCharge", attribute.getName());
    assertEquals(AttributeType.DECIMAL, attribute.getType());
    assertEquals(AttributeCategory.QUANTITY, attribute.getTags().getCategory());
    assertEquals(AttributeKind.MONEY, attribute.getTags().getKind());
    assertEquals(Unit.I_N_R, attribute.getTags().getUnit());
    assertEquals(UnitDisplayPosition.PREFIX, attribute.getTags().getUnitDisplayPosition());
    assertEquals(PositiveIndicator.LOW, attribute.getTags().getPositiveIndicator());
    assertEquals(AttributeSummary.SUM, attribute.getTags().getFirstSummary());
    assertEquals(AttributeSummary.AVG, attribute.getTags().getSecondSummary());
    assertNull(attribute.getAllowedValues());
    assertNull(attribute.getMissingAttributePolicy());
    assertNull(attribute.getDefaultValue());
  }

  @Test(expected = JsonMappingException.class)
  public void testInvalidDataSketchField() throws Exception {
    // 'Dimensions' plural form is invalid.
    final String src =
        "{"
            + "  'attributeName': 'zipcode',"
            + "  'type': 'Integer',"
            + "  'tags': {"
            + "    'category': 'Dimensions'"
            + "  }"
            + "}";
    objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
  }

  @Test
  public void testJsonStringEnumEntryConflict() throws Exception {
    final String src =
        "{"
            + "  'attributeName': 'gender',"
            + "  'type': 'string',"
            + "  'allowedValues': ['ONE', 'TWO', 'THREE', 'TWO']"
            + "}";
    final AttributeConfig attribute =
        objectMapper.readValue(src.replace("'", "\""), AttributeConfig.class);
    assertEquals("gender", attribute.getName());
    assertEquals(AttributeType.STRING, attribute.getType());
    assertEquals(
        Arrays.asList("ONE", "TWO", "THREE", "TWO"),
        attribute.getAllowedValues().stream()
            .map(AttributeValueGeneric::asString)
            .collect(Collectors.toList()));
  }
}
