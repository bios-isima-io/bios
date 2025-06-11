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
package io.isima.bios.integrations;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.AttributeType;
import io.isima.bios.models.SourceAttributeSchema;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Test;

public class SourceSchemaFinderTest {

  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    mapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void parseSimpleJsonObject() throws Exception {
    String src =
        "{"
            + "'one':'hello',"
            + "'Two': 123,"
            + "'three': -443.24,"
            + "'Four': false,"
            + "'five': null"
            + "}";

    final var object = mapper.readValue(src.replace("'", "\""), Object.class);

    final var attributes = new ArrayList<SourceAttributeSchema>();
    assertTrue(SourceSchemaFinder.parseJsonObject(object, attributes));
    assertEquals(5, attributes.size());

    var attr = attributes.get(0);
    assertEquals("one", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("hello", attr.getExampleValue());

    attr = attributes.get(1);
    assertEquals("Two", attr.getSourceAttributeName());
    assertEquals(AttributeType.INTEGER, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("number", attr.getOriginalType());
    assertEquals(123L, attr.getExampleValue());

    attr = attributes.get(2);
    assertEquals("three", attr.getSourceAttributeName());
    assertEquals(AttributeType.DECIMAL, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("number", attr.getOriginalType());
    assertEquals(-443.24, attr.getExampleValue());

    attr = attributes.get(3);
    assertEquals("Four", attr.getSourceAttributeName());
    assertEquals(AttributeType.BOOLEAN, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("boolean", attr.getOriginalType());
    assertEquals(false, attr.getExampleValue());

    attr = attributes.get(4);
    assertEquals("five", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(true, attr.getIsNullable());
    assertEquals("null", attr.getOriginalType());
    assertNull(attr.getExampleValue());
  }

  @Test
  public void parseNestedJsonObject() throws Exception {
    String src =
        "{"
            + "  'one':'hello',"
            + "  'amount':123.45,"
            + "  'props': {"
            + "    'user': 'naoki',"
            + "    'phone': 6507226550"
            + "  },"
            + "  'items': ["
            + "    {"
            + "      'itemId': 1,"
            + "      'day': '6/5',"
            + "      'achieved': true,"
            + "      'note': 'well done'"
            + "    },"
            + "    {"
            + "      'itemId': '2',"
            + "      'day': '6/6',"
            + "      'achieved': false,"
            + "      'note': null"
            + "    },"
            + "    {"
            + "      'itemId': 3,"
            + "      'day': '6/7',"
            + "      'achieved': false,"
            + "      'excuse': 'I was sleepy'"
            + "    }"
            + "  ]"
            + "}";

    final var object = mapper.readValue(src.replace("'", "\""), Object.class);

    final var attributes = new ArrayList<SourceAttributeSchema>();
    assertTrue(SourceSchemaFinder.parseJsonObject(object, attributes));
    assertEquals(9, attributes.size());

    var attr = attributes.get(0);
    assertEquals("one", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("hello", attr.getExampleValue());

    attr = attributes.get(1);
    assertEquals("amount", attr.getSourceAttributeName());
    assertEquals(AttributeType.DECIMAL, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("number", attr.getOriginalType());
    assertEquals(123.45, attr.getExampleValue());

    attr = attributes.get(2);
    assertEquals("props/user", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("naoki", attr.getExampleValue());

    attr = attributes.get(3);
    assertEquals("props/phone", attr.getSourceAttributeName());
    assertEquals(AttributeType.INTEGER, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("number", attr.getOriginalType());
    assertEquals(6507226550L, attr.getExampleValue());

    attr = attributes.get(4);
    assertEquals("items/*/itemId", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("number, string", attr.getOriginalType());
    assertEquals(3L, attr.getExampleValue());

    attr = attributes.get(5);
    assertEquals("items/*/day", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("6/7", attr.getExampleValue());

    attr = attributes.get(6);
    assertEquals("items/*/achieved", attr.getSourceAttributeName());
    assertEquals(AttributeType.BOOLEAN, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("boolean", attr.getOriginalType());
    assertEquals(false, attr.getExampleValue());

    attr = attributes.get(7);
    assertEquals("items/*/note", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(true, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("well done", attr.getExampleValue());

    attr = attributes.get(8);
    assertEquals("items/*/excuse", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(true, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("I was sleepy", attr.getExampleValue());
  }

  @Test
  public void parseListValues() throws Exception {
    String src = "{" + "  'title':'hello'," + "  'items': [2, 3, 4, 5]" + "}";

    final var object = mapper.readValue(src.replace("'", "\""), Object.class);

    final var attributes = new ArrayList<SourceAttributeSchema>();
    assertTrue(SourceSchemaFinder.parseJsonObject(object, attributes));
    assertEquals(2, attributes.size());

    var attr = attributes.get(0);
    assertEquals("title", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("hello", attr.getExampleValue());

    attr = attributes.get(1);
    assertEquals("items", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("list", attr.getOriginalType());
    assertEquals(List.of(2L, 3L, 4L, 5L), attr.getExampleValue());
  }

  @Test
  public void parseComplexNest() throws Exception {
    String src =
        "{"
            + "  'props': {"
            + "    'user': 'naoki',"
            + "    'history': ["
            + "      {"
            + "        'date': '4/30/2021',"
            + "        'command': 'ls'"
            + "      }"
            + "    ],"
            + "    'cardInfo': {'address': 'RWC'}"
            + "  },"
            + "  'items': ["
            + "    {"
            + "      'itemId': 1,"
            + "      'tags': ['gcp', 'aws', 'azure'],"
            + "      'meta': {'region': 'us-west1', 'cpu': 8}"
            + "    }"
            + "  ]"
            + "}";

    final var object = mapper.readValue(src.replace("'", "\""), Object.class);

    final var attributes = new ArrayList<SourceAttributeSchema>();
    assertTrue(SourceSchemaFinder.parseJsonObject(object, attributes));
    assertEquals(7, attributes.size());

    var attr = attributes.get(0);
    assertEquals("props/user", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("naoki", attr.getExampleValue());

    attr = attributes.get(1);
    assertEquals("props/history/*/date", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("4/30/2021", attr.getExampleValue());

    attr = attributes.get(2);
    assertEquals("props/history/*/command", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("ls", attr.getExampleValue());

    attr = attributes.get(3);
    assertEquals("props/cardInfo/address", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("string", attr.getOriginalType());
    assertEquals("RWC", attr.getExampleValue());

    attr = attributes.get(4);
    assertEquals("items/*/itemId", attr.getSourceAttributeName());
    assertEquals(AttributeType.INTEGER, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("number", attr.getOriginalType());
    assertEquals(1L, attr.getExampleValue());

    attr = attributes.get(5);
    assertEquals("items/*/tags", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("list", attr.getOriginalType());
    assertEquals(List.of("gcp", "aws", "azure"), attr.getExampleValue());

    attr = attributes.get(6);
    assertEquals("items/*/meta", attr.getSourceAttributeName());
    assertEquals(AttributeType.STRING, attr.getSuggestedType());
    assertEquals(false, attr.getIsNullable());
    assertEquals("object", attr.getOriginalType());
    assertEquals(Map.of("region", "us-west1", "cpu", 8L), attr.getExampleValue());
  }

  @Test
  public void parseFlatValue() throws Exception {
    String src = "'This',is,actually,a,'csv'";
    final var object = mapper.readValue(src.replace("'", "\""), Object.class);
    final var attributes = new ArrayList<SourceAttributeSchema>();
    assertFalse(SourceSchemaFinder.parseJsonObject(object, attributes));
    assertEquals(0, attributes.size());
  }

  @Test
  public void parseFlatArray() throws Exception {
    String src = "[1,2,3,4]";
    final var object = mapper.readValue(src.replace("'", "\""), Object.class);
    final var attributes = new ArrayList<SourceAttributeSchema>();
    assertFalse(SourceSchemaFinder.parseJsonObject(object, attributes));
    assertEquals(0, attributes.size());
  }
}
