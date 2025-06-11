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
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class EqualsTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void attributeValueGenericEquals() {
    final var value1 = new AttributeValueGeneric("hello", AttributeType.STRING);
    assertTrue(value1.equals(value1));

    final var value2 = new AttributeValueGeneric("hello", AttributeType.STRING);
    assertTrue(value1.equals(value2));
    assertEquals(value1.hashCode(), value2.hashCode());

    final var value3 = new AttributeValueGeneric("hi", AttributeType.STRING);
    assertFalse(value1.equals(value3));
    assertNotEquals(value1.hashCode(), value3.hashCode());

    final var value4 = new AttributeValueGeneric(1, AttributeType.INTEGER);
    final var value5 = new AttributeValueGeneric(1, AttributeType.DECIMAL);
    assertFalse(value4.equals(value5));
    assertNotEquals(value4.hashCode(), value5.hashCode());
  }

  @Test
  public void attributeTagsEquals() throws Exception {
    final var src =
        "{"
            + "    'category': 'Quantity',"
            + "    'kind': 'Money',"
            + "    'unit': 'INR',"
            + "    'unitDisplayPosition': 'Prefix',"
            + "    'positiveIndicator': 'High',"
            + "    'firstSummary': 'SUM',"
            + "    'secondSummary': 'AVG'"
            + "  }";

    final var tags = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);

    assertTrue(tags.equals(tags));
    assertFalse(tags.equals(null));
    assertFalse(tags.equals("hello"));
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      assertTrue(tags.equals(tags2));
      assertEquals(tags.hashCode(), tags.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setCategory(AttributeCategory.DESCRIPTION);
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setKind(AttributeKind.DURATION);
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setOtherKindName("whatever");
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setUnitDisplayName("Indian Rupee");
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setUnitDisplayPosition(UnitDisplayPosition.SUFFIX);
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setPositiveIndicator(PositiveIndicator.LOW);
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setFirstSummary(AttributeSummary.DISTINCTCOUNT);
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
    {
      final var tags2 = objectMapper.readValue(src.replace("'", "\""), AttributeTags.class);
      tags2.setSecondSummary(AttributeSummary.DISTINCTCOUNT);
      assertFalse(tags.equals(tags2));
      assertNotEquals(tags.hashCode(), tags2.hashCode());
    }
  }

  @Test
  public void ingestTimeLagEquals() throws Exception {
    final var src =
        "{"
            + "    'ingestTimeLagName': 'deliveryTimeLag',"
            + "    'attribute': 'delivery',"
            + "    'as': 'deliveryDelay',"
            + "    'tags': {"
            + "      'category': 'Quantity',"
            + "      'kind': 'Timestamp',"
            + "      'unit': 'UnixMillisecond'"
            + "    },"
            + "    'fillIn': 0"
            + "  }";

    final var timeLag =
        objectMapper.readValue(src.replace("'", "\""), IngestTimeLagEnrichmentConfig.class);

    assertTrue(timeLag.equals(timeLag));
    assertFalse(timeLag.equals(null));
    assertFalse(timeLag.equals("hello"));
    {
      final var timeLag2 =
          objectMapper.readValue(src.replace("'", "\""), IngestTimeLagEnrichmentConfig.class);
      assertTrue(timeLag.equals(timeLag2));
      assertEquals(timeLag.hashCode(), timeLag2.hashCode());
    }
    {
      final var timeLag2 =
          objectMapper.readValue(src.replace("'", "\""), IngestTimeLagEnrichmentConfig.class);
      timeLag2.setName("wrongName");
      assertFalse(timeLag.equals(timeLag2));
      assertNotEquals(timeLag.hashCode(), timeLag2.hashCode());
    }
    {
      final var timeLag2 =
          objectMapper.readValue(src.replace("'", "\""), IngestTimeLagEnrichmentConfig.class);
      timeLag2.setAttribute("createdTime");
      assertFalse(timeLag.equals(timeLag2));
      assertNotEquals(timeLag.hashCode(), timeLag2.hashCode());
    }
    {
      final var timeLag2 =
          objectMapper.readValue(src.replace("'", "\""), IngestTimeLagEnrichmentConfig.class);
      timeLag2.setAs("createDelay");
      assertFalse(timeLag.equals(timeLag2));
      assertNotEquals(timeLag.hashCode(), timeLag2.hashCode());
    }
    {
      final var timeLag2 =
          objectMapper.readValue(src.replace("'", "\""), IngestTimeLagEnrichmentConfig.class);
      timeLag2.getTags().setUnit(Unit.UNIX_SECOND);
      assertFalse(timeLag.equals(timeLag2));
      assertNotEquals(timeLag.hashCode(), timeLag2.hashCode());
    }
    {
      final var timeLag2 =
          objectMapper.readValue(src.replace("'", "\""), IngestTimeLagEnrichmentConfig.class);
      timeLag2.setFillInSerialized("-1");
      assertFalse(timeLag.equals(timeLag2));
      assertNotEquals(timeLag.hashCode(), timeLag2.hashCode());
    }
  }
}
