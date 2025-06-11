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
import java.util.Arrays;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class EnrichmentTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{"
            + "  'enrichmentName': 'orderStatusPreProcess',"
            + "  'foreignKey': ["
            + "    'statusId'"
            + "  ],"
            + "  'missingLookupPolicy': 'Reject',"
            + "  'contextName': 'orderStatusContext',"
            + "  'contextAttributes': ["
            + "    {"
            + "    'attributeName': 'statusName',"
            + "    'as': 'status'"
            + "    },"
            + "    {"
            + "      'attributeName': 'department'"
            + "    }"
            + "  ]"
            + "}";

    final EnrichmentConfigSignal enrichment =
        objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);

    assertEquals("orderStatusPreProcess", enrichment.getName());
    assertEquals(Arrays.asList("statusId"), enrichment.getForeignKey());
    assertEquals(MissingLookupPolicy.REJECT, enrichment.getMissingLookupPolicy());
    assertEquals("orderStatusContext", enrichment.getContextName());
    assertEquals(2, enrichment.getContextAttributes().size());
    assertEquals("statusName", enrichment.getContextAttributes().get(0).getAttributeName());
    assertEquals("status", enrichment.getContextAttributes().get(0).getAs());
    assertEquals("department", enrichment.getContextAttributes().get(1).getAttributeName());
    assertNull(enrichment.getContextAttributes().get(1).getAs());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertEquals(expected, objectMapper.writeValueAsString(enrichment));

    assertTrue(enrichment.equals(enrichment));
    assertFalse(enrichment.equals(null));
    assertFalse(enrichment.equals("hello"));
    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      assertTrue(enrichment.equals(enrichment2));
      assertEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      enrichment2.setName("another");
      assertFalse(enrichment.equals(enrichment2));
      assertNotEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      enrichment2.setForeignKey(List.of("someKey"));
      assertFalse(enrichment.equals(enrichment2));
      assertNotEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      enrichment2.setMissingLookupPolicy(MissingLookupPolicy.STORE_FILL_IN_VALUE);
      assertFalse(enrichment.equals(enrichment2));
      assertNotEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      enrichment2.setContextName("aaaaa");
      assertFalse(enrichment.equals(enrichment2));
      assertNotEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      enrichment2.getContextAttributes().get(0).setAs("BBBBB");
      assertFalse(enrichment.equals(enrichment2));
      assertNotEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
  }

  @Test
  public void testFillInConfig() throws Exception {
    final String src =
        "{"
            + "  'enrichmentName': 'orderStatusPreProcess',"
            + "  'foreignKey': ["
            + "    'statusId'"
            + "  ],"
            + "  'missingLookupPolicy': 'StoreFillInValue',"
            + "  'contextName': 'orderStatusContext',"
            + "  'contextAttributes': ["
            + "    {"
            + "    'attributeName': 'statusName',"
            + "    'as': 'status',"
            + "    'fillIn': 'MISSING'"
            + "    },"
            + "    {"
            + "      'attributeName': 'department',"
            + "       'fillIn': 'UNKNOWN'"
            + "    }"
            + "  ]"
            + "}";

    final EnrichmentConfigSignal enrichment =
        objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);

    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      assertTrue(enrichment.equals(enrichment2));
      assertEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
    {
      final var enrichment2 =
          objectMapper.readValue(src.replace("'", "\""), EnrichmentConfigSignal.class);
      enrichment2.getContextAttributes().get(0).setAs("UNKNOWN");
      assertFalse(enrichment.equals(enrichment2));
      assertNotEquals(enrichment.hashCode(), enrichment2.hashCode());
    }
  }
}
