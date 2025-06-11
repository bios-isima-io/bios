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

public class EnrichTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{"
            + "  'missingLookupPolicy': 'StoreFillInValue',"
            + "  'enrichments': ["
            + "    {"
            + "      'enrichmentName': 'orderStatusPreProcess',"
            + "      'foreignKey': ["
            + "        'statusId'"
            + "      ],"
            + "      'missingLookupPolicy': 'Reject',"
            + "      'contextName': 'orderStatusContext',"
            + "      'contextAttributes': ["
            + "        {"
            + "        'attributeName': 'statusName',"
            + "        'as': 'status'"
            + "        },"
            + "        {"
            + "          'attributeName': 'department'"
            + "        }"
            + "      ]"
            + "    }"
            + "  ]"
            + "}";

    final EnrichConfig enrich = objectMapper.readValue(src.replace("'", "\""), EnrichConfig.class);
    assertEquals(MissingLookupPolicy.STORE_FILL_IN_VALUE, enrich.getMissingLookupPolicy());
    assertEquals(1, enrich.getEnrichments().size());

    final EnrichmentConfigSignal enrichment = enrich.getEnrichments().get(0);
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
    assertEquals(expected, objectMapper.writeValueAsString(enrich));

    assertTrue(enrich.equals(enrich));
    assertFalse(enrich.equals(null));
    assertFalse(enrich.equals("hello"));
    {
      final var enrich2 = objectMapper.readValue(src.replace("'", "\""), EnrichConfig.class);
      assertTrue(enrich.equals(enrich2));
      assertEquals(enrich.hashCode(), enrich2.hashCode());
    }
    {
      final var enrich2 = objectMapper.readValue(src.replace("'", "\""), EnrichConfig.class);
      enrich2.setMissingLookupPolicy(MissingLookupPolicy.REJECT);
      assertFalse(enrich.equals(enrich2));
      assertNotEquals(enrich.hashCode(), enrich2.hashCode());
    }
    {
      final var enrich2 = objectMapper.readValue(src.replace("'", "\""), EnrichConfig.class);
      enrich2.getEnrichments().get(0).setName("whatever");
      assertFalse(enrich.equals(enrich2));
      assertNotEquals(enrich.hashCode(), enrich2.hashCode());
    }
    {
      final var enrich2 = objectMapper.readValue(src.replace("'", "\""), EnrichConfig.class);
      final var timeLag = new IngestTimeLagEnrichmentConfig("delay", "delay");
      enrich2.setIngestTimeLag(List.of(timeLag));
      assertFalse(enrich.equals(enrich2));
      assertNotEquals(enrich.hashCode(), enrich2.hashCode());
    }
  }
}
