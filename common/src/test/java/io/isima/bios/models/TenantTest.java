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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class TenantTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{"
            + "  'tenantName': 'testTenant',"
            + "  'version': 1588197288382,"
            + "  'signals': [{"
            + "    'signalName': 'signal_with_postprocess_example',"
            + "    'version': 1588197288384,"
            + "    'biosVersion': 1588197288385,"
            + "    'missingAttributePolicy': 'Reject',"
            + "    'attributes': ["
            + "      {"
            + "        'attributeName': 'customer_id',"
            + "        'type': 'Integer'"
            + "      },"
            + "      {"
            + "        'attributeName': 'access_point_id',"
            + "        'type': 'Integer'"
            + "      },"
            + "      {"
            + "        'attributeName': 'stay_length',"
            + "        'type': 'Integer'"
            + "      },"
            + "      {"
            + "        'attributeName': 'latitude',"
            + "        'type': 'Decimal'"
            + "      }"
            + "    ]"
            + "  }]"
            + "}";

    final TenantConfig tenant = objectMapper.readValue(src.replace("'", "\""), TenantConfig.class);
    assertEquals("testTenant", tenant.getName());
    assertEquals(1, tenant.getSignals().size());

    final SignalConfig signal = tenant.getSignals().get(0);
    assertEquals("signal_with_postprocess_example", signal.getName());
    assertEquals(Long.valueOf(1588197288384L), signal.getVersion());
    assertEquals(Long.valueOf(1588197288385L), signal.getBiosVersion());
    assertEquals(MissingAttributePolicy.REJECT, signal.getMissingAttributePolicy());
    assertEquals(4, signal.getAttributes().size());
    assertEquals("customer_id", signal.getAttributes().get(0).getName());
    assertEquals(AttributeType.INTEGER, signal.getAttributes().get(0).getType());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertEquals(expected, objectMapper.writeValueAsString(tenant));
  }
}
