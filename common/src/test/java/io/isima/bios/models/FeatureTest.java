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
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;

public class FeatureTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {

    final String src =
        "{"
            + "  'featureName': 'byCountryState',"
            + "  'dimensions': ['country', 'state'],"
            + "  'attributes': ['stayLength', 'orderId'],"
            + "  'featureInterval': 300000,"
            + "  'alerts': ["
            + "    {"
            + "      'alertName': 'alertForAnomalyStayLength',"
            + "      'condition': 'anomaly(sum(stayLength),min(stayLength),max(stayLength))',"
            + "      'webhookUrl': 'https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422',"
            + "      'userName': 'test@isima.io',"
            + "      'password': 'Test@123!'"
            + "    },"
            + "    {"
            + "      'alertName': 'alertForLongStay',"
            + "      'condition': 'sum(stayLength)/count()>2',"
            + "      'webhookUrl': 'https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422'"
            + "    }"
            + "  ],"
            + "  'materializedAs': 'AccumulatingCount'"
            + "}";

    final SignalFeatureConfig feature =
        objectMapper.readValue(src.replace("'", "\""), SignalFeatureConfig.class);
    assertEquals("byCountryState", feature.getName());
    assertEquals(Arrays.asList("country", "state"), feature.getDimensions());
    assertEquals(Arrays.asList("stayLength", "orderId"), feature.getAttributes());
    assertEquals(Long.valueOf(300000), feature.getFeatureInterval());
    assertEquals(MaterializedAs.ACCUMULATING_COUNT, feature.getMaterializedAs());
    assertEquals(2, feature.getAlerts().size());

    assertEquals("alertForAnomalyStayLength", feature.getAlerts().get(0).getName());
    assertEquals(
        "anomaly(sum(stayLength),min(stayLength),max(stayLength))",
        feature.getAlerts().get(0).getCondition());
    assertEquals(
        "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
        feature.getAlerts().get(0).getWebhookUrl());
    assertEquals("test@isima.io", feature.getAlerts().get(0).getUserName());
    assertEquals("Test@123!", feature.getAlerts().get(0).getPassword());

    assertEquals("alertForLongStay", feature.getAlerts().get(1).getName());
    assertEquals("sum(stayLength)/count()>2", feature.getAlerts().get(1).getCondition());
    assertEquals(
        "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
        feature.getAlerts().get(1).getWebhookUrl());
    assertNull(feature.getAlerts().get(1).getUserName());
    assertNull(feature.getAlerts().get(1).getPassword());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertEquals(expected, objectMapper.writeValueAsString(feature));
  }
}
