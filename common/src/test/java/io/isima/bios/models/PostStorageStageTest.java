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
import java.util.Arrays;
import org.junit.BeforeClass;
import org.junit.Test;

public class PostStorageStageTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {

    final String src =
        "{"
            + "  'features': ["
            + "    {"
            + "      'featureName': 'byCountryState',"
            + "      'dimensions': ['country', 'state'],"
            + "      'attributes': ['stayLength', 'orderId'],"
            + "      'featureInterval': 300000,"
            + "      'alerts': ["
            + "        {"
            + "          'alertName': 'alertForAnomalyStayLength',"
            + "          'condition': 'anomaly(sum(stayLength),min(stayLength),max(stayLength))',"
            + "          'webhookUrl': 'https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422'"
            + "        },"
            + "        {"
            + "          'alertName': 'alertForLongStay',"
            + "          'condition': 'sum(stayLength)/count()>2',"
            + "          'webhookUrl': 'https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422'"
            + "        }"
            + "      ]"
            + "    }"
            + "  ]"
            + "}";

    final PostStorageStageConfig stage =
        objectMapper.readValue(src.replace("'", "\""), PostStorageStageConfig.class);
    assertEquals(1, stage.getFeatures().size());

    final SignalFeatureConfig feature = stage.getFeatures().get(0);
    assertEquals("byCountryState", feature.getName());
    assertEquals(Arrays.asList("country", "state"), feature.getDimensions());
    assertEquals(Arrays.asList("stayLength", "orderId"), feature.getAttributes());
    assertEquals(Long.valueOf(300000), feature.getFeatureInterval());
    assertEquals(2, feature.getAlerts().size());

    assertEquals("alertForAnomalyStayLength", feature.getAlerts().get(0).getName());
    assertEquals(
        "anomaly(sum(stayLength),min(stayLength),max(stayLength))",
        feature.getAlerts().get(0).getCondition());
    assertEquals(
        "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
        feature.getAlerts().get(0).getWebhookUrl());

    assertEquals("alertForLongStay", feature.getAlerts().get(1).getName());
    assertEquals("sum(stayLength)/count()>2", feature.getAlerts().get(1).getCondition());
    assertEquals(
        "https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422",
        feature.getAlerts().get(1).getWebhookUrl());

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertEquals(expected, objectMapper.writeValueAsString(stage));
  }
}
