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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignalTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{"
            + "  'signalName': 'signal_with_postprocess_example',"
            + "  'version': 1588197288384,"
            + "  'biosVersion': 1588197288385,"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {"
            + "      'attributeName': 'customer_id',"
            + "      'type': 'Integer',"
            + "      'tags': {"
            + "        'category': 'Dimension'"
            + "       }"
            + "    },"
            + "    {"
            + "      'attributeName': 'access_point_id',"
            + "      'type': 'Integer'"
            + "    },"
            + "    {"
            + "      'attributeName': 'stay_length',"
            + "      'type': 'Integer'"
            + "    },"
            + "    {"
            + "      'attributeName': 'latitude',"
            + "      'type': 'Decimal'"
            + "    },"
            + "    {"
            + "      'attributeName': 'longitude',"
            + "      'type': 'Decimal',"
            + "      'tags': {"
            + "        'category': 'Quantity'"
            + "       }"
            + "    },"
            + "    {"
            + "      'attributeName': 'customer_name',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'gender',"
            + "      'type': 'String',"
            + "      'allowedValues': ["
            + "        'MALE',"
            + "        'FEMALE',"
            + "        'UNKNOWN'"
            + "      ]"
            + "    },"
            + "    {"
            + "      'attributeName': 'country',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'state',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'createTimestamp',"
            + "      'type': 'Integer',"
            + "      'tags': {'category': 'Quantity', 'kind': 'Timestamp', 'unit': 'UnixSecond'}"
            + "    }"
            + "  ],"
            + "  'enrich': {"
            + "    'missingLookupPolicy': 'StoreFillInValue',"
            + "    'enrichments': ["
            + "      {"
            + "        'enrichmentName': 'join_customer_info',"
            + "        'foreignKey': ["
            + "          'customer_id'"
            + "        ],"
            + "        'missingLookupPolicy': 'StoreFillInValue',"
            + "        'contextName': 'user_context',"
            + "        'contextAttributes': ["
            + "          {"
            + "            'attributeName': 'name',"
            + "            'as': 'customer_name',"
            + "            'fillIn': 'n/a'"
            + "          },"
            + "          {"
            + "            'attributeName': 'gender',"
            + "            'fillIn': 'UNKNOWN'"
            + "          }"
            + "        ]"
            + "      },"
            + "      {"
            + "        'enrichmentName': 'join_access_point_location',"
            + "        'foreignKey': ['access_point_id'],"
            + "        'missingLookupPolicy': 'Reject',"
            + "        'contextName': 'access_point_context',"
            + "        'contextAttributes': ["
            + "          {"
            + "            'attributeName': 'country'"
            + "          },"
            + "          {"
            + "            'attributeName': 'state'"
            + "          },"
            + "          {"
            + "            'attributeName': 'city'"
            + "          }"
            + "        ]"
            + "      }"
            + "    ],"
            + "    'ingestTimeLag': ["
            + "      {"
            + "        'ingestTimeLagName': 'lagSinceCreateTime',"
            + "        'attribute': 'createTimestamp',"
            + "        'as': 'ingestTimeLag',"
            + "        'tags': {'category': 'Quantity', 'kind': 'Duration', 'unit': 'Second'},"
            + "        'fillIn': -1.0"
            + "      }"
            + "    ]"
            + "  },"
            + "  'postStorageStage': {"
            + "    'features': ["
            + "      {"
            + "        'featureName': 'by_country_state',"
            + "        'dimensions': ["
            + "          'country',"
            + "          'state'"
            + "        ],"
            + "        'attributes': ["
            + "          'stay_length',"
            + "          'order_id'"
            + "        ],"
            + "        'dataSketches': ['Moments'],"
            + "        'featureInterval': 300000,"
            + "        'alerts': ["
            + "          {"
            + "            'alertName': 'alertForAnomalyStayLength',"
            + "            'condition': 'anomaly(sum(stayLength),min(stayLength),max(stayLength))',"
            + "            'webhookUrl': 'https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422'"
            + "          },"
            + "          {"
            + "            'alertName': 'alertForLongStay',"
            + "            'condition': '(sum(stay_length)/count())>2',"
            + "            'webhookUrl': 'https://webhook.site/99743393-3a47-473f-8676-319e8c5d9422'"
            + "          }"
            + "        ]"
            + "      }"
            + "    ]"
            + "  }"
            + "}";

    final SignalConfig signal = objectMapper.readValue(src.replace("'", "\""), SignalConfig.class);
    assertEquals("signal_with_postprocess_example", signal.getName());
    assertEquals(Long.valueOf(1588197288384L), signal.getVersion());
    assertEquals(Long.valueOf(1588197288385L), signal.getBiosVersion());
    assertEquals(MissingAttributePolicy.REJECT, signal.getMissingAttributePolicy());
    assertEquals(10, signal.getAttributes().size());
    assertEquals("customer_id", signal.getAttributes().get(0).getName());
    assertEquals(AttributeType.INTEGER, signal.getAttributes().get(0).getType());
    assertEquals(
        MissingLookupPolicy.STORE_FILL_IN_VALUE, signal.getEnrich().getMissingLookupPolicy());
    assertEquals(2, signal.getEnrich().getEnrichments().size());
    assertEquals("join_customer_info", signal.getEnrich().getEnrichments().get(0).getName());
    assertEquals(1, signal.getPostStorageStage().getFeatures().size());
    assertEquals("by_country_state", signal.getPostStorageStage().getFeatures().get(0).getName());
    assertEquals(
        DataSketchType.MOMENTS,
        signal.getPostStorageStage().getFeatures().get(0).getDataSketches().get(0));
    final var ingestTimeLag = signal.getEnrich().getIngestTimeLag();
    assertNotNull(ingestTimeLag);
    assertEquals(1, ingestTimeLag.size());
    assertEquals("lagSinceCreateTime", ingestTimeLag.get(0).getName());
    assertEquals("createTimestamp", ingestTimeLag.get(0).getAttribute());
    assertEquals("ingestTimeLag", ingestTimeLag.get(0).getAs());
    assertEquals(AttributeCategory.QUANTITY, ingestTimeLag.get(0).getTags().getCategory());
    assertEquals(AttributeKind.DURATION, ingestTimeLag.get(0).getTags().getKind());
    assertEquals(Unit.SECOND, ingestTimeLag.get(0).getTags().getUnit());
    assertEquals("-1.0", ingestTimeLag.get(0).getFillInSerialized());

    signal
        .getEnrich()
        .getIngestTimeLag()
        .get(0)
        .setFillIn(new AttributeValueGeneric(-1.0, AttributeType.DECIMAL));
    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertEquals(expected, objectMapper.writeValueAsString(signal));

    // copy constructor test
    final var clone = new SignalConfig(signal);
    assertThat(clone.getName(), is(signal.getName()));
    assertThat(clone.getVersion(), is(signal.getVersion()));
    assertThat(clone.getBiosVersion(), is(signal.getBiosVersion()));
    assertThat(clone.getMissingAttributePolicy(), is(signal.getMissingAttributePolicy()));
    assertThat(clone.getEnrich(), is(signal.getEnrich()));
    assertThat(clone.getPostStorageStage(), is(signal.getPostStorageStage()));
    assertThat(clone.getEnrich().getIngestTimeLag(), is(signal.getEnrich().getIngestTimeLag()));
  }

  @Test
  public void testEnrichmentWithFillin() throws Exception {
    final var signal = new SignalConfig();
    signal.setName("signalWithFillin");
    signal.setVersion(1596644386208L);
    signal.setMissingAttributePolicy(MissingAttributePolicy.REJECT);
    signal.setAttributes(
        List.of(
            new AttributeConfig("incidentId", AttributeType.INTEGER),
            new AttributeConfig("zipCode", AttributeType.INTEGER)));
    signal.setEnrich(new EnrichConfig());
    signal.getEnrich().setEnrichments(List.of(new EnrichmentConfigSignal()));
    signal.getEnrich().getEnrichments().get(0).setName("joinAreaName");
    signal.getEnrich().getEnrichments().get(0).setForeignKey(List.of("zipCode"));
    signal
        .getEnrich()
        .getEnrichments()
        .get(0)
        .setMissingLookupPolicy(MissingLookupPolicy.STORE_FILL_IN_VALUE);
    signal.getEnrich().getEnrichments().get(0).setContextName("theContext");
    signal
        .getEnrich()
        .getEnrichments()
        .get(0)
        .setContextAttributes(List.of(new EnrichmentAttribute()));
    signal
        .getEnrich()
        .getEnrichments()
        .get(0)
        .getContextAttributes()
        .get(0)
        .setAttributeName("areaName");
    signal
        .getEnrich()
        .getEnrichments()
        .get(0)
        .getContextAttributes()
        .get(0)
        .setFillIn(new AttributeValueGeneric("n/a", AttributeType.STRING));

    final var serialized = objectMapper.writeValueAsString(signal);
    final var expected =
        "{'signalName':'signalWithFillin','version':1596644386208,"
            + "'missingAttributePolicy':'Reject',"
            + "'attributes':[{'attributeName':'incidentId','type':'Integer'},"
            + "{'attributeName':'zipCode','type':'Integer'}],"
            + "'enrich':{'enrichments':["
            + "{'enrichmentName':'joinAreaName','foreignKey':['zipCode'],"
            + "'missingLookupPolicy':'StoreFillInValue',"
            + "'contextName':'theContext',"
            + "'contextAttributes':[{'attributeName':'areaName','fillIn':'n/a'}]}]}}";
    assertEquals(expected.replace("'", "\""), serialized);
  }

  @Test
  public void testConstructorWithName() {
    final var signal = new SignalConfig("testTestSignalName");
    assertThat(signal.getName(), is("testTestSignalName"));
  }
}
