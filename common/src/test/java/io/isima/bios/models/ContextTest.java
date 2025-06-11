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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{"
            + "  'contextName': 'contextExample',"
            + "  'version': 1588175368661,"
            + "  'biosVersion': 1588175368660,"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {"
            + "      'attributeName': 'resourceId',"
            + "      'type': 'String',"
            + "      'tags': {"
            + "        'category': 'Dimension'"
            + "       }"
            + "    },"
            + "    {"
            + "      'attributeName': 'resourceTypeNum',"
            + "      'type': 'Integer'"
            + "    },"
            + "    {"
            + "      'attributeName': 'zipcode',"
            + "      'type': 'String',"
            + "      'tags': {"
            + "        'category': 'Description'"
            + "       },"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': 'missing_zipcode'"
            + "    },"
            + "    {"
            + "      'attributeName': 'lastUpdated',"
            + "      'type': 'Integer'"
            + "    }"
            + "  ],"
            + "  'primaryKey': ["
            + "    'resourceId'"
            + "  ],"
            + "  'missingLookupPolicy': 'FailParentLookup',"
            + "  'enrichments': ["
            + "    {"
            + "      'enrichmentName': 'resourceType',"
            + "      'foreignKey': ['resourceTypeNum'],"
            + "      'enrichedAttributes': ["
            + "        { 'value': 'resourceType.resourceTypeCode' },"
            + "        { 'value': 'resourceType.resourceCategory', 'as': 'myResourceCategory'}"
            + "      ]"
            + "    },"
            + "    {"
            + "      'enrichmentName': 'orderStats',"
            + "      'foreignKey': ['zipcode'],"
            + "      'missingLookupPolicy': 'StoreFillInValue',"
            + "      'enrichedAttributes': ["
            + "        { 'value': 'orders:byZipcode.count', 'fillIn': '0'},"
            + "        { 'value': 'orders:byZipcode.deliveryTime_sum', 'fillIn': '0'},"
            + "        { 'value': 'orders:byZipcode.deliveryTime_max', 'fillIn': '0'}"
            + "      ]"
            + "    },"
            + "    {"
            + "      'enrichmentName': 'atcStats',"
            + "      'foreignKey': ['zipcode'],"
            + "      'missingLookupPolicy': 'StoreFillInValue',"
            + "      'enrichedAttributes': ["
            + "        { 'value': 'cartStatistics.atcCount', 'fillIn': '0'}"
            + "      ]"
            + "    },"
            + "    {"
            + "      'enrichmentName': 'resourceDetails',"
            + "      'foreignKey': ['resourceId'],"
            + "      'enrichedAttributes': ["
            + "        {"
            + "          'valuePickFirst': ["
            + "            'staff.cost',"
            + "            'doctor.salary',"
            + "            'venue.cost',"
            + "            'auxiliary.cost',"
            + "            'machine.cost',"
            + "            'workspace.rent'"
            + "            ],"
            + "          'as': 'cost'"
            + "        },"
            + "        {"
            + "          'valuePickFirst': ["
            + "            'staff.aliasName',"
            + "            'doctor.aliasName',"
            + "            'venue.venueId',"
            + "            'auxiliary.auxiliaryId',"
            + "            'machine.machineId',"
            + "            'workspace.workspaceId'"
            + "          ],"
            + "          'as': 'resourceValue'"
            + "        }"
            + "      ]"
            + "    }"
            + "  ],"
            + "  'features': ["
            + "    {"
            + "      'featureName': 'index1',"
            + "      'dimensions': ['resourceId'],"
            + "      'attributes': ['zipcode', 'myResourceCategory'],"
            + "      'aggregated': false,"
            + "      'indexed': true,"
            + "      'indexType': 'RangeQuery'"
            + "    },"
            + "    {"
            + "      'featureName': 'index2',"
            + "      'dimensions': ['zipcode'],"
            + "      'attributes': ['resourceId'],"
            + "      'aggregated': true,"
            + "      'indexed': true,"
            + "      'indexType': 'ExactMatch'"
            + "    },"
            + "    {"
            + "      'featureName': 'feature1',"
            + "      'dimensions': ['resourceTypeCode', 'cost'],"
            + "      'attributes': ['resourceId'],"
            + "      'aggregated': true,"
            + "      'indexed': false"
            + "    },"
            + "    {"
            + "      'featureName': 'feature2',"
            + "      'dimensions': ['resourceTypeCode'],"
            + "      'attributes': ['cost'],"
            + "      'aggregated': true"
            + "    }"
            + "  ],"
            + "  'ttl': 300000"
            + "}";

    final ContextConfig context =
        objectMapper.readValue(src.replace("'", "\""), ContextConfig.class);

    assertEquals("contextExample", context.getName());
    assertEquals(Long.valueOf(1588175368661L), context.getVersion());
    assertEquals(Long.valueOf(1588175368660L), context.getBiosVersion());
    assertEquals(MissingAttributePolicy.REJECT, context.getMissingAttributePolicy());
    assertEquals(4, context.getAttributes().size());
    assertEquals("resourceId", context.getAttributes().get(0).getName());
    assertEquals(AttributeType.STRING, context.getAttributes().get(0).getType());
    assertEquals("resourceTypeNum", context.getAttributes().get(1).getName());
    assertEquals(AttributeType.INTEGER, context.getAttributes().get(1).getType());
    assertEquals(
        MissingAttributePolicy.STORE_DEFAULT_VALUE,
        context.getAttributes().get(2).getMissingAttributePolicy());
    assertEquals("missing_zipcode", context.getAttributes().get(2).getDefaultValue().asString());

    assertEquals(1, context.getPrimaryKey().size());
    assertEquals("resourceId", context.getPrimaryKey().get(0));

    assertEquals(MissingLookupPolicy.FAIL_PARENT_LOOKUP, context.getMissingLookupPolicy());
    assertEquals(4, context.getEnrichments().size());
    assertEquals("resourceType", context.getEnrichments().get(0).getName());
    assertEquals("zipcode", context.getEnrichments().get(1).getForeignKey().get(0));
    assertEquals(
        MissingLookupPolicy.STORE_FILL_IN_VALUE,
        context.getEnrichments().get(2).getMissingLookupPolicy());
    assertEquals(2, context.getEnrichments().get(3).getEnrichedAttributes().size());
    assertEquals(
        "staff.cost",
        context.getEnrichments().get(3).getEnrichedAttributes().get(0).getValuePickFirst().get(0));
    assertEquals(
        "resourceValue", context.getEnrichments().get(3).getEnrichedAttributes().get(1).getAs());
    assertEquals(Long.valueOf(300000L), context.getTtl());

    assertEquals(4, context.getFeatures().size());
    assertEquals("index1", context.getFeatures().get(0).getName());
    assertEquals("index2", context.getFeatures().get(1).getName());
    assertEquals(true, context.getFeatures().get(1).getIndexed());
    assertEquals("zipcode", context.getFeatures().get(1).getDimensions().get(0));

    final String expected = src.replace("'", "\"").replace(" ", "").replace("\n", "");
    assertEquals(expected, objectMapper.writeValueAsString(context));

    // copy constructor test
    final var clone = new ContextConfig(context);
    assertThat(clone.getName(), is(context.getName()));
    assertThat(clone.getVersion(), is(context.getVersion()));
    assertThat(clone.getBiosVersion(), is(context.getBiosVersion()));
    assertThat(clone.getMissingAttributePolicy(), is(context.getMissingAttributePolicy()));
    assertThat(clone.getAttributes(), is(context.getAttributes()));
    assertThat(clone.getPrimaryKey(), is(context.getPrimaryKey()));
    assertThat(clone.getMissingLookupPolicy(), is(context.getMissingLookupPolicy()));
    assertThat(clone.getEnrichments(), is(context.getEnrichments()));
    assertThat(clone.getTtl(), is(context.getTtl()));
    assertThat(clone.getFeatures(), is(context.getFeatures()));
  }

  @Test
  public void testConstructorWithName() {
    final var contextConfig = new ContextConfig("geoIp");
    assertThat(contextConfig.getName(), is("geoIp"));
  }
}
