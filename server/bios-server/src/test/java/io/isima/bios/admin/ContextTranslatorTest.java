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
package io.isima.bios.admin;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exceptions.validator.ConstraintViolationValidatorException;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.ContextConfig;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.MissingLookupPolicy;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ContextTranslatorTest {
  private static ObjectMapper objectMapper;

  private ContextConfig contextConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Before
  public void setUp() throws Exception {
    final String src =
        "{"
            + "  'contextName': 'contextExample',"
            + "  'version': 1588175368661,"
            + "  'biosVersion': 1588175368660,"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {"
            + "      'attributeName': 'userName',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'abc',"
            + "      'type': 'Integer',"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': -1"
            + "    },"
            + "    {"
            + "      'attributeName': 'stringEnum',"
            + "      'type': 'String',"
            + "      'allowedValues': ['UNKNOWN', 'MALE', 'FEMALE'],"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': 'UNKNOWN'"
            + "    },"
            + "    {"
            + "      'attributeName': 'resourceId',"
            + "      'type': 'String'"
            + "    },"
            + "    {"
            + "      'attributeName': 'resourceTypeNum',"
            + "      'type': 'Integer'"
            + "    },"
            + "    {"
            + "      'attributeName': 'zipcode',"
            + "      'type': 'String',"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': 'missing_zipcode'"
            + "    },"
            + "    {"
            + "      'attributeName': 'lastUpdated',"
            + "      'type': 'Integer'"
            + "    }"
            + "  ],"
            + "  'primaryKey': ["
            + "    'userName'"
            + "  ],"
            + "  'versionAttribute': 'lastUpdated',"
            + "  'dataSynthesisStatus': 'Enabled',"
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
            + "  'ttl': 86400000,"
            + "  'features': ["
            + "    {"
            + "      'featureName': 'index1',"
            + "      'dimensions': ['resourceId'],"
            + "      'attributes': ['zipcode', 'myResourceCategory'],"
            + "      'dataSketches': [],"
            + "      'indexed': true,"
            + "      'indexType': 'RangeQuery'"
            + "    },"
            + "    {"
            + "      'featureName': 'index2',"
            + "      'dimensions': ['zipcode'],"
            + "      'attributes': ['resourceId'],"
            + "      'dataSketches': [],"
            + "      'indexed': true,"
            + "      'indexType': 'ExactMatch'"
            + "    },"
            + "    {"
            + "      'featureName': 'index3',"
            + "      'dimensions': ['resourceTypeCode', 'cost'],"
            + "      'attributes': ['resourceId'],"
            + "      'dataSketches': [],"
            + "      'indexed': true,"
            + "      'indexType': 'RangeQuery'"
            + "    }"
            + "  ]"
            + "}";
    contextConfig = objectMapper.readValue(src.replace("'", "\""), ContextConfig.class);
  }

  @Test
  public void testBasic() throws ValidatorException {
    final StreamConfig streamConfig = Translators.toTfos(contextConfig);
    assertEquals(StreamType.CONTEXT, streamConfig.getType());
    assertEquals("contextExample", streamConfig.getName());
    assertEquals(Long.valueOf(1588175368661L), streamConfig.getVersion());
    assertEquals(MissingAttributePolicyV1.STRICT, streamConfig.getMissingValuePolicy());

    assertEquals(7, streamConfig.getAttributes().size());

    assertEquals("userName", streamConfig.getAttributes().get(0).getName());
    assertEquals(
        InternalAttributeType.STRING, streamConfig.getAttributes().get(0).getAttributeType());

    assertEquals("abc", streamConfig.getAttributes().get(1).getName());
    assertEquals(
        InternalAttributeType.LONG, streamConfig.getAttributes().get(1).getAttributeType());
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        streamConfig.getAttributes().get(1).getMissingValuePolicy());
    assertEquals(Long.valueOf(-1), streamConfig.getAttributes().get(1).getDefaultValue());

    assertEquals("stringEnum", streamConfig.getAttributes().get(2).getName());
    assertEquals(
        InternalAttributeType.ENUM, streamConfig.getAttributes().get(2).getAttributeType());
    assertEquals(
        List.of("UNKNOWN", "MALE", "FEMALE"), streamConfig.getAttributes().get(2).getEnum());
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        streamConfig.getAttributes().get(2).getMissingValuePolicy());
    assertEquals("UNKNOWN", streamConfig.getAttributes().get(2).getDefaultValue());

    assertEquals(List.of("userName"), streamConfig.getPrimaryKey());

    assertEquals(
        MissingAttributePolicyV1.FAIL_PARENT_LOOKUP, streamConfig.getMissingLookupPolicy());
    assertEquals(4, streamConfig.getContextEnrichments().size());
    assertEquals("resourceType", streamConfig.getContextEnrichments().get(0).getName());
    assertEquals("zipcode", streamConfig.getContextEnrichments().get(1).getForeignKey().get(0));
    assertEquals(
        MissingLookupPolicy.STORE_FILL_IN_VALUE,
        streamConfig.getContextEnrichments().get(2).getMissingLookupPolicy());
    assertEquals(2, streamConfig.getContextEnrichments().get(3).getEnrichedAttributes().size());
    assertEquals(
        "staff.cost",
        streamConfig
            .getContextEnrichments()
            .get(3)
            .getEnrichedAttributes()
            .get(0)
            .getValuePickFirst()
            .get(0));
    assertEquals(
        "resourceValue",
        streamConfig.getContextEnrichments().get(3).getEnrichedAttributes().get(1).getAs());
    assertEquals((Long) 86400000L, streamConfig.getTtl());

    assertEquals("index1", streamConfig.getFeatures().get(0).getName());
    assertEquals("zipcode", streamConfig.getFeatures().get(1).getDimensions().get(0));
  }

  @Test
  public void testAllTypes() throws ValidatorException {
    contextConfig
        .getAttributes()
        .add(new AttributeConfig("decimalAttr", io.isima.bios.models.AttributeType.DECIMAL));
    contextConfig
        .getAttributes()
        .add(new AttributeConfig("booleanAttr", io.isima.bios.models.AttributeType.BOOLEAN));
    contextConfig
        .getAttributes()
        .add(new AttributeConfig("blobAttr", io.isima.bios.models.AttributeType.BLOB));

    final StreamConfig streamConfig = Translators.toTfos(contextConfig);
    assertEquals("contextExample", streamConfig.getName());
    assertEquals(Long.valueOf(1588175368661L), streamConfig.getVersion());
    assertEquals(MissingAttributePolicyV1.STRICT, streamConfig.getMissingValuePolicy());

    assertEquals(10, streamConfig.getAttributes().size());

    assertEquals("decimalAttr", streamConfig.getAttributes().get(7).getName());
    assertEquals(
        InternalAttributeType.DOUBLE, streamConfig.getAttributes().get(7).getAttributeType());

    assertEquals("booleanAttr", streamConfig.getAttributes().get(8).getName());
    assertEquals(
        InternalAttributeType.BOOLEAN, streamConfig.getAttributes().get(8).getAttributeType());

    assertEquals("blobAttr", streamConfig.getAttributes().get(9).getName());
    assertEquals(
        InternalAttributeType.BLOB, streamConfig.getAttributes().get(9).getAttributeType());

    assertEquals(List.of("userName"), streamConfig.getPrimaryKey());
  }

  @Test(expected = ConstraintViolationValidatorException.class)
  public void testMissingPrimaryKey() throws ValidatorException {
    contextConfig.setPrimaryKey(null);
    Translators.toTfos(contextConfig);
  }

  @Test(expected = ConstraintViolationValidatorException.class)
  public void testEmptyPrimaryKey() throws ValidatorException {
    contextConfig.getPrimaryKey().clear();
    Translators.toTfos(contextConfig);
  }

  @Test(expected = ConstraintViolationValidatorException.class)
  public void testInvalidPrimaryKey() throws ValidatorException {
    contextConfig.getPrimaryKey().clear();
    contextConfig.getPrimaryKey().add("noSuchKey");
    Translators.toTfos(contextConfig);
  }

  @Test
  public void testTwoDimensionalPrimaryKey() throws ValidatorException {
    contextConfig.getPrimaryKey().add("abc");
    final var tfosContext = Translators.toTfos(contextConfig);
    assertEquals(List.of("userName", "abc"), tfosContext.getPrimaryKey());
  }

  @Test(expected = NotImplementedValidatorException.class)
  public void testIntegerEnum() throws ValidatorException {
    final io.isima.bios.models.AttributeType strType = io.isima.bios.models.AttributeType.INTEGER;
    final var enumAttr = new AttributeConfig("integerEnum", strType);
    enumAttr.setAllowedValues(
        List.of(
            new AttributeValueGeneric("10", strType),
            new AttributeValueGeneric("20", strType),
            new AttributeValueGeneric("30", strType)));
    contextConfig.getAttributes().add(enumAttr);
    Translators.toTfos(contextConfig);
  }
}
