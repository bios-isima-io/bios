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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.exceptions.validator.NotImplementedValidatorException;
import io.isima.bios.exceptions.validator.ValidatorException;
import io.isima.bios.models.AttributeCategory;
import io.isima.bios.models.AttributeConfig;
import io.isima.bios.models.AttributeKind;
import io.isima.bios.models.AttributeValueGeneric;
import io.isima.bios.models.DataSketchType;
import io.isima.bios.models.MissingAttributePolicyV1;
import io.isima.bios.models.SignalConfig;
import io.isima.bios.models.v1.ActionType;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SignalTranslatorTest {
  private static ObjectMapper objectMapper;

  private SignalConfig simpleConfig;
  private SignalConfig fullConfig;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Before
  public void setUp() throws Exception {
    final String srcSimple =
        "{"
            + "  'signalName': 'signalExample',"
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
            + "      'tags': {"
            + "        'category': 'Quantity',"
            + "        'kind': 'OtherKind',"
            + "        'otherKindName': 'Power'"
            + "       },"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': -1,"
            + "      'unit': '$',"
            + "      'unitPosition': 'Prefix',"
            + "      'positiveIndicator': 'High'"
            + "    },"
            + "    {"
            + "      'attributeName': 'stringEnum',"
            + "      'type': 'String',"
            + "      'allowedValues': ['UNKNOWN', 'MALE', 'FEMALE'],"
            + "      'missingAttributePolicy': 'StoreDefaultValue',"
            + "      'default': 'UNKNOWN'"
            + "    }"
            + "  ]"
            + "}";

    simpleConfig = objectMapper.readValue(srcSimple.replace("'", "\""), SignalConfig.class);

    final String srcFull =
        "{"
            + "  'signalName': 'signal_with_postprocess_example',"
            + "  'biosVersion': 1588197288385,"
            + "  'version': 15552000000,"
            + "  'missingAttributePolicy': 'Reject',"
            + "  'attributes': ["
            + "    {'attributeName': 'customer_id',     'type': 'Integer'},"
            + "    {'attributeName': 'access_point_id', 'type': 'Integer'},"
            + "    {'attributeName': 'stay_length',     'type': 'Integer'},"
            + "    {'attributeName': 'latitude',        'type': 'Decimal'},"
            + "    {'attributeName': 'longitude',       'type': 'Decimal'},"
            + "    {'attributeName': 'customer_name',   'type': 'String'},"
            + "    {'attributeName': 'gender',          'type': 'String',"
            + "     'allowedValues': ['MALE', 'FEMALE', 'UNKNOWN']},"
            + "    {'attributeName': 'country',         'type': 'String'},"
            + "    {'attributeName': 'state',           'type': 'String'}"
            + "  ],"
            + "  'enrich': {"
            + "    'missingLookupPolicy': 'StoreFillInValue',"
            + "    'enrichments': ["
            + "      {"
            + "        'enrichmentName': 'join_customer_info',"
            + "        'foreignKey': ['customer_id'],"
            + "        'missingLookupPolicy': 'StoreFillInValue',"
            + "        'contextName': 'user_context',"
            + "        'contextAttributes': ["
            + "          {"
            + "            'attributeName': 'name',"
            + "            'as': 'customer_name',"
            + "            'fillIn': 'n/a'"
            + "          }, {"
            + "            'attributeName': 'gender',"
            + "            'fillIn': 'UNKNOWN'"
            + "          }"
            + "        ]"
            + "      }, {"
            + "        'enrichmentName': 'join_access_point_location',"
            + "        'foreignKey': ['access_point_id'],"
            + "        'missingLookupPolicy': 'Reject',"
            + "        'contextName': 'access_point_context',"
            + "        'contextAttributes': ["
            + "          {'attributeName': 'country'},"
            + "          {'attributeName': 'state'},"
            + "          {'attributeName': 'city'}"
            + "        ]"
            + "      }"
            + "    ]"
            + "  },"
            + "  'postStorageStage': {"
            + "    'features': ["
            + "      {"
            + "        'featureName': 'by_country_state',"
            + "        'dimensions': ['country', 'state'],"
            + "        'attributes': ['stay_length', 'order_id'],"
            + "        'dataSketches': ['Moments'],"
            + "        'featureInterval': 300000"
            + "      },"
            + "      {"
            + "        'featureName': 'lastTenStates',"
            + "        'dimensions': ['country'],"
            + "        'attributes': ['state'],"
            + "        'dataSketches': ['LastN'],"
            + "        'featureAsContextName': 'visitedStates',"
            + "        'items': 10,"
            + "        'ttl': 360000000,"
            + "        'featureInterval': 60000,"
            + "        'materializedAs': 'AccumulatingCount'"
            + "      }"
            + "    ]"
            + "  },"
            + "  'dataSynthesisStatus': 'enabled',"
            + "  'exportDestinationId': '7af36ddc-715f-46e4-adda-caee2dbebf9e'"
            + "}";
    fullConfig = objectMapper.readValue(srcFull.replace("'", "\""), SignalConfig.class);
  }

  @Test
  public void testBasic() throws ValidatorException {
    final StreamConfig streamConfig = Translators.toTfos(simpleConfig);
    assertEquals(StreamType.SIGNAL, streamConfig.getType());
    assertEquals("signalExample", streamConfig.getName());
    assertEquals(Long.valueOf(1588175368661L), streamConfig.getVersion());
    assertEquals(MissingAttributePolicyV1.STRICT, streamConfig.getMissingValuePolicy());

    assertEquals(3, streamConfig.getAttributes().size());

    assertEquals("userName", streamConfig.getAttributes().get(0).getName());
    assertEquals(
        InternalAttributeType.STRING, streamConfig.getAttributes().get(0).getAttributeType());

    assertEquals("abc", streamConfig.getAttributes().get(1).getName());
    assertEquals(
        InternalAttributeType.LONG, streamConfig.getAttributes().get(1).getAttributeType());
    // assertEquals(AttributeCategory.ADDITIVE, streamConfig.getAttributes().get(1).getCategory());
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        streamConfig.getAttributes().get(1).getMissingValuePolicy());
    assertEquals((long) -1, streamConfig.getAttributes().get(1).getDefaultValue());
    assertEquals(
        AttributeCategory.QUANTITY, streamConfig.getAttributes().get(1).getTags().getCategory());
    assertEquals(AttributeKind.OTHER_KIND, streamConfig.getAttributes().get(1).getTags().getKind());
    assertEquals("Power", streamConfig.getAttributes().get(1).getTags().getOtherKindName());

    assertEquals("stringEnum", streamConfig.getAttributes().get(2).getName());
    assertEquals(
        InternalAttributeType.ENUM, streamConfig.getAttributes().get(2).getAttributeType());
    assertEquals(
        List.of("UNKNOWN", "MALE", "FEMALE"), streamConfig.getAttributes().get(2).getEnum());
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        streamConfig.getAttributes().get(2).getMissingValuePolicy());
    assertEquals("UNKNOWN", streamConfig.getAttributes().get(2).getDefaultValue());
  }

  @Test
  public void testAllTypes() throws ValidatorException {
    simpleConfig
        .getAttributes()
        .add(new AttributeConfig("decimalAttr", io.isima.bios.models.AttributeType.DECIMAL));
    simpleConfig
        .getAttributes()
        .add(new AttributeConfig("booleanAttr", io.isima.bios.models.AttributeType.BOOLEAN));
    simpleConfig
        .getAttributes()
        .add(new AttributeConfig("blobAttr", io.isima.bios.models.AttributeType.BLOB));

    final StreamConfig streamConfig = Translators.toTfos(simpleConfig);
    assertEquals("signalExample", streamConfig.getName());
    assertEquals(Long.valueOf(1588175368661L), streamConfig.getVersion());
    assertEquals(MissingAttributePolicyV1.STRICT, streamConfig.getMissingValuePolicy());

    assertEquals(6, streamConfig.getAttributes().size());

    assertEquals("decimalAttr", streamConfig.getAttributes().get(3).getName());
    assertEquals(
        InternalAttributeType.DOUBLE, streamConfig.getAttributes().get(3).getAttributeType());

    assertEquals("booleanAttr", streamConfig.getAttributes().get(4).getName());
    assertEquals(
        InternalAttributeType.BOOLEAN, streamConfig.getAttributes().get(4).getAttributeType());

    assertEquals("blobAttr", streamConfig.getAttributes().get(5).getName());
    assertEquals(
        InternalAttributeType.BLOB, streamConfig.getAttributes().get(5).getAttributeType());
  }

  @Test
  public void testFullSignalProperties() throws ValidatorException {
    final StreamConfig stream = Translators.toTfos(fullConfig);
    assertEquals("signal_with_postprocess_example", stream.getName());
    // check version
    assertEquals(Long.valueOf(15552000000L), stream.getVersion());

    // check export destination ID
    assertEquals("7af36ddc-715f-46e4-adda-caee2dbebf9e", stream.getExportDestinationId());

    // check attributes... only briefly (other cases cover more)
    assertEquals(
        List.of(
            "customer_id",
            "access_point_id",
            "stay_length",
            "latitude",
            "longitude",
            "customer_name",
            "gender",
            "country",
            "state"),
        stream.getAttributes().stream().map(AttributeDesc::getName).collect(Collectors.toList()));

    // check preprocesses
    assertEquals(2, stream.getPreprocesses().size());
    final var prp0 = stream.getPreprocesses().get(0);
    assertEquals("join_customer_info", prp0.getName());
    assertEquals(List.of("customer_id"), prp0.getForeignKey());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, prp0.getMissingLookupPolicy());

    assertEquals(2, prp0.getActions().size());

    assertEquals(ActionType.MERGE, prp0.getActions().get(0).getActionType());
    assertEquals("name", prp0.getActions().get(0).getAttribute());
    assertEquals("customer_name", prp0.getActions().get(0).getAs());
    assertEquals("user_context", prp0.getActions().get(0).getContext());
    assertEquals("n/a", prp0.getActions().get(0).getDefaultValue());

    assertEquals(ActionType.MERGE, prp0.getActions().get(1).getActionType());
    assertEquals("gender", prp0.getActions().get(1).getAttribute());
    assertNull(prp0.getActions().get(1).getAs());
    assertEquals("user_context", prp0.getActions().get(1).getContext());
    assertEquals("UNKNOWN", prp0.getActions().get(1).getDefaultValue());

    final var prp1 = stream.getPreprocesses().get(1);
    assertEquals("join_access_point_location", prp1.getName());
    assertEquals(List.of("access_point_id"), prp1.getForeignKey());
    assertEquals(MissingAttributePolicyV1.STRICT, prp1.getMissingLookupPolicy());

    assertEquals(3, prp1.getActions().size());

    // check view
    assertNotNull(stream.getViews());
    assertEquals(2, stream.getViews().size());
    final var view0 = stream.getViews().get(0);
    assertEquals("by_country_state", view0.getName());
    assertEquals(List.of("country", "state"), view0.getGroupBy());
    assertEquals(List.of("stay_length", "order_id"), view0.getAttributes());
    assertEquals(List.of(DataSketchType.MOMENTS), view0.getDataSketches());

    final var view1 = stream.getViews().get(1);
    assertEquals("lastTenStates", view1.getName());
    assertEquals(List.of("country"), view1.getGroupBy());
    assertEquals(List.of("state"), view1.getAttributes());
    assertEquals(List.of(DataSketchType.LAST_N), view1.getDataSketches());
    assertEquals("visitedStates", view1.getFeatureAsContextName());
    assertEquals(Long.valueOf(10), view1.getLastNItems());
    assertEquals(Long.valueOf(360000000), view1.getLastNTtl());
    assertEquals(Boolean.TRUE, view1.getSnapshot());

    // check post processes
    assertNotNull(stream.getPostprocesses());
    assertEquals(2, stream.getPostprocesses().size());

    final var pop0 = stream.getPostprocesses().get(0);
    assertNotNull(pop0);
    assertEquals("by_country_state", pop0.getView());
    assertNotNull(pop0.getRollups());
    assertEquals(1, pop0.getRollups().size());
    final var rollup0 = pop0.getRollups().get(0);
    assertNotNull(rollup0);
    assertEquals("signal_with_postprocess_example.rollup.by_country_state", rollup0.getName());
    assertEquals(300000, rollup0.getInterval().getValueInMillis());

    final var pop1 = stream.getPostprocesses().get(1);
    assertNotNull(pop1);
    assertEquals("lastTenStates", pop1.getView());
    assertNotNull(pop1.getRollups());
    assertEquals(1, pop1.getRollups().size());
    final var rollup1 = pop1.getRollups().get(0);
    assertNotNull(rollup1);
    assertEquals("signal_with_postprocess_example.rollup.lastTenStates", rollup1.getName());
    assertEquals(60000, rollup1.getInterval().getValueInMillis());
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
    simpleConfig.getAttributes().add(enumAttr);
    Translators.toTfos(simpleConfig);
  }

  @Test
  public void testFunctionDomainCapture() {
    assertEquals("abc", StreamConfigTranslator.captureDomain("sum(abc)"));
    assertEquals("indented", StreamConfigTranslator.captureDomain("  Min(indented)"));
    assertEquals("trailed", StreamConfigTranslator.captureDomain("Min(trailed)  "));
    assertNull(StreamConfigTranslator.captureDomain("bad syntax(N)"));
    assertNull(StreamConfigTranslator.captureDomain("thisIsWrong(y) + x"));
    assertEquals("spaces", StreamConfigTranslator.captureDomain(" max ( spaces )  "));
    assertEquals("", StreamConfigTranslator.captureDomain("count()"));
    assertEquals("", StreamConfigTranslator.captureDomain(" count ( ) "));
  }

  @Test
  public void testFunctionDomainRetrieval() throws Exception {
    final var metrics =
        List.of("count()", "sum(abc)", "Min(Abc)", "max(stayLength)", "min(stayLength)");
    final var domains = StreamConfigTranslator.retrieveMetricDomains(metrics, "Test.metrics");
    assertEquals(List.of("Abc", "stayLength"), domains);
  }
}
