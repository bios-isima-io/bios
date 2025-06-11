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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.impl.AdminImpl;
import io.isima.bios.metrics.MetricsStreamProvider;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.BiosObjectMapperProvider;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamConfigBuilderTest {
  private static ObjectMapper tfosMapper;
  private static ObjectMapper biosMapper;

  private static AdminImpl admin;
  private static String tenantName;
  private static String originalSignalStreamName;
  private static String originalContextStreamName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    tfosMapper = TfosObjectMapperProvider.get();
    biosMapper = BiosObjectMapperProvider.get();

    admin = new AdminImpl(null, new MetricsStreamProvider());
    tenantName = "test";
    final var tenant = new TenantConfig(tenantName);
    Long timestamp = System.currentTimeMillis();
    admin.addTenant(tenant, RequestPhase.INITIAL, timestamp);
    admin.addTenant(tenant, RequestPhase.FINAL, timestamp);

    final String ctxSrc =
        "      {'attributes': [{'name': 'transaction_id', 'type': 'long'},"
            + "                {'defaultValue': 'missing_product_name',"
            + "                 'name': 'product_name',"
            + "                 'type': 'string'},"
            + "                {'defaultValue': 'missing_manufacturer_name',"
            + "                 'name': 'manufacturer_name',"
            + "                 'type': 'string'},"
            + "                {'name': 'price', 'type': 'double'},"
            + "                {'name': 'eventTimestamp', 'type': 'long'},"
            + "                {'defaultValue': 0, 'name': 'asfdasdf', 'type': 'double'}],"
            + " 'versionAttribute': 'eventTimestamp',"
            + " 'missingValuePolicy': 'strict',"
            + " 'name': 'Ctx0609211924ravid1test',"
            + " 'type': 'context',"
            + " 'version': 1592366669331,"
            + " 'globalLookupPolicy' : 'fail_parent_lookup',"
            + " 'dataSynthesisStatus':'Disabled',"
            + " 'ttl':86400000,"
            + "  'features': ["
            + "    {"
            + "      'featureName': 'index1',"
            + "      'dimensions': ['manufacturer_name'],"
            + "      'attributes': ['price'],"
            + "      'dataSketches': [],"
            + "      'indexed': true,"
            + "      'indexType': 'RangeQuery'"
            + "    }"
            + "  ]"
            + "}";

    final String ctxSrc2 =
        "      {'attributes': [{'name': 'lng_sig_attr', 'type': 'long'},"
            + "                {'name': 'timestamps', 'type': 'string'}],"
            + " 'missingValuePolicy': 'strict',"
            + " 'name': 'timestamps',"
            + " 'type': 'context'"
            + "}";

    final String src =
        "{'attributes': [{'defaultValue': 0, 'name': 'lng_sig_attr', 'type': 'long'},"
            + "                {'defaultValue': 0, 'name': 'dbl_sig_attr', 'type': 'double', "
            + "                 'tags': {'category': 'Quantity', 'kind': 'Dimensionless', "
            + "                          'unit': 'Percent', 'unitDisplayPosition': 'Suffix', "
            + "                          'positiveIndicator': 'Low', "
            + "                          'firstSummary': 'min'} },"
            + "                {'defaultValue': 'nil', 'name': 'str_sig_attr', 'type': 'string'},"
            + "                {'defaultValue': 0, 'name': 'eventTimestamp', 'type': 'long',"
            + "                  'tags': {'category': 'Quantity', 'kind': 'Timestamp',"
            + "                           'unit': 'UnixMillisecond'}}],"
            + " 'missingValuePolicy': 'use_default',"
            + " 'name': 'Sig0605030302ravid1test',"
            + " 'postprocesses': [{'rollups': [{'horizon': {'timeunit': 'minute', 'value': 5},"
            + "                                 'interval': {'timeunit': 'minute', 'value': 5},"
            + "                                 'name': 'rollup_Feature12312',"
            + "                                 'schemaName': 'rollup_Feature12312'}],"
            + "                    'view': 'Feature12312'},"
            + "                   {'rollups': [{'horizon': {'timeunit': 'minute', 'value': 1},"
            + "                                 'interval': {'timeunit': 'minute', 'value': 1},"
            + "                                 'name': 'sig.rollup.feature2'}],"
            + "                    'view': 'lastFifteenTimestamps'}],"
            + " 'preprocesses': [{'actions': [{'actionType': 'merge',"
            + "                                'attribute': 'manufacturer_name',"
            + "                                'context': 'Ctx0609211924ravid1test',"
            + "                                'defaultValue': '',"
            + "                                'missingLookupPolicy': 'strict'},"
            + "                               {'actionType': 'merge',"
            + "                                'as': 'alias2',"
            + "                                'attribute': 'price',"
            + "                                'context': 'Ctx0609211924ravid1test',"
            + "                                'defaultValue': 0,"
            + "                                'missingLookupPolicy': 'strict'},"
            + "                               {'actionType': 'merge',"
            + "                                'as': 'alias1',"
            + "                                'attribute': 'product_name',"
            + "                                'context': 'Ctx0609211924ravid1test',"
            + "                                'defaultValue': '',"
            + "                                'missingLookupPolicy': 'strict'}],"
            + "                   'condition': 'lng_sig_attr',"
            + "                   'missingLookupPolicy': 'strict',"
            + "                   'name': 'EnrichJun16_1'}],"
            + " 'ingestTimeLag': [{"
            + "    'ingestTimeLagName': 'lagSinceEventTimestamp',"
            + "    'attribute': 'eventTimestamp',"
            + "    'as': 'lag'"
            + "  }],"
            + " 'type': 'signal',"
            + " 'version': 1592366646461,"
            + " 'views': [{'attributes': ['lng_sig_attr'],"
            + "            'groupBy': ['str_sig_attr'],"
            + "            'name': 'Feature12312',"
            + "            'schemaVersion': 1592366646461},"
            + "           {'attributes': ['eventTimestamp'],"
            + "            'groupBy': ['lng_sig_attr'],"
            + "            'name': 'lastFifteenTimestamps',"
            + "            'materializedAs': 'LastN',"
            + "            'featureAsContextName': 'timestamps',"
            + "            'items': 15,"
            + "            'ttl': 36000000000,"
            + "            'snapshot': false}],"
            + " 'dataSynthesisStatus': 'enabled'}";

    // add context
    final StreamConfig ctx = tfosMapper.readValue(ctxSrc.replace("'", "\""), StreamConfig.class);
    ctx.setVersion(System.currentTimeMillis());
    admin.addStream(tenantName, ctx, RequestPhase.INITIAL);
    admin.addStream(tenantName, ctx, RequestPhase.FINAL);
    originalContextStreamName = ctx.getName();

    // add another context
    final StreamConfig ctx2 = tfosMapper.readValue(ctxSrc2.replace("'", "\""), StreamConfig.class);
    ctx2.setVersion(System.currentTimeMillis());
    admin.addStream(tenantName, ctx2, RequestPhase.INITIAL);
    admin.addStream(tenantName, ctx2, RequestPhase.FINAL);

    // add signal
    final StreamConfig origSignalConfig =
        tfosMapper.readValue(src.replace("'", "\""), StreamConfig.class);
    origSignalConfig.setVersion(System.currentTimeMillis());
    admin.addStream(tenantName, origSignalConfig, RequestPhase.INITIAL);
    admin.addStream(tenantName, origSignalConfig, RequestPhase.FINAL);
    originalSignalStreamName = origSignalConfig.getName();
  }

  @Test
  public void testSignalConfigBuilder() throws Exception {

    StreamDesc origSignalDesc = admin.getStream(tenantName, originalSignalStreamName);

    // translate
    final SignalConfig signal = SignalConfigBuilder.getBuilder(origSignalDesc).build();
    verifySignal(origSignalDesc, signal);
  }

  // Property 'condition' in PreProcessDesc is deprecated and foreignKey will be used instead
  // since version 1.1.29. The schema validator in admin would move the condition data to
  // foreignKey, but the translator has to be able to handle both.
  @Test
  public void testSignalBuildEnrichmentFromForeignKey() throws Exception {

    StreamDesc origSignalDesc = admin.getStream(tenantName, originalSignalStreamName).duplicate();
    final var prp = origSignalDesc.getPreprocesses().get(0);
    prp.setCondition(prp.getForeignKey().get(0));
    prp.setForeignKey(null);

    // translate
    final SignalConfig signal = SignalConfigBuilder.getBuilder(origSignalDesc).build();
    verifySignal(origSignalDesc, signal);
  }

  private void verifySignal(StreamDesc origSignalDesc, SignalConfig signal) {
    assertEquals(originalSignalStreamName, signal.getName());
    assertEquals(MissingAttributePolicy.STORE_DEFAULT_VALUE, signal.getMissingAttributePolicy());
    assertEquals(4, signal.getAttributes().size());

    assertEquals("lng_sig_attr", signal.getAttributes().get(0).getName());
    assertEquals(AttributeType.INTEGER, signal.getAttributes().get(0).getType());
    assertEquals(0L, signal.getAttributes().get(0).getDefaultValue().asLong());
    assertEquals("dbl_sig_attr", signal.getAttributes().get(1).getName());
    assertEquals(AttributeType.DECIMAL, signal.getAttributes().get(1).getType());
    assertEquals(0.0, signal.getAttributes().get(1).getDefaultValue().asDouble(), 1.e-10);
    assertEquals(AttributeCategory.QUANTITY, signal.getAttributes().get(1).getTags().getCategory());
    assertEquals(Unit.PERCENT, signal.getAttributes().get(1).getTags().getUnit());
    assertEquals(
        UnitDisplayPosition.SUFFIX,
        signal.getAttributes().get(1).getTags().getUnitDisplayPosition());
    assertEquals(
        PositiveIndicator.LOW, signal.getAttributes().get(1).getTags().getPositiveIndicator());
    assertEquals("str_sig_attr", signal.getAttributes().get(2).getName());
    assertEquals(AttributeType.STRING, signal.getAttributes().get(2).getType());
    assertEquals("nil", signal.getAttributes().get(2).getDefaultValue().asString());

    assertEquals(1, signal.getEnrich().getEnrichments().size());
    final var enrichment = signal.getEnrich().getEnrichments().get(0);
    assertEquals(origSignalDesc.getPreprocesses().get(0).getName(), enrichment.getName());
    assertEquals(List.of("lng_sig_attr"), enrichment.getForeignKey());
    assertEquals(MissingLookupPolicy.REJECT, enrichment.getMissingLookupPolicy());
    assertEquals("Ctx0609211924ravid1test", enrichment.getContextName());
    final var contextAttributes = enrichment.getContextAttributes();
    assertNotNull(contextAttributes);
    assertEquals(3, contextAttributes.size());
    assertEquals("manufacturer_name", contextAttributes.get(0).getAttributeName());
    assertNull(contextAttributes.get(0).getAs());
    assertEquals(
        new AttributeValueGeneric("", AttributeType.STRING), contextAttributes.get(0).getFillIn());
    assertEquals("price", contextAttributes.get(1).getAttributeName());
    assertEquals("alias2", contextAttributes.get(1).getAs());
    assertEquals(
        new AttributeValueGeneric(0.0, AttributeType.DECIMAL),
        contextAttributes.get(1).getFillIn());
    assertEquals("product_name", contextAttributes.get(2).getAttributeName());
    assertEquals("alias1", contextAttributes.get(2).getAs());
    assertEquals(
        new AttributeValueGeneric("", AttributeType.STRING), contextAttributes.get(2).getFillIn());

    assertEquals(2, signal.getPostStorageStage().getFeatures().size());
    final var feature0 = signal.getPostStorageStage().getFeatures().get(0);
    assertEquals(origSignalDesc.getViews().get(0).getName(), feature0.getName());
    assertNull(feature0.getMaterializedAs());

    final var feature1 = signal.getPostStorageStage().getFeatures().get(1);
    final var view1 = origSignalDesc.getViews().get(1);
    assertEquals(view1.getName(), feature1.getName());
    assertEquals(view1.getGroupBy(), feature1.getDimensions());
    assertEquals(view1.getAttributes(), feature1.getAttributes());
    assertEquals(view1.getDataSketches(), feature1.getDataSketches());
    assertEquals(view1.getFeatureAsContextName(), feature1.getFeatureAsContextName());
    assertEquals(view1.getLastNItems(), feature1.getItems());
    assertEquals(view1.getLastNTtl(), feature1.getTtl());
    assertNull(feature1.getMaterializedAs());
    assertEquals(Long.valueOf(60000L), feature1.getFeatureInterval());

    final var dynamic = signal.getEnrich().getIngestTimeLag();
    assertNotNull(dynamic);
    assertEquals(1, dynamic.size());
    assertEquals("lagSinceEventTimestamp", dynamic.get(0).getName());
    assertEquals("eventTimestamp", dynamic.get(0).getAttribute());
    assertEquals("lag", dynamic.get(0).getAs());
  }

  @Test
  public void testLeastSignalConfigBuild() throws Exception {
    StreamDesc origSignalDesc = admin.getStream(tenantName, originalSignalStreamName);
    final var signal = SignalConfigBuilder.getBuilder(origSignalDesc).least(true).build();
    assertEquals(
        String.format("{\"signalName\":\"%s\"}", originalSignalStreamName),
        biosMapper.writeValueAsString(signal));
  }

  @Test
  public void testContextConfigBuilder() throws Exception {
    StreamDesc origContextDesc = admin.getStream(tenantName, originalContextStreamName);
    origContextDesc.setSchemaVersion(123L);

    // translate
    final ContextConfig context = ContextConfigBuilder.getBuilder(origContextDesc).build();

    verifyContext(origContextDesc, context, List.of("transaction_id"));
  }

  @Test
  public void testContextConfigBuilderEmptyPrimaryKey() throws Exception {
    StreamDesc origContextDesc = admin.getStream(tenantName, originalContextStreamName);
    origContextDesc.setSchemaVersion(123L);
    origContextDesc.setPrimaryKey(null);

    // translate
    final ContextConfig context = ContextConfigBuilder.getBuilder(origContextDesc).build();

    verifyContext(origContextDesc, context, List.of("transaction_id"));
  }

  // Property 'primaryKey' in StreamDesc/StreamConfig is introduced since version 1.1.29.
  @Test
  public void testContextBuildWithEplicitPrimaryKey() throws Exception {
    StreamDesc origContextDesc = admin.getStream(tenantName, originalContextStreamName);
    origContextDesc.setSchemaVersion(123L);
    origContextDesc.setPrimaryKey(List.of("manufacturer_name"));

    // translate
    final ContextConfig context = ContextConfigBuilder.getBuilder(origContextDesc).build();

    verifyContext(origContextDesc, context, List.of("manufacturer_name"));
  }

  // Property 'primaryKey' in StreamDesc/StreamConfig is introduced since version 1.1.29.
  @Test
  public void testContextBuildWithEplicitCompositePrimaryKey() throws Exception {
    StreamDesc origContextDesc = admin.getStream(tenantName, originalContextStreamName);
    origContextDesc.setSchemaVersion(123L);
    origContextDesc.setPrimaryKey(List.of("manufacturer_name", "transaction_id"));

    // translate
    final ContextConfig context = ContextConfigBuilder.getBuilder(origContextDesc).build();

    verifyContext(origContextDesc, context, List.of("manufacturer_name", "transaction_id"));
  }

  private void verifyContext(
      StreamDesc origContextDesc, ContextConfig context, List<String> primaryKey) {
    assertEquals(originalContextStreamName, context.getName());
    assertEquals(origContextDesc.getVersion(), context.getVersion());
    assertEquals(origContextDesc.getSchemaVersion(), context.getBiosVersion());
    assertEquals(MissingAttributePolicy.REJECT, context.getMissingAttributePolicy());

    assertEquals(primaryKey, context.getPrimaryKey());

    assertEquals(6, context.getAttributes().size());
    final var attributes = context.getAttributes();
    assertEquals("transaction_id", attributes.get(0).getName());
    assertEquals(AttributeType.INTEGER, attributes.get(0).getType());
    assertEquals("product_name", attributes.get(1).getName());
    assertEquals(AttributeType.STRING, attributes.get(1).getType());
    assertEquals("missing_product_name", attributes.get(1).getDefaultValue().asString());
    assertEquals("manufacturer_name", attributes.get(2).getName());
    assertEquals(AttributeType.STRING, attributes.get(2).getType());
    assertEquals("missing_manufacturer_name", attributes.get(2).getDefaultValue().asString());
    assertEquals("price", attributes.get(3).getName());
    assertEquals(AttributeType.DECIMAL, attributes.get(3).getType());
    assertEquals("eventTimestamp", attributes.get(4).getName());
    assertEquals(AttributeType.INTEGER, attributes.get(4).getType());
    assertEquals("asfdasdf", attributes.get(5).getName());
    assertEquals(AttributeType.DECIMAL, attributes.get(5).getType());
    assertEquals(0.0, attributes.get(5).getDefaultValue().asDouble(), 0.0);

    assertEquals(MissingLookupPolicy.FAIL_PARENT_LOOKUP, context.getMissingLookupPolicy());
    assertEquals((Long) 86400000L, context.getTtl());
    assertEquals("index1", context.getFeatures().get(0).getName());
    assertEquals("price", context.getFeatures().get(0).getAttributes().get(0));
  }

  @Test
  public void testLeastContextConfigBuild() throws Exception {
    StreamDesc origContextDesc = admin.getStream(tenantName, originalContextStreamName);
    final ContextConfig context =
        ContextConfigBuilder.getBuilder(origContextDesc).least(true).build();
    assertEquals(
        String.format("{\"contextName\":\"%s\"}", originalContextStreamName),
        biosMapper.writeValueAsString(context));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongConversionFromCtoS() throws Exception {
    StreamDesc origContextDesc = admin.getStream(tenantName, originalContextStreamName);
    SignalConfigBuilder.getBuilder(origContextDesc);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongConversionFromStoC() throws Exception {
    StreamDesc origSignalDesc = admin.getStream(tenantName, originalSignalStreamName);
    ContextConfigBuilder.getBuilder(origSignalDesc);
  }
}
