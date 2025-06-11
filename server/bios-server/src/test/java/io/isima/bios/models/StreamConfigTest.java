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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.FeatureDesc;
import io.isima.bios.models.v1.InternalAttributeType;
import io.isima.bios.models.v1.PreprocessDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.StreamType;
import io.isima.bios.models.v1.ViewDesc;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class StreamConfigTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
  }

  private void checkInitialConfig(StreamConfig config, String name, StreamType type) {
    assertEquals(name, config.getName());
    assertEquals(type, config.getType());
    assertNotNull(config.getAttributes());
    assertNull(config.getAdditionalAttributes());
    assertNull(config.getPreprocesses());
    assertNull(config.getVersion());
  }

  @Test
  public void testConstructors() {
    checkInitialConfig(new StreamConfig(), null, StreamType.SIGNAL);
    checkInitialConfig(new StreamConfig("hello"), "hello", StreamType.SIGNAL);
    checkInitialConfig(new StreamConfig("hello", StreamType.CONTEXT), "hello", StreamType.CONTEXT);
  }

  private IngestTimeLagEnrichmentConfig createIngestTimeLagEnrichmentConfig() {
    var itl = new IngestTimeLagEnrichmentConfig();
    itl.setName("test");
    itl.setAttribute("second");
    itl.setAs("secondLag");
    itl.setFillIn(new AttributeValueGeneric(10.1, io.isima.bios.models.AttributeType.DECIMAL));
    return itl;
  }

  @Test
  public void testDuplicate() {
    // Build an instance
    String streamName = "testConfig";
    StreamConfig original = new StreamConfig(streamName);
    original.addAttribute(new AttributeDesc("one", InternalAttributeType.STRING));
    original.addAttribute(
        new AttributeDesc("two", InternalAttributeType.LONG)
            .setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT)
            .setDefaultValue(100));
    original.addAttribute(
        new AttributeDesc("enumenum", InternalAttributeType.ENUM)
            .setEnum(Arrays.asList(new String[] {"ichi", "ni", "san"})));
    original.addAdditionalAttribute(new AttributeDesc("three", InternalAttributeType.DOUBLE));
    original.addAdditionalAttribute(new AttributeDesc("four", InternalAttributeType.INET));
    ViewDesc view = new ViewDesc();
    view.setName("viewName");
    view.setGroupBy(Arrays.asList("section", "subsection"));
    view.setAttributes(Arrays.asList("pages", "lines"));
    original.addView(view);
    original.addPreprocess(new PreprocessDesc("pp"));
    original.setType(StreamType.CONTEXT).setVersion(12345L);
    original.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    original.setMissingLookupPolicy(MissingAttributePolicyV1.STRICT);
    original.addEnrichment(new EnrichmentConfigContext("enrichment1"));
    original.getContextEnrichments().get(0).addEnrichedAttribute(new EnrichedAttribute());
    original.setTtl(98765L);
    IngestTimeLagEnrichmentConfig itl = createIngestTimeLagEnrichmentConfig();
    original.setIngestTimeLag(List.of(itl));

    FeatureDesc feature = new FeatureDesc();
    feature.setName("feature1");
    feature.setDimensions(List.of("dim1"));
    feature.setAttributes(List.of("attr1", "attr2"));
    feature.setIndexed(true);
    feature.setSnapshot(true);
    original.setFeatures(List.of(feature));

    // make a clone
    final StreamConfig clone = original.duplicate();
    assertNotSame(original, clone);

    // check name
    assertEquals(streamName, clone.getName());
    assertEquals(clone.getName(), original.getName());
    // check type
    assertEquals(StreamType.CONTEXT, clone.getType());
    assertEquals(clone.getType(), original.getType());
    // check missing value policy
    assertEquals(clone.getMissingValuePolicy(), original.getMissingValuePolicy());
    // check version
    assertEquals((Long) 12345L, clone.getVersion());
    assertEquals((Long) 12345L, original.getVersion());
    // check deleted flag
    // check event attributes
    assertNotSame(clone.getAttributes(), original.getAttributes());
    assertEquals(clone.getAttributes().size(), original.getAttributes().size());
    for (int i = 0; i < original.getAttributes().size(); ++i) {
      assertNotSame(clone.getAttributes().get(i), original.getAttributes().get(i));
      assertEquals(
          clone.getAttributes().get(i).getName(), original.getAttributes().get(i).getName());
      assertEquals(
          clone.getAttributes().get(i).getAttributeType(),
          original.getAttributes().get(i).getAttributeType());
      assertEquals(
          clone.getAttributes().get(i).getMissingValuePolicy(),
          original.getAttributes().get(i).getMissingValuePolicy());
      assertEquals(
          clone.getAttributes().get(i).getInternalDefaultValue(),
          original.getAttributes().get(i).getInternalDefaultValue());
      List<String> cloneEnum = clone.getAttributes().get(i).getEnum();
      List<String> origEnum = original.getAttributes().get(i).getEnum();
      if (origEnum == null) {
        assertNull(cloneEnum);
      } else {
        assertNotNull(cloneEnum);
        assertNotSame(cloneEnum, origEnum);
        assertEquals(cloneEnum, origEnum);
      }
    }
    assertNotSame(clone.getAttributes(), original.getAttributes());
    // check additional attributes
    assertEquals(clone.getAdditionalAttributes().size(), original.getAdditionalAttributes().size());
    for (int i = 0; i < original.getAdditionalAttributes().size(); ++i) {
      assertNotSame(
          clone.getAdditionalAttributes().get(i), original.getAdditionalAttributes().get(i));
      assertEquals(
          clone.getAdditionalAttributes().get(i).getName(),
          original.getAdditionalAttributes().get(i).getName());
      assertEquals(
          clone.getAdditionalAttributes().get(i).getAttributeType(),
          original.getAdditionalAttributes().get(i).getAttributeType());
    }
    // check view
    assertNotSame(clone.getViews(), original.getViews());
    assertEquals(clone.getViews(), original.getViews());
    // check preprocess
    List<PreprocessDesc> ppClone = clone.getPreprocesses();
    List<PreprocessDesc> ppOrig = original.getPreprocesses();
    assertNotSame(ppClone, ppOrig);
    assertEquals(ppClone.size(), ppOrig.size());
    for (int i = 0; i < ppOrig.size(); ++i) {
      assertNotSame(ppClone.get(i), ppOrig.get(i));
      assertEquals(ppClone.get(i).getName(), ppOrig.get(i).getName());
    }
    assertEquals(original.getMissingLookupPolicy(), clone.getMissingLookupPolicy());
    // dss
    assertEquals(clone.getContextEnrichments(), original.getContextEnrichments());
    assertEquals(clone.getTtl(), original.getTtl());
    assertEquals(clone.getIngestTimeLag(), original.getIngestTimeLag());
    assertEquals(clone.getFeatures(), original.getFeatures());

    assertNull(clone.getPrimaryKey());

    // duplicate primary key
    original.setPrimaryKey(List.of("hello", "world"));
    final var clone2 = original.duplicate();
    assertEquals(List.of("hello", "world"), clone2.getPrimaryKey());
  }

  @Test
  public void testJsonSerialization() throws IOException {
    StreamConfig config = new StreamConfig("test_stream").setVersion(12345L);
    config.addAttribute(new AttributeDesc("first", InternalAttributeType.STRING));
    config.addAttribute(new AttributeDesc("second", InternalAttributeType.LONG));
    config.addAdditionalAttribute(new AttributeDesc("additional", InternalAttributeType.INET));
    config.setTtl(98765L);
    IngestTimeLagEnrichmentConfig itl = createIngestTimeLagEnrichmentConfig();
    config.setIngestTimeLag(List.of(itl));
    FeatureDesc feature = new FeatureDesc();
    feature.setName("feature1");
    feature.setSnapshot(true);
    config.setFeatures(List.of(feature));
    config.setExportDestinationId("7af36ddc-715f-46e4-adda-caee2dbebf9e");

    String out = mapper.writeValueAsString(config);
    assertTrue(out.contains("\"name\":\"test_stream\""));
    assertTrue(out.contains("\"version\":12345"));
    assertTrue(out.contains("\"attributes\":[{\"name\":\"first\",\"type\":\"string\"}"));
    assertTrue(out.contains("additionalAttribute"));
    assertTrue(out.contains("\"ttl\":98765"));
    assertTrue(
        out.contains(
            "\"ingestTimeLag\":[{\"ingestTimeLagName\":\"test\",\"attribute\":"
                + "\"second\",\"as\":\"secondLag\",\"fillIn\":10.1}]"));
    assertFalse(out.contains("deleted"));
    assertTrue(out.contains("\"featureName\":\"feature1\""));
    assertTrue(out.contains("\"snapshot\":true"));
    assertTrue(out.contains("\"exportDestinationId\":\"7af36ddc-715f-46e4-adda-caee2dbebf9e\""));

    String src =
        "{\"name\":\"test_stream\",\"type\":\"context\",\"version\":12345,"
            + "\"eventAttributes\":[{\"name\":\"first\",\"type\":\"string\"}],\"deleted\":true,"
            + "\"exportDestinationId\":\"7af36ddc-715f-46e4-adda-caee2dbebf9e\"}";
    StreamConfig decoded = mapper.readValue(src, StreamConfig.class);
    assertEquals("test_stream", decoded.getName());
    assertEquals(StreamType.CONTEXT, decoded.getType());
    assertEquals(Long.valueOf(12345L), decoded.getVersion());
    assertEquals("7af36ddc-715f-46e4-adda-caee2dbebf9e", decoded.getExportDestinationId());
  }

  @Test
  public void testEnrichedContextMarshalling() throws IOException {
    final String resourceName = "/EnrichedContextTfos.json";
    final byte[] bytes = getClass().getResourceAsStream(resourceName).readAllBytes();
    final String src = new String(bytes, StandardCharsets.UTF_8);
    final StreamConfig config =
        mapper.readValue(getClass().getResource(resourceName), StreamConfig.class);

    assertEquals("enrichedContext", config.getName());
    assertEquals(Long.valueOf(1588175368660L), config.getVersion());
    assertEquals(MissingAttributePolicyV1.STRICT, config.getMissingValuePolicy());
    assertEquals(3, config.getAttributes().size());
    assertEquals("resourceId", config.getAttributes().get(0).getName());
    assertEquals(InternalAttributeType.STRING, config.getAttributes().get(0).getAttributeType());
    assertEquals("resourceTypeNum", config.getAttributes().get(1).getName());
    assertEquals(InternalAttributeType.LONG, config.getAttributes().get(1).getAttributeType());
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        config.getAttributes().get(2).getMissingValuePolicy());

    assertEquals(MissingAttributePolicyV1.FAIL_PARENT_LOOKUP, config.getMissingLookupPolicy());
    assertEquals(2, config.getContextEnrichments().size());
    assertEquals("resourceType", config.getContextEnrichments().get(0).getName());
    assertEquals("resourceTypeNum", config.getContextEnrichments().get(0).getForeignKey().get(0));
    assertEquals(2, config.getContextEnrichments().get(0).getEnrichedAttributes().size());
    assertEquals(
        "resourceType.resourceTypeCode",
        config.getContextEnrichments().get(0).getEnrichedAttributes().get(0).getValue());
    assertEquals(
        "myResourceCategory",
        config.getContextEnrichments().get(0).getEnrichedAttributes().get(1).getAs());
    assertEquals(
        6,
        config
            .getContextEnrichments()
            .get(1)
            .getEnrichedAttributes()
            .get(0)
            .getValuePickFirst()
            .size());
    assertEquals(
        "staff.cost",
        config
            .getContextEnrichments()
            .get(1)
            .getEnrichedAttributes()
            .get(0)
            .getValuePickFirst()
            .get(0));
    assertEquals(
        "resourceValue",
        config.getContextEnrichments().get(1).getEnrichedAttributes().get(1).getAs());
    assertEquals((Long) 86400000L, config.getTtl());

    final String expected = src.replace(" ", "").replace("\n", "");
    assertEquals(expected, mapper.writeValueAsString(config));

    // copy constructor test
    final var clone = new StreamConfig(config);
    assertEquals(clone.getName(), config.getName());
    assertEquals(clone.getVersion(), config.getVersion());
    assertEquals(clone.getMissingValuePolicy(), config.getMissingValuePolicy());
    assertEquals(clone.getAttributes(), config.getAttributes());
    assertEquals(clone.getContextEnrichments(), config.getContextEnrichments());
    assertEquals(clone.getTtl(), config.getTtl());
  }

  @Test
  public void testFeatureAsContextMarshalling() throws IOException {
    final String resourceName = "/FeatureAsContextTfos.json";
    final byte[] bytes = getClass().getResourceAsStream(resourceName).readAllBytes();
    final String src = new String(bytes, StandardCharsets.UTF_8);
    final StreamConfig config =
        mapper.readValue(getClass().getResource(resourceName), StreamConfig.class);

    assertEquals("orders", config.getName());
    assertEquals(3, config.getViews().size());
    assertEquals(3, config.getPostprocesses().size());
    assertEquals("byZipcode", config.getViews().get(0).getName());
    assertEquals("byZipcode", config.getPostprocesses().get(0).getView());
    assertEquals(1, config.getPostprocesses().get(0).getRollups().size());

    final String expected = src.replace(" ", "").replace("\n", "");
    assertEquals(expected, mapper.writeValueAsString(config));

    // copy constructor test
    final var clone = new StreamConfig(config);
    assertEquals(clone.getName(), config.getName());
    assertEquals(clone.getViews(), config.getViews());
    assertEquals(clone.getPostprocesses(), config.getPostprocesses());
  }
}
