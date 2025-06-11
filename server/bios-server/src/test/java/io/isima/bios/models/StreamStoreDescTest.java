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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamComparators;
import io.isima.bios.admin.v1.StreamStoreDesc;
import io.isima.bios.models.v1.ActionDesc;
import io.isima.bios.models.v1.ActionType;
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

public class StreamStoreDescTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
  }

  private void checkInitialConfig(
      StreamStoreDesc config, String name, StreamType type, Long version, boolean isDeleted) {
    assertEquals(name, config.getName());
    assertEquals(type, config.getType());
    assertEquals(version, config.getVersion());
    assertEquals(isDeleted, config.isDeleted());
    assertNotNull(config.getAttributes());
    assertNull(config.getAdditionalAttributes());
    assertNull(config.getPreprocesses());
  }

  @Test
  public void testConstructors() {
    checkInitialConfig(new StreamStoreDesc(), null, StreamType.SIGNAL, null, false);
    checkInitialConfig(new StreamStoreDesc("hello"), "hello", StreamType.SIGNAL, null, false);
    checkInitialConfig(
        new StreamStoreDesc("hello", 12345L, true), "hello", StreamType.SIGNAL, 12345L, true);
    checkInitialConfig(
        new StreamStoreDesc("hello", StreamType.CONTEXT), "hello", StreamType.CONTEXT, null, false);
    final StreamConfig streamConfig =
        new StreamConfig("abc")
            .setVersion(12345L)
            .setType(StreamType.CONTEXT)
            .addAttribute(new AttributeDesc("first", InternalAttributeType.STRING))
            .addAdditionalAttribute(new AttributeDesc("additional", InternalAttributeType.INT));
    final StreamStoreDesc store = new StreamStoreDesc(streamConfig).setDeleted(true);
    assertEquals("abc", store.getName());
    assertEquals(Long.valueOf(12345L), store.getVersion());
    assertTrue(store.isDeleted());
    assertEquals(StreamType.CONTEXT, store.getType());
    assertEquals(1, store.getAttributes().size());
    assertEquals("first", store.getAttributes().get(0).getName());
    assertEquals(InternalAttributeType.STRING, store.getAttributes().get(0).getAttributeType());
    assertEquals(1, store.getAdditionalAttributes().size());
    assertEquals("additional", store.getAdditionalAttributes().get(0).getName());
    assertEquals(
        InternalAttributeType.INT, store.getAdditionalAttributes().get(0).getAttributeType());
  }

  @Test
  public void testDuplicateSignal() throws JsonProcessingException {
    // Build an instance
    String streamName = "testSignal";
    StreamStoreDesc original = new StreamStoreDesc(streamName);
    original.addAttribute(new AttributeDesc("one", InternalAttributeType.STRING));
    original.addAttribute(
        new AttributeDesc("two", InternalAttributeType.INT)
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
    final var prp = new PreprocessDesc("pp");
    prp.setCondition("two");
    prp.setForeignKey(List.of("one", "two"));
    prp.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    final var action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("remote");
    action.setAs("imported");
    action.setContext("remoteContext");
    action.setDefaultValue("MISSING");
    prp.setActions(List.of(action));
    original.addPreprocess(prp);
    original.setType(StreamType.SIGNAL).setVersion(12345L);
    original.setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT);
    original.setPrevName("previous");
    original.setPrevVersion(8312642L);
    original.setTableName("tentative_table_name");
    original.setSchemaName("schemaschema");
    original.setSchemaVersion(19950802L);
    original.setDeleted(true);
    original.setIndexWindowLength(1571358563197L);

    // make a clone
    StreamStoreDesc clone = original.duplicate();
    assertNotSame(original, clone);

    // check name
    assertEquals(streamName, clone.getName());
    assertEquals(clone.getName(), original.getName());
    // check type
    assertEquals(StreamType.SIGNAL, clone.getType());
    assertEquals(clone.getType(), original.getType());
    // check missing value policy
    assertEquals(clone.getMissingValuePolicy(), original.getMissingValuePolicy());
    // check version
    assertEquals((Long) 12345L, clone.getVersion());
    assertEquals((Long) 12345L, original.getVersion());
    // check deleted flag
    assertTrue(clone.isDeleted());
    assertEquals(clone.isDeleted(), original.isDeleted());
    // check index window length
    assertEquals(clone.getIndexWindowLength(), original.getIndexWindowLength());
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
      assertTrue(StreamComparators.equals(ppClone.get(i), ppOrig.get(i)));
      assertEquals(ppClone.get(i).getForeignKey(), ppOrig.get(i).getForeignKey());
    }

    // check StreamConfigStore properties
    assertEquals("previous", clone.getPrevName());
    assertEquals(Long.valueOf(8312642L), clone.getPrevVersion());
    assertEquals("tentative_table_name", clone.getTableName());
    assertEquals("schemaschema", clone.getSchemaName());
    assertEquals(Long.valueOf(19950802L), clone.getSchemaVersion());

    // Check that original and clone have the same serialized form.
    String serializedOriginal = mapper.writeValueAsString(original);
    String serializedClone = mapper.writeValueAsString(clone);
    assertEquals(serializedOriginal, serializedClone);
  }

  @Test
  public void testDuplicateContext() throws JsonProcessingException {
    // Build an instance
    String streamName = "testContext";
    StreamStoreDesc original = new StreamStoreDesc(streamName);
    original.setType(StreamType.CONTEXT);
    original.addAttribute(new AttributeDesc("one", InternalAttributeType.STRING));
    original.addAttribute(
        new AttributeDesc("two", InternalAttributeType.LONG)
            .setMissingValuePolicy(MissingAttributePolicyV1.USE_DEFAULT)
            .setDefaultValue(100));
    original.addAttribute(
        new AttributeDesc("enumenum", InternalAttributeType.ENUM)
            .setEnum(Arrays.asList(new String[] {"ichi", "ni", "san"})));
    original.addAttribute(new AttributeDesc("three", InternalAttributeType.DOUBLE));
    original.addAttribute(new AttributeDesc("four", InternalAttributeType.LONG));
    original.setPrimaryKey(List.of("one", "two"));
    final var feature1 = new FeatureDesc();
    feature1.setName("feature1");
    feature1.setDimensions(List.of("enumenum", "three"));
    feature1.setAttributes(List.of("four"));
    feature1.setIndexed(true);
    feature1.setIndexType(IndexType.RANGE_QUERY);
    original.setFeatures(List.of(feature1));

    // make a clone
    StreamStoreDesc clone = original.duplicate();
    assertNotSame(original, clone);

    // check name
    assertEquals(streamName, clone.getName());
    assertEquals(clone.getName(), original.getName());
    // check type
    assertEquals(StreamType.CONTEXT, clone.getType());
    assertEquals(clone.getType(), original.getType());
    // check missing value policy
    assertEquals(clone.getMissingValuePolicy(), original.getMissingValuePolicy());
    // check attributes
    assertEquals(original.getAttributes(), clone.getAttributes());
    // check primary key
    assertEquals(original.getPrimaryKey(), clone.getPrimaryKey());
    // check features
    assertEquals(original.getFeatures(), clone.getFeatures());

    // Check that original and clone have the same serialized form.
    String serializedOriginal = mapper.writeValueAsString(original);
    String serializedClone = mapper.writeValueAsString(clone);
    assertEquals(serializedOriginal, serializedClone);
  }

  @Test
  public void testJsonSerialization() throws IOException {
    StreamStoreDesc config = new StreamStoreDesc("test_stream", 12345L, true);
    config.setType(StreamType.CONTEXT);
    config.addAttribute(new AttributeDesc("first", InternalAttributeType.STRING));
    config.addAttribute(new AttributeDesc("second", InternalAttributeType.STRING));
    config.addAttribute(new AttributeDesc("third", InternalAttributeType.STRING));
    config.setPrimaryKey(List.of("first", "second"));
    config.addAdditionalAttribute(new AttributeDesc("additional", InternalAttributeType.INET));
    config.setPrevName("old_one");
    config.setPrevVersion(19182L);
    config.setTableName("evt_039ad98de8cc3d19aed88d0a7e8f77d3");
    config.setSchemaName("Abcdefg");
    config.setSchemaVersion(20010210L);
    config.setIndexWindowLength(1571358563197L);
    config.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    final var prp = new PreprocessDesc("pp");
    prp.setCondition("two");
    prp.setForeignKey(List.of("one", "two"));
    prp.setMissingLookupPolicy(MissingAttributePolicyV1.USE_DEFAULT);
    final var action = new ActionDesc();
    action.setActionType(ActionType.MERGE);
    action.setAttribute("remote");
    action.setAs("imported");
    action.setContext("remoteContext");
    action.setDefaultValue("MISSING");
    prp.setActions(List.of(action));
    config.addPreprocess(prp);

    String out = mapper.writeValueAsString(config);
    System.out.println(out);
    assertTrue(out.contains("\"name\":\"test_stream\""));
    assertTrue(out.contains("\"version\":12345"));
    assertTrue(
        out.contains(
            ("'attributes':["
                    + "{'name':'first','type':'string'},"
                    + "{'name':'second','type':'string'},"
                    + "{'name':'third','type':'string'}]")
                .replace("'", "\"")));
    assertFalse(out.contains("additionalAttribute"));
    assertFalse(out.contains("deleted"));
    assertTrue(out, out.contains("\"globalLookupPolicy\":\"use_default\""));
    assertTrue(out, out.contains("'primaryKey':['first','second']".replace("'", "\"")));
    //
    assertTrue(out, out.contains("\"prevName\":\"old_one\""));
    assertTrue(out, out.contains("\"prevVersion\":19182"));
    assertTrue(out, out.contains("\"tableName\":\"evt_039ad98de8cc3d19aed88d0a7e8f77d3\""));
    assertTrue(out, out.contains("\"schemaName\":\"Abcdefg\""));
    assertTrue(out, out.contains("\"schemaVersion\":20010210"));
    assertTrue(out, out.contains("\"indexWindowLength\":1571358563197"));

    StreamStoreDesc decoded = mapper.readValue(out, StreamStoreDesc.class);
    assertEquals("test_stream", decoded.getName());
    assertEquals(StreamType.CONTEXT, decoded.getType());
    assertEquals(Long.valueOf(12345L), decoded.getVersion());
    assertFalse(decoded.isDeleted());
    assertEquals(config.getAttributes(), decoded.getAttributes());
    assertNull(decoded.getAdditionalAttributes());
    assertEquals(config.getPrimaryKey(), decoded.getPrimaryKey());
    assertEquals("old_one", decoded.getPrevName());
    assertEquals(Long.valueOf(19182L), decoded.getPrevVersion());
    assertEquals("evt_039ad98de8cc3d19aed88d0a7e8f77d3", decoded.getTableName());
    assertEquals("Abcdefg", decoded.getSchemaName());
    assertEquals(Long.valueOf(20010210L), decoded.getSchemaVersion());
    assertEquals(Long.valueOf(1571358563197L), decoded.getIndexWindowLength());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, decoded.getMissingLookupPolicy());
    assertTrue(
        StreamComparators.equals(
            config.getPreprocesses().get(0), decoded.getPreprocesses().get(0)));
    assertEquals(config.getPrimaryKey(), decoded.getPrimaryKey());
  }

  @Test
  public void testJsonDeserialization() throws IOException {
    final String resourceName = "/testJsonReverseSerialization.json";
    final byte[] bytes = getClass().getResourceAsStream(resourceName).readAllBytes();
    final String src = new String(bytes, StandardCharsets.UTF_8);
    StreamStoreDesc decoded = mapper.readValue(src, StreamStoreDesc.class);
    assertEquals("special_page_viewed_signal", decoded.getName());
    assertEquals(StreamType.SIGNAL, decoded.getType());
    assertNotNull(decoded.getAttributes());
    assertEquals(List.of("funnel", "subscription"), decoded.getPrimaryKey());
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, decoded.getMissingLookupPolicy());
    var preprocess = decoded.getPreprocesses().get(0);
    assertEquals(MissingAttributePolicyV1.USE_DEFAULT, preprocess.getMissingLookupPolicy());
    assertEquals(
        MissingAttributePolicyV1.USE_DEFAULT,
        preprocess.getActions().get(0).getMissingLookupPolicy());
    assertEquals("visitorId", preprocess.getCondition());
    assertEquals(List.of("VisitorId", "extra"), preprocess.getForeignKey());
  }

  @Test
  public void testAttributeProxies() throws IOException {
    final String resourceName = "/testJsonReverseSerialization.json";
    final byte[] bytes = getClass().getResourceAsStream(resourceName).readAllBytes();
    final String src = new String(bytes, StandardCharsets.UTF_8);
    StreamStoreDesc streamStoreDesc = mapper.readValue(src, StreamStoreDesc.class);
    streamStoreDesc.setVersion(System.currentTimeMillis());

    streamStoreDesc.initAttributeProxyInfo();
    assertEquals(23, streamStoreDesc.getMaxAttributeProxy().shortValue());
    assertEquals(1, streamStoreDesc.getAttributeProxyInfo().get("visitorid").getProxy());
    assertEquals(2, streamStoreDesc.getAttributeProxyInfo().get("userid").getProxy());
    assertEquals(23, streamStoreDesc.getAttributeProxyInfo().get("universal").getProxy());

    streamStoreDesc.addAdditionalAttribute(
        new AttributeDesc("joinedAttr1", InternalAttributeType.DOUBLE));
    streamStoreDesc.addAdditionalAttribute(
        new AttributeDesc("joinedAttr2", InternalAttributeType.LONG));
    streamStoreDesc.addAdditionalAttribute(
        new AttributeDesc("joinedAttr3", InternalAttributeType.STRING));
    streamStoreDesc.addAdditionalAttribute(
        new AttributeDesc("joinedAttr4", InternalAttributeType.BLOB));
    streamStoreDesc.addAdditionalAttribute(
        new AttributeDesc("joinedAttr5", InternalAttributeType.BOOLEAN));
    streamStoreDesc.addAdditionalAttribute(
        new AttributeDesc("joinedAttr6", InternalAttributeType.DOUBLE));
    streamStoreDesc.initAttributeProxyInfo();
    assertEquals(29, streamStoreDesc.getMaxAttributeProxy().shortValue());
    assertEquals(1, streamStoreDesc.getAttributeProxyInfo().get("visitorid").getProxy());
    assertEquals(23, streamStoreDesc.getAttributeProxyInfo().get("universal").getProxy());
    assertEquals(24, streamStoreDesc.getAttributeProxyInfo().get("joinedattr1").getProxy());
    assertEquals(29, streamStoreDesc.getAttributeProxyInfo().get("joinedattr6").getProxy());

    streamStoreDesc.addAttribute(new AttributeDesc("newAttr1", InternalAttributeType.DOUBLE));
    streamStoreDesc.addAttribute(new AttributeDesc("newAttr2", InternalAttributeType.STRING));
    streamStoreDesc.initAttributeProxyInfo();
    assertEquals(31, streamStoreDesc.getMaxAttributeProxy().shortValue());
    assertEquals(1, streamStoreDesc.getAttributeProxyInfo().get("visitorid").getProxy());
    assertEquals(29, streamStoreDesc.getAttributeProxyInfo().get("joinedattr6").getProxy());
    assertEquals(30, streamStoreDesc.getAttributeProxyInfo().get("newattr1").getProxy());
    assertEquals(31, streamStoreDesc.getAttributeProxyInfo().get("newattr2").getProxy());
  }
}
