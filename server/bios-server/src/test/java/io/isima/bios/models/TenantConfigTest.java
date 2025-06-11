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

import static io.isima.bios.models.v1.InternalAttributeType.INT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.admin.v1.StreamDesc;
import io.isima.bios.admin.v1.TenantDesc;
import io.isima.bios.models.v1.AttributeDesc;
import io.isima.bios.models.v1.StreamConfig;
import io.isima.bios.models.v1.TenantConfig;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TenantConfigTest {
  private static ObjectMapper mapper;

  @BeforeClass
  public static void setUpClass() {
    mapper = TfosObjectMapperProvider.get();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {}

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  private void checkInitialConfig(TenantConfig config, String name) {
    assertEquals(name, config.getName());
    assertNotNull(config.getStreams());
  }

  @Test
  public void testConstructors() {
    checkInitialConfig(new TenantConfig(), null);
    checkInitialConfig(new TenantConfig("hello"), "hello");
  }

  @Test
  public void testDuplicate() {
    TenantConfig config = new TenantConfig("hello");
    config.setVersion(12345L);
    config.setDeleted(true);
    StreamConfig first = new StreamConfig("first").addAttribute(new AttributeDesc("one", INT));
    StreamConfig second = new StreamConfig("second").addAttribute(new AttributeDesc("three", INT));
    config.addStream(first).addStream(second);

    TenantConfig clone = config.duplicate();
    assertNotSame(config, clone);

    // check name
    assertEquals("hello", config.getName());
    assertEquals("hello", clone.getName());

    // check version
    assertEquals(Long.valueOf(12345L), config.getVersion());
    assertEquals(Long.valueOf(12345L), clone.getVersion());

    // check deleted flag
    assertTrue(config.isDeleted());
    assertTrue(clone.isDeleted());

    // check streams
    assertNotSame(config.getStreams(), clone.getStreams());
    assertEquals(2, config.getStreams().size());
    assertEquals(2, clone.getStreams().size());
    assertNotSame(config.getStreams().get(0), clone.getStreams().get(0));
    assertEquals("first", config.getStreams().get(0).getName());
    assertEquals("first", clone.getStreams().get(0).getName());

    // modify the clone and verify the original is not affected
    clone.setName("world");
    clone.getStreams().clear();
    clone.setVersion(23456L);
    clone.setDeleted(false);
    assertEquals("hello", config.getName());
    assertEquals(Long.valueOf(12345L), config.getVersion());
    assertTrue(config.isDeleted());
    assertEquals(2, config.getStreams().size());
    assertEquals("first", config.getStreams().get(0).getName());
  }

  @Test
  public void testFindStream() {
    TenantDesc config = new TenantDesc("hello", 12345L, false);
    StreamDesc first = new StreamDesc("first", 12345L).addAttribute(new AttributeDesc("one", INT));
    StreamDesc second =
        new StreamDesc("second", 12345L).addAttribute(new AttributeDesc("three", INT));
    config.addStream(first).addStream(second);

    StreamDesc stream = config.getStream("first", false);
    assertNotNull(stream);
    assertEquals("first", stream.getName());

    stream = config.getStream("SECOND", false);
    assertNotNull(stream);
    assertEquals("second", stream.getName());

    stream = config.getStream("third", false);
    assertNull(stream);
  }

  @Test
  public void testJsonSerialization() throws IOException {
    TenantConfig config = new TenantConfig("test_tenant").setVersion(12345L).setDeleted(false);
    String out = mapper.writeValueAsString(config);
    assertTrue(out.contains("\"name\":\"test_tenant\""));
    assertTrue(out.contains("\"version\":12345"));
    assertFalse(out.contains("deleted"));

    String src = "{\"name\":\"test_tenant\",\"streams\":[],\"version\":12345,\"deleted\":true}";
    TenantConfig decoded = mapper.readValue(src, TenantConfig.class);
    assertEquals("test_tenant", decoded.getName());
    assertEquals(Long.valueOf(12345L), decoded.getVersion());
    assertFalse(decoded.isDeleted());
  }
}
