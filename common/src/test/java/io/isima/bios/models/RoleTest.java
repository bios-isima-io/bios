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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.utils.BiosObjectMapperProvider;
import org.junit.BeforeClass;
import org.junit.Test;

public class RoleTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testSerializer() throws Exception {
    assertEquals("\"SystemAdmin\"", objectMapper.writeValueAsString(Role.SYSTEM_ADMIN));
    assertEquals("\"TenantAdmin\"", objectMapper.writeValueAsString(Role.TENANT_ADMIN));
    assertEquals(
        "\"SchemaExtractIngest\"", objectMapper.writeValueAsString(Role.SCHEMA_EXTRACT_INGEST));
    assertEquals("\"Extract\"", objectMapper.writeValueAsString(Role.EXTRACT));
    assertEquals("\"Ingest\"", objectMapper.writeValueAsString(Role.INGEST));
  }

  @Test
  public void testDeserializer() throws Exception {
    assertEquals(Role.SYSTEM_ADMIN, objectMapper.readValue("\"systemAdmin\"", Role.class));
    assertEquals(Role.TENANT_ADMIN, objectMapper.readValue("\"tenantAdmin\"", Role.class));
    assertEquals(
        Role.SCHEMA_EXTRACT_INGEST, objectMapper.readValue("\"SchemaExtractIngest\"", Role.class));
    assertEquals(Role.EXTRACT, objectMapper.readValue("\"extract\"", Role.class));
    assertEquals(Role.INGEST, objectMapper.readValue("\"ingest\"", Role.class));
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializerFail() throws Exception {
    objectMapper.readValue("unknwon", Role.class);
  }
}
