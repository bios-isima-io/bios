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
import io.isima.bios.models.v1.Permission;
import io.isima.bios.utils.TfosObjectMapperProvider;
import org.junit.Test;

public class PermissionTest {

  private final ObjectMapper mapper = TfosObjectMapperProvider.get();

  @Test
  public void testDeserialization() throws Exception {
    String permIdStr = String.valueOf(Permission.BI_REPORT.getId());
    Permission permission = mapper.readValue(permIdStr, Permission.class);

    assertEquals(Permission.BI_REPORT, permission);
  }

  @Test
  public void testSerialization() throws Exception {
    String expectedOutput = "{\"id\":4,\"name\":\"Admin\"}";
    assertEquals(expectedOutput, mapper.writeValueAsString(Permission.ADMIN));
  }
}
