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
import io.isima.bios.utils.BiosObjectMapperProvider;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.Test;

public class UserTest {
  private static ObjectMapper objectMapper;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    objectMapper = BiosObjectMapperProvider.get();
  }

  @Test
  public void testJsonMarshalling() throws Exception {
    final String src =
        "{"
            + "  'userId': '12345-55432',"
            + "  'email': 'test@example.com',"
            + "  'fullName': 'John Doe',"
            + "  'tenantName': 'testTenant',"
            + "  'roles': ['TenantAdmin', 'Internal'],"
            + "  'status': 'Active',"
            + "  'password': 'mypassword',"
            + "  'devInstance': {"
            + "    'devInstanceName': 'myDevInstance',"
            + "    'sshKey': 'ssh-key',"
            + "    'sshUser': 'testUser',"
            + "    'sshIp': '10.20.30.40'"
            + "  },"
            + "  'homePageConfig': '{\\'some\\':\\'json\\'}',"
            + "  'createTimestamp': 1588197288384,"
            + "  'modifyTimestamp': 1588197288385"
            + "}";

    final UserConfig user = objectMapper.readValue(src.replace("'", "\""), UserConfig.class);
    assertEquals("12345-55432", user.getUserId());
    assertEquals("test@example.com", user.getEmail());
    assertEquals("John Doe", user.getFullName());
    assertEquals("testTenant", user.getTenantName());
    assertEquals(List.of(Role.TENANT_ADMIN, Role.INTERNAL), user.getRoles());
    assertEquals(MemberStatus.ACTIVE, user.getStatus());
    assertEquals("mypassword", user.getPassword());
    assertEquals("myDevInstance", user.getDevInstance().getDevInstanceName());
    assertEquals("ssh-key", user.getDevInstance().getSshKey());
    assertEquals("testUser", user.getDevInstance().getSshUser());
    assertEquals("10.20.30.40", user.getDevInstance().getSshIp());
    assertEquals("homePageConfig", "{\"some\":\"json\"}", user.getHomePageConfig());
    assertEquals(Long.valueOf(1588197288384L), user.getCreateTimestamp());
    assertEquals(Long.valueOf(1588197288385L), user.getModifyTimestamp());

    final String expected =
        src.replace("John Doe", "_temp_")
            .replace("'", "\"")
            .replace(" ", "")
            .replace("\n", "")
            .replace("_temp_", "John Doe");
    assertEquals(expected, objectMapper.writeValueAsString(user));
  }
}
