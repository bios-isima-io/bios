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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.isima.bios.models.LoginResponse.AuthCode;
import io.isima.bios.models.v1.Permission;
import io.isima.bios.utils.TfosObjectMapperProvider;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class LoginResponseTest {

  @Test
  public void testConstruction() {
    List<Permission> permissions = Arrays.asList(Permission.ADMIN);

    LoginResponse resp =
        new LoginResponse(
            "dummy-token",
            "testTenant",
            null,
            "testApp",
            AppType.REALTIME,
            12345L,
            permissions,
            60000L,
            null);
    assertEquals(AuthCode.SUCCESS, resp.getCode());
    assertEquals("dummy-token", resp.getToken());
    assertEquals("testTenant", resp.getTenant());
    assertEquals("testApp", resp.getAppName());
    assertEquals(AppType.REALTIME, resp.getAppType());
    assertEquals(12345L, resp.getExpiry().longValue());
    assertEquals(permissions, resp.getPermissions());
    assertEquals(60000L, resp.getSessionTimeoutMillis().longValue());
  }

  @Test
  public void testJsonCodec() throws IOException {
    ObjectMapper mapper = TfosObjectMapperProvider.get();

    final List<Permission> permissions = Arrays.asList(Permission.INGEST);

    final LoginResponse resp =
        new LoginResponse(
            "test-token",
            null,
            "isima",
            "testApp",
            AppType.BATCH,
            12345L,
            permissions,
            60000L,
            null);

    final String json = mapper.writeValueAsString(resp);

    // property 'tenant' shouldn't be dumped
    assertFalse(json.contains("tenant"));

    final String toDecode = json.replace("\"token\":", "\"tenant\":\"isima\",\"token\":");

    LoginResponse decoded = mapper.readValue(toDecode, LoginResponse.class);
    assertEquals(AuthCode.SUCCESS, decoded.getCode());
    assertEquals("test-token", decoded.getToken());
    assertNull(decoded.getTenant()); // property 'tenant' shouldn't be loaded
    assertEquals("testApp", decoded.getAppName());
    assertEquals(AppType.BATCH, decoded.getAppType());
    assertEquals(12345L, decoded.getExpiry().longValue());
    permissions.forEach(perm -> assertTrue(decoded.getPermissions().contains(perm)));
    assertEquals(60000L, decoded.getSessionTimeoutMillis().longValue());
  }

  @Test
  public void testSerializationDeserializationWithMinimalValue() throws IOException {
    ObjectMapper mapper = TfosObjectMapperProvider.get();

    LoginResponse loginResponse = new LoginResponse(AuthCode.SUCCESS);
    loginResponse.setSessionAttributes(null);

    String json = mapper.writeValueAsString(loginResponse);
    assertEquals("{\"code\":\"success\"}", json);

    LoginResponse decodedLoginResponse = mapper.readValue(json, LoginResponse.class);
    assertEquals(AuthCode.SUCCESS, decodedLoginResponse.getCode());
    assertTrue(decodedLoginResponse.getPermissions().isEmpty());
  }
}
