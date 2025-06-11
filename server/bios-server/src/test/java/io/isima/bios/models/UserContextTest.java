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

import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class UserContextTest {
  final List<Integer> permissions = Arrays.asList(1, 2);

  @Test
  public void testScopeValue() {
    final String tenant = "tenant1";
    final String user = "admin@tenant1.com";
    final String scope = String.format(UserContext.SCOPE_TENANT, tenant);

    final UserContext userContext =
        new UserContext(100L, 10L, user, tenant, permissions, "UserContextTest", AppType.REALTIME);
    userContext.setScope(scope);

    final String expectedScope = scope;
    assertEquals(expectedScope, userContext.getScope());
  }

  @Test
  public void testScopeValueWithoutSetting() {
    final String tenant = "tenant2";
    final String user = "admin@tenant2.com";

    final UserContext userContext =
        new UserContext(200L, 20L, user, tenant, permissions, "UserContextTest", AppType.REALTIME);

    final String expectedScope = String.format(UserContext.SCOPE_TENANT, tenant);
    assertEquals(expectedScope, userContext.getScope());
  }

  @Test
  public void testScopeValueForSysTenant() {
    final String tenant = "/";
    final String user = "platformadmin";

    final UserContext userContext =
        new UserContext(300L, 30L, user, tenant, permissions, "UserContextTest", AppType.REALTIME);

    final String expectedScope = tenant;
    assertEquals(expectedScope, userContext.getScope());
  }
}
